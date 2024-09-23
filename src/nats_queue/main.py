import logging
import nats
import asyncio
import uuid
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.DEBUG
)

logger = logging.getLogger('nats')

user = os.environ.get('NATS_USER')
password = os.environ.get('NATS_PASSWORD')

class Job:
    def __init__(self, queue_name, name, data, delay=0, meta=None):
        self.id = str(uuid.uuid4())
        self.queue_name = queue_name
        self.name = name
        self.data = data
        self.delay = delay
        self.meta = meta or {}
        self.retry_count = 0

    @property
    def subject(self):
        return f"{self.queue_name}.{self.name}"

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "data": self.data,
            "meta": self.meta,
            "delay": self.delay,
            "retry_count": self.retry_count
        }

class Queue:
    def __init__(self, topic_name: str, priorities: int = 1):
        self.topic_name = topic_name
        self.priorities = priorities
        self.nc = None
        self.js = None
        
    async def connect(self):
        logger.info("Подключение к NATS...")
        try:
            self.nc = await nats.connect(servers=["nats://localhost:4222"], user=user, password=password)
            self.js = self.nc.jetstream()
            logger.info("Успешно подключено к NATS")

            try:
                await self.js.delete_stream(self.topic_name)
                logger.info(f"Старый стрим '{self.topic_name}' успешно удален.")
            except Exception as e:
                logger.warning(f"Ошибка при удалении стрима '{self.topic_name}': {str(e)}")

            subjects = [f"{self.topic_name}.*.*"]
            await self.js.add_stream(name=self.topic_name, subjects=subjects)
        except Exception as e:
            logger.error(f"Ошибка подключения к NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            logger.info("Закрытие соединения с NATS...")
            await self.nc.close() 
            logger.info("Соединение закрыто")

    async def addJob(self, job: Job, priority: int = 0):
        if priority >= self.priorities:
            priority = self.priorities - 1 
        logger.info(f"Добавление задачи {job.subject} в очередь с приоритетом {priority}")
        job_data = json.dumps(job.to_dict()).encode()
        await self.js.publish(f"{job.queue_name}.{job.name}.{priority}", job_data, headers={"Nats-Msg-Id": job.id})
    
    async def addJobs(self, jobs: list[Job], priority: int = 0):
        for job in jobs:
            await self.addJob(job, priority)

class RateLimiter:
    def __init__(self, max_tasks, duration):
        self.max_tasks = max_tasks
        self.duration = duration
        self.processed_count = 0
        self.start_time = time.time()
        logger.debug(f"RateLimiter создан с max_tasks={max_tasks}, duration={duration}.")

    def increment(self):
        self.processed_count += 1
        logger.debug(f"Увеличение счетчика обработанных задач: {self.processed_count}.")

    async def check_limit(self):
        current_time = time.time()
        elapsed = current_time - self.start_time
        
        if elapsed < self.duration and self.processed_count >= self.max_tasks:
            wait_time = self.duration - elapsed
            logger.info(f"Достижение лимита обработки. Ожидание {wait_time:.2f} секунд.")
            await asyncio.sleep(wait_time)
            self.start_time = time.time()
            self.processed_count = 0
            logger.debug(f"Состояние сброшено: start_time обновлен, processed_count сброшен.")

class Worker:
    def __init__(self, topic_name: str, concurrency: int, rate_limit: dict, processor_callback, priorities: int = 1, max_retries: int = 3):
        self.topic_name = topic_name
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.processor_callback = processor_callback
        self.priorities = priorities
        self.nc = None
        self.js = None
        self.queue = asyncio.Queue()
        self.max_retries = max_retries
        self.semaphore = asyncio.Semaphore(concurrency)  # Используем семафор для контроля параллелизма
        self.processing_jobs = set() 

    async def connect(self):
        logger.info("Подключение воркера к NATS...")
        try:
            self.nc = await nats.connect(servers=["nats://localhost:4222"], user=user, password=password)
            self.js = self.nc.jetstream()
            logger.info("Воркер успешно подключен к NATS")
        except Exception as e:
            logger.error(f"Ошибка подключения воркера к NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            logger.info("Закрытие воркера...")
            await self.nc.close()
            logger.info("Воркер успешно отключен")

    async def _process_task(self, job_data, msg):
        async with self.semaphore:  # Захватываем семафор
            try:
                logger.info(f"Обработка задачи {job_data['name']}")

                if job_data.get('retry_count', 0) >= self.max_retries:
                    logger.error(f"Максимальное количество попыток для задачи {job_data['name']} превышено.")
                    return

                timeout = self.rate_limit['duration']
                await asyncio.wait_for(self.processor_callback(job_data), timeout=timeout)
                await msg.ack()
                logger.info(f"Задача {job_data['name']} успешно обработана.")
                msg_id = msg.headers.get("Nats-Msg-Id", None)
                self.processing_jobs.remove(msg_id)  # Удаляем идентификатор после успешной обработки

            except asyncio.TimeoutError:
                job_data['retry_count'] += 1
                logger.error(f"Тайм-аут задачи {job_data['name']}. Попытка {job_data['retry_count']}.")
                await self.queue.put((job_data, msg))
            except Exception as e:
                job_data['retry_count'] += 1
                logger.error(f"Ошибка при обработке задачи: {str(e)}")
                await self.queue.put((job_data, msg))

    async def fetch_messages(self, sub):
        try:
            msgs = await sub.fetch(10, timeout=5)
            logger.debug(f'Получено {len(msgs)}')
            for msg in msgs:
                job_data = json.loads(msg.data.decode())

                # Получаем уникальный идентификатор сообщения
                msg_id = msg.headers.get("Nats-Msg-Id", None)

                # Проверяем, находится ли задача уже в процессе обработки
                if msg_id in self.processing_jobs:
                    logger.warning(f"Сообщение {msg_id} уже обрабатывается, пропускаем.")
                    continue

                await self.queue.put((job_data, msg))
                self.processing_jobs.add(msg_id)  # Добавляем идентификатор в множество
        except nats.errors.TimeoutError:
            logger.error(f"Не удалось получить сообщения: истекло время ожидания.")
        except Exception as e:
            logger.error(f"Ошибка получения сообщений: {e}")


    async def start(self):
        subscriptions = []
        for priority in range(self.priorities):
            topic = f"{self.topic_name}.*.{priority}"
            try:
                sub = await self.js.pull_subscribe(topic, durable=f"worker_group_{priority}")
                logger.debug(f"Подписка на {topic} успешна: {sub}")
                subscriptions.append(sub)
            except Exception as e:
                logger.error(f"Ошибка подписки на {topic}: {e}")

        limiter = RateLimiter(self.rate_limit['max'], self.rate_limit['duration'])

        while True:
            fetch_tasks = [self.fetch_messages(sub) for sub in subscriptions]
            await asyncio.gather(*fetch_tasks)

            while not self.queue.empty() and not self.semaphore.locked():
                await limiter.check_limit()
                if self.semaphore.locked():
                    logger.info("Семафор занят, ожидаем...")
                    break

                job_data, msg = await self.queue.get()
                asyncio.create_task(self._process_task(job_data, msg))
                limiter.increment()

            if self.queue.empty():
                logger.info("Нет непрочитанных сообщений, ожидаем...")
                await asyncio.sleep(1)

async def process_job(job_data):
    logger.info(f"Выполняется {job_data['name']} что-то делает...")
    await asyncio.sleep(5)

async def main():
    queue = Queue(topic_name="my_queue", priorities=3)
    await queue.connect()

    jobs = [Job(queue_name="my_queue", name=f"task_{i+1}", data={"key": f"value_{i+1}"}) for i in range(10)]
    
    await queue.addJobs(jobs)
    
    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 5, "duration": 30},
        processor_callback=process_job,
        priorities=3,
        max_retries=3
    )
    
    await worker.connect()
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
