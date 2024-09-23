import logging
import nats
import asyncio
import uuid
import json
import time
import os

logger = logging.getLogger('nats')
logging.basicConfig(level=logging.DEBUG)
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

    @property
    def subject(self):
        return f"{self.queue_name}.{self.name}"  # Основной сабжект без приоритета

    def to_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "data": self.data,
            "meta": self.meta,
            "delay": self.delay
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

            # Удаление старого стрима (если необходимо)
            try:
                await self.js.delete_stream(self.topic_name)
                logger.info(f"Старый стрим '{self.topic_name}' успешно удален.")
            except Exception as e:
                logger.warning(f"Ошибка при удалении стрима '{self.topic_name}': {str(e)}")

            # Создание нового стрима с поддержкой формата {queueName}.{jobName}.{priority}
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

class Worker:
    def __init__(self, topic_name: str, concurrency: int, rate_limit: dict, processor_callback, priorities: int = 1):
        self.topic_name = topic_name
        self.concurrency = concurrency
        self.rate_limit = rate_limit
        self.processor_callback = processor_callback
        self.priorities = priorities
        self.nc = None
        self.js = None
        self.pending_tasks = set()
        self.processed_jobs = set()
        self.last_process_time = time.time()
        self.semaphore = asyncio.Semaphore(concurrency)

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

    async def _process_task(self, msg, timeout=10):
        async with self.semaphore:
            try:
                logger.info(f"Обработка задачи {msg.data.decode()}")
                task = asyncio.create_task(self.processor_callback(msg))
                self.pending_tasks.add(task)
                task.add_done_callback(self.pending_tasks.discard)
                await asyncio.wait_for(task, timeout=timeout)
                await msg.ack()
            except asyncio.TimeoutError:
                logger.error(f"Тайм-аут задачи {msg.data.decode()}. Повторная отправка.")
                await msg.nak(delay=5)
            except Exception as e:
                logger.error(f"Ошибка при обработке задачи: {str(e)}")
                await msg.nak(delay=5)

    async def _rate_limit(self):
        # Получаю текущее время в секундах
        current_time = time.time()

        # Вычисляю время, прошедшее с последней обработки задачи
        elapsed_time = current_time - self.last_process_time

        # Рассчитываю допустимое время между обработками
        # duration - общее время, в течение которого могут быть обработаны max задач
        allowed_time = self.rate_limit['duration'] / self.rate_limit['max']

        # Если время, прошедшее с последней обработки, меньше допустимого времени
        if elapsed_time < allowed_time:
            # Засыпаем на оставшееся время, чтобы не превышать лимит
            await asyncio.sleep(allowed_time - elapsed_time)

        # Обновляем время последней обработки на текущее время
        self.last_process_time = time.time()


    async def start(self):
        subscriptions = []
        for priority in range(self.priorities):
            topic = f"{self.topic_name}.*.{priority}"  # Подписка на формат с приоритетом
            try:
                sub = await self.js.pull_subscribe(topic, durable=f"worker_group_{priority}")
                logger.debug(f"Подписка на {topic} успешна: {sub}")
                subscriptions.append(sub)
            except Exception as e:
                logger.error(f"Ошибка подписки на {topic}: {e}")
                continue

        while True:
            found_messages = False
            for sub in subscriptions:
                try:
                    msgs = await sub.fetch(self.concurrency, timeout=5)
                    logger.debug(f"Получено {len(msgs)} сообщений")
                    if msgs:
                        found_messages = True
                        await self._rate_limit()  # Применяем лимит скорости
                        for msg in msgs:
                            logger.debug(f"Обработка сообщения {msg.data.decode()}")
                            asyncio.create_task(self._process_task(msg))
                    else:
                        logger.info("Нет непрочитанных сообщений")
                        await asyncio.sleep(1)
                    break
                except Exception as e:
                    logger.error(f"Ошибка получения сообщений: {e}")
                    await asyncio.sleep(1)
            if not found_messages:
                logger.debug("Нет подходящих подписок")
                await asyncio.sleep(1)

# Пример использования
async def process_job(msg):
    job_data = json.loads(msg.data.decode())
    logger.info(f"Обработка задачи: {job_data['id']}, данные: {job_data['data']}")
    await asyncio.sleep(2)  # Имитация длительной работы
    logger.info(f"Задача {job_data['id']} успешно обработана")

async def main():
    queue = Queue(topic_name="my_queue", priorities=3)
    await queue.connect()

    jobs = [Job(queue_name="my_queue", name=f"task_{i}", data={"key": f"value_{i}"}) for i in range(10)]
    await queue.addJobs(jobs, priority=0)

    worker = Worker(
        topic_name="my_queue",
        concurrency=3,
        rate_limit={"max": 2, "duration": 5},
        processor_callback=process_job,
        priorities=3
    )
    
    await worker.connect()
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
