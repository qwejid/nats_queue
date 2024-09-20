import logging
import nats
import asyncio
import uuid
import json

logger = logging.getLogger('nats')
logging.basicConfig(level=logging.INFO)

class Job:
    def __init__(self, name, data, delay=0, meta=None):
        self.id = str(uuid.uuid4())  # Уникальный идентификатор задачи
        self.name = name  # Название задачи
        self.data = data  # Данные для обработки
        self.delay = delay  # Задержка перед выполнением задачи в секундах
        self.meta = meta or {}  # Метаинформация
    
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
        self.topic_name = topic_name  # Топик для очереди
        self.priorities = priorities # Количество уровней приоритетов
        self.nc = None  # Контекст NATS
        self.js = None  # Контекст JetStream
        
    async def connect(self):
        logger.info("Подключение к NATS...")
        try:
            self.nc = await nats.connect("nats://ruser:T0pS3cr3t@localhost:4222")  # Подключение к NATS
            self.js = self.nc.jetstream()  # Инициализация JetStream
            logger.info("Успешно подключено к NATS")

            try:
                await self.js.delete_stream(self.topic_name)
                logger.info(f"Старый поток {self.topic_name} удален")
            except Exception as e:
                logger.info(f"Удаление старого потока не требуется: {e}")

            # Создание потока с учетом всех приоритетных тем
            subjects = [f"{self.topic_name}_priority_{i}" for i in range(self.priorities)]
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
        """
        Добавление задания в очередь с учетом приоритета.
        Чем меньше значение priority, тем выше приоритет.
        """
        if priority >= self.priorities:
            priority = self.priorities - 1 
        topic = f"{self.topic_name}_priority_{priority}"
        logger.info(f"Добавление задачи {job.id} в очередь с приоритетом {priority}")
        job_data = json.dumps(job.to_dict()).encode()
        await self.js.publish(topic, job_data)
    
    # Добавление нескольких задач в очередь
    async def addJobs(self, jobs: list[Job], priority: int = 0):
        for job in jobs:
            await self.addJob(job, priority)
    
    
class Worker:
    def __init__(self, topic_name: str, concurrency: int, rate_limit: dict, processor_callback, priorities: int = 1):
        """
        'priorities' определяет, сколько уровней приоритета поддерживается.
        Воркеры сначала обрабатывают задачи с наивысшим приоритетом.
        """
        self.topic_name = topic_name  # Топик, из которого воркер будет брать задачи
        self.concurrency = concurrency  # Количество задач, которые могут обрабатываться одновременно
        self.rate_limit = rate_limit  # Ограничение по скорости: max + duration
        self.processor_callback = processor_callback  # Функция обработки задачи
        self.priorities = priorities # Количество уровней приоритета
        self.nc = None  # Контекст NATS
        self.js = None  # Контекст JetStream
        self.pending_tasks = set()
    
    async def connect(self):
        logger.info("Подключение воркера к NATS...")
        try:
            self.nc = await nats.connect("nats://ruser:T0pS3cr3t@localhost:4222")  # Подключение к NATS
            self.js = self.nc.jetstream()  # Инициализация JetStream
            logger.info("Воркер успешно подключен к NATS")
        except Exception as e:
            logger.error(f"Ошибка подключения воркера к NATS: {e}")
            raise

    async def close(self):
        if self.nc:
            logger.info("Закрытие воркера...")
            await self.nc.close()
            logger.info("Воркер успешно отключен")

    # Обработка одной задачи
    async def _process_task(self, msg, timeout=10):
        try:
            logger.info(f"Обработка задачи {msg.data.decode()}")
            task = asyncio.create_task(self.processor_callback(msg))
            self.pending_tasks.add(task)
            task.add_done_callback(self.pending_tasks.discard)
            await asyncio.wait_for(task, timeout=timeout)
            await msg.ack()  
        except asyncio.TimeoutError:
            logger.error(f"Тайм-аут задачи {msg.data.decode()}. Повторная отправка.")
            await msg.nak(delay=5)  # Повторная отправка с задержкой
        except Exception as e:
            logger.error(f"Ошибка при обработке задачи: {str(e)}")
            await msg.nak(delay=5)  # Задержка при ошибке


    
    # Запуск воркера с учётом приоритета задач
    async def start(self):
        subscriptions = []
        for priority in range(self.priorities):
            topic = f"{self.topic_name}_priority_{priority}"
            sub = await self.js.pull_subscribe(topic, durable=f"worker_group_{priority}")
            subscriptions.append(sub)
        
        while True:
            for sub in subscriptions:
                try:
                    msgs = await sub.fetch(self.concurrency, timeout=5)
                    if msgs:
                        for msg in msgs:
                            asyncio.create_task(self._process_task(msg))
                    else:
                        await asyncio.sleep(1) 
                    break  # Если задачи найдены, приоритетный pull завершён
                except Exception as e:
                    logger.error(f"Ошибка получения сообщений: {e}")
                    await asyncio.sleep(1) 


async def process_task(msg):
    print(f"Processing: {msg.data.decode()}")
    await asyncio.sleep(1)


async def main():
    queue = Queue("example_queue", priorities=3)
    await queue.connect()

    job = Job("task_1", {"data": "test"}, delay=0)
    await queue.addJob(job, priority=1)

    worker = Worker("example_queue", concurrency=5, rate_limit={"max": 10, "duration": 1}, processor_callback=process_task, priorities=3)
    await worker.connect()
    await worker.start()

if __name__ == "__main__":
    asyncio.run(main())
