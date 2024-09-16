import uuid
from datetime import datetime, timedelta
import nats

class Job:
    def __init__(self, name, data, delay=0, meta=None):
        self.id = str(uuid.uuid4())  # Уникальный идентификатор задачи
        self.name = name  # Название задачи
        self.data = data  # Данные для обработки
        self.delay = delay  # Задержка перед выполнением задачи в секундах
        self.meta = meta or {}  # Метаинформация
        self.scheduled_time = datetime.now() + timedelta(seconds=delay)


class Queue:
    def __init__(self, topic_name):
        self.topic_name = topic_name  # Топик для очереди
        self.nc = None  # Контекст NATS
        self.js = None  # Контекст JetStream
        
    
    async def connect(self):
        self.nc = await nats.connect()  # Подключение к NATS
        self.js = self.nc.jetstream()  # Инициализация JetStream

        # Создание потока (если он не существует)
        await self.js.add_stream(name=self.topic_name, subjects=[self.topic_name])

    async def addJob(self, job: Job):
        # Добавление одной задачи в JetStream
        await self.js.publish(self.topic_name, job.data.encode('utf-8'))
    
    async def addJobs(self, jobs: list[Job]):
        # Добавление нескольких задач в очередь
        for job in jobs:
            await self.addJob(job)
    
    async def close(self):
        await self.nc.drain()  # Закрытие соединения


class Worker:
    def __init__(self, topic_name, concurrency, rate_limit, processor):
        self.topic_name = topic_name  # Топик, из которого воркер будет брать задачи
        self.concurrency = concurrency  # Количество задач, которые могут обрабатываться одновременно
        self.rate_limit = rate_limit  # Ограничение по скорости: max + duration
        self.processor = processor  # Функция обработки задачи
        self.nc = None  # Контекст NATS
        self.js = None  # Контекст JetStream
    
    async def connect(self):
        self.nc = await nats.connect()  # Подключение к NATS
        self.js = self.nc.jetstream()  # Инициализация JetStream
        
        # Подписка на топик
        await self.js.subscribe(self.topic_name, queue="worker-group", cb=self._message_handler)

    async def _message_handler(self, msg):
        # Внутренний обработчик сообщений
        await self.processor(msg)  # Вызов переданного процессора задач
    
    async def close(self):
        await self.nc.drain()  # Закрытие соединения
