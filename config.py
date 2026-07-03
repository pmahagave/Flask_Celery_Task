import os

class Config:
    # Database
    DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'items.db')
    
    # RabbitMQ
    RABBITMQ_HOST = 'localhost'
    RABBITMQ_PORT = 5672
    RABBITMQ_USER = 'guest'
    RABBITMQ_PASSWORD = 'guest'
    
    # Queue
    QUEUE_NAME = 'item_queue'
    
    # Celery
    CELERY_BROKER_URL = f'amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}//'