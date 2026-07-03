import json
import pika
from config import Config
from models import db
from tasks import process_item

class Worker:
    def __init__(self):
        print("🔧 Initializing Worker...")
        self.queue_name = Config.QUEUE_NAME
        self.connect()
    
    def connect(self):
        print(f"🔗 Connecting to RabbitMQ at {Config.RABBITMQ_HOST}:{Config.RABBITMQ_PORT}...")
        credentials = pika.PlainCredentials(
            Config.RABBITMQ_USER,
            Config.RABBITMQ_PASSWORD
        )
        parameters = pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300
        )
        try:
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            print(f"✅ Connected to RabbitMQ. Listening on: {self.queue_name}")
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            raise
    
    def callback(self, ch, method, properties, body):
        try:
            message = body.decode('utf-8')
            print(f"📩 Received: {message}")
            
            # Directly call the task function (Celery Worker not needed)
            from tasks import process_item
            result = process_item(message)  # Direct call
            
            print(f"✅ Task completed: {result}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"❌ Error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    
    def start(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.callback
        )
        print("🔄 Waiting for messages... (Press CTRL+C to stop)")
        self.channel.start_consuming()

if __name__ == '__main__':
    print("🚀 Starting Celery Worker...")
    db.init_db()
    worker = Worker()
    worker.start()