import json
import random
import time
import uuid
from datetime import datetime
import logging
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderEventProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='orders'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
        
    def connect(self):
        """Создание подключения к Kafka с полной конфигурацией"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            logger.info(f"Успешно подключились к Kafka на {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Ошибка подключения к Kafka: {e}")
            raise
    
    def generate_order(self):
        products = [
            {"product_id": 1, "name": "Ноутбук", "price": 75000, "category": "Электроника"},
            {"product_id": 2, "name": "Мышь", "price": 1500, "category": "Электроника"},
            {"product_id": 3, "name": "Книга SQL", "price": 2500, "category": "Книги"},
            {"product_id": 4, "name": "Клавиатура", "price": 5000, "category": "Электроника"},
            {"product_id": 5, "name": "Монитор", "price": 25000, "category": "Электроника"},
            {"product_id": 6, "name": "Книга Python", "price": 3500, "category": "Книги"}
        ]
        customers = [
            {"id": 1, "name": "Анна Смирнова", "city": "Москва"},
            {"id": 2, "name": "Петр Иванов", "city": "СПб"},
            {"id": 3, "name": "Мария Сидорова", "city": "Казань"},
            {"id": 4, "name": "Иван Петров", "city": "Москва"},
            {"id": 5, "name": "Елена Козлова", "city": "Новосибирск"}
        ]
        
        product = random.choice(products)
        customer = random.choice(customers)
        quantity = random.randint(1, 3)
        
        return {
            "order_id": str(uuid.uuid4())[:8],
            "timestamp": datetime.now().isoformat(),
            "customer": customer,
            "items": [{
                "product_id": product["product_id"],
                "product_name": product["name"],
                "category": product["category"],
                "quantity": quantity,
                "unit_price": product["price"],
                "total_price": quantity * product["price"]
            }],
            "total_amount": quantity * product["price"],
            "payment_method": random.choice(["card", "cash", "online"])
        }
    
    def send_order(self, order):
        """Отправка заказа с партиционированием по customer_id"""
        key = str(order['customer']['id'])
        future = self.producer.send(self.topic, key=key, value=order)
        try:
            record_metadata = future.get(timeout=10)
            logger.info(f"Заказ {order['order_id']} отправлен в партицию {record_metadata.partition} с оффсетом {record_metadata.offset}")
        except Exception as e:
            logger.error(f"Ошибка отправки заказа: {e}")
        return future
    
    def run(self, interval_seconds=1, max_orders=20):
        logger.info(f"Запуск продюсера. Отправка {max_orders} заказов с интервалом {interval_seconds}с...")
        self.connect()
        for _ in range(max_orders):
            order = self.generate_order()
            self.send_order(order)
            time.sleep(interval_seconds)
        self.producer.flush()
        logger.info("Все тестовые заказы успешно отправлены!")
        self.producer.close()

if __name__ == "__main__":
    producer = OrderEventProducer()
    producer.run(interval_seconds=1, max_orders=20)