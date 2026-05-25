import json
import logging
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class OrderStatsConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='orders', group_id='order_stats_group'):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        
        self.stats = {
            'total_orders': 0,
            'total_revenue': 0.0,
            'orders_by_category': defaultdict(int),
            'orders_by_city': defaultdict(int),
            'recent_orders': [],
            'start_time': datetime.now()
        }
        
    def connect(self):
        """Подключение консюмера с десериализацией JSON"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None
            )
            logger.info(f"Консюмер подключен к Kafka. Слушаем топик: {self.topic}")
        except Exception as e:
            logger.error(f"Ошибка подключения консюмера: {e}")
            raise
    
    def update_stats(self, order):
        """Обновление бизнес-метрик в реальном времени"""
        self.stats['total_orders'] += 1
        self.stats['total_revenue'] += order['total_amount']
        
        for item in order['items']:
            self.stats['orders_by_category'][item['category']] += 1
        
        city = order['customer']['city']
        self.stats['orders_by_city'][city] += 1
        
        self.stats['recent_orders'].append({
            'order_id': order['order_id'],
            'customer': order['customer']['name'],
            'total': order['total_amount']
        })
        if len(self.stats['recent_orders']) > 10:
            self.stats['recent_orders'].pop(0)
    
    def print_stats(self):
        """Красивый вывод текущих результатов в консоль"""
        logger.info("=" * 60)
        logger.info("📊 ТЕКУЩАЯ СТАТИСТИКА ПОТОКА ЗАКАЗОВ (REAL-TIME)")
        logger.info(f"Всего обработано заказов: {self.stats['total_orders']}")
        logger.info(f"Общая выручка компании:   {self.stats['total_revenue']:,.2f} руб.")
        
        avg_check = self.stats['total_revenue'] / self.stats['total_orders'] if self.stats['total_orders'] > 0 else 0
        logger.info(f"Средний чек покупателя:   {avg_check:,.2f} руб.")
        logger.info(f"Продажи по категориям:    {dict(self.stats['orders_by_category'])}")
        logger.info(f"География продаж (города): {dict(self.stats['orders_by_city'])}")
        logger.info("=" * 60)
    
    def run(self):
        self.connect()
        logger.info("Ожидаем поступления новых заказов из потока...")
        try:
            for message in self.consumer:
                order = message.value
                logger.info(f"🔥 Поймали новый заказ {order['order_id']} от клиента {order['customer']['name']}")
                self.update_stats(order)
                self.print_stats()
        except KeyboardInterrupt:
            logger.info("Консюмер остановлен пользователем.")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = OrderStatsConsumer()
    consumer.run()