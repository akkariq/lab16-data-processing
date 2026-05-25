"""
ETL Pipeline для анализа продаж интернет-магазина
Этапы: Extract → Transform → Load → Visualize
Студент: Иванников С.С.
"""

import os
import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import logging

# Настройка логирования в консоль и в файл
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/etl.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class SalesETLPipeline:
    """ETL пайплайн для обработки данных о продажах"""
    
    def __init__(self, csv_path, db_path='sales.db'):
        self.csv_path = csv_path
        self.db_path = db_path
        self.raw_data = None
        self.cleaned_data = None
        self.aggregated_data = None
        
    def extract(self):
        """Этап 1: Извлечение данных из CSV-файла"""
        logger.info("--- НАЧАЛО ЭТАПА EXTRACT ---")
        try:
            if not os.path.exists(self.csv_path):
                raise FileNotFoundError(f"Файл {self.csv_path} отсутствует.")
            
            self.raw_data = pd.read_csv(self.csv_path)
            if self.raw_data.empty:
                raise ValueError("Файл CSV пуст.")
                
            logger.info(f"Успешно загружено: {len(self.raw_data)} строк, {len(self.raw_data.columns)} колонок")
            logger.info(f"Исходные колонки: {list(self.raw_data.columns)}")
        except Exception as e:
            logger.error(f"Ошибка на этапе извлечения данных: {e}")
            raise
        return self.raw_data
    
    def transform(self):
        """Этап 2: Трансформация и очистка данных"""
        logger.info("--- НАЧАЛО ЭТАПА TRANSFORM ---")
        df = self.raw_data.copy()
        
        # 1. Удаление полных дубликатов
        initial_rows = len(df)
        df.drop_duplicates(inplace=True)
        duplicated_removed = initial_rows - len(df)
        logger.info(f"Удалено полных дубликатов: {duplicated_removed} строк")
        
        # 2. Преобразование типов данных перед обработкой пропусков
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
        df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        
        # 3. Обработка пропусков (NaN)
        # Числовые — на медиану
        quantity_median = df['quantity'].median()
        price_median = df['price_per_unit'].median()
        
        df['quantity'] = df['quantity'].fillna(quantity_median)
        df['price_per_unit'] = df['price_per_unit'].fillna(price_median)
        
        # Текстовые — на "Unknown"
        text_cols = ['product_name', 'category', 'customer_name', 'customer_city', 'payment_method']
        for col in text_cols:
            df[col] = df[col].fillna("Unknown").str.strip()
            
        logger.info("Пропуски успешно заполнены (числовые -> медиана, текстовые -> 'Unknown')")
        
        # 4. Фильтрация аномалий (количество <= 0, цена <= 0)
        before_anomalies = len(df)
        df = df[(df['quantity'] > 0) & (df['price_per_unit'] > 0)]
        anomalies_removed = before_anomalies - len(df)
        logger.info(f"Удалено аномальных строк (quantity/price <= 0): {anomalies_removed}")
        
        # 5. Создание расчетной колонки total_amount
        df['total_amount'] = df['quantity'] * df['price_per_unit']
        
        # 6. Обогащение данных: выделение месяца month_year
        df['month_year'] = df['order_date'].dt.strftime('%Y-%m')
        df['month_year'] = df['month_year'].fillna("Unknown")
        
        self.cleaned_data = df
        logger.info(f"Трансформация завершена. Финальный размер датасета: {len(df)} строк")
        return self.cleaned_data
    
    def aggregate(self):
        """Этап 3: Агрегация данных для бизнес-аналитики"""
        logger.info("--- НАЧАЛО ЭТАПА AGGREGATE ---")
        df = self.cleaned_data.copy()
        
        # Группировка по категориям и периодам
        self.aggregated_data = df.groupby(['category', 'month_year']).agg({
            'quantity': 'sum',
            'total_amount': 'sum',
            'price_per_unit': 'mean',
            'order_id': 'nunique'
        }).rename(columns={
            'quantity': 'total_quantity',
            'total_amount': 'total_revenue',
            'price_per_unit': 'avg_price',
            'order_id': 'order_count'
        }).reset_index()
        
        # Округление средней цены
        self.aggregated_data['avg_price'] = self.aggregated_data['avg_price'].round(2)
        
        logger.info(f"Агрегация завершена. Сформировано {len(self.aggregated_data)} аналитических групп.")
        return self.aggregated_data
    
    def load_to_sqlite(self):
        """Этап 4: Загрузка очищенных и агрегированных данных в SQLite"""
        logger.info("--- НАЧАЛО ЭТАПА LOAD ---")
        try:
            engine = create_engine(f'sqlite:///{self.db_path}')
            
            # Запись очищенных данных в таблицу
            self.cleaned_data.to_sql('sales_cleaned', engine, if_exists='replace', index=False)
            # Запись агрегированных результатов
            self.aggregated_data.to_sql('sales_aggregated', engine, if_exists='replace', index=False)
            
            # Корректная проверка созданных таблиц через инспектор SQLAlchemy
            from sqlalchemy import inspect
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            logger.info(f"Данные успешно загружены в БД: {self.db_path}. Созданы таблицы: {tables}")
        except Exception as e:
            logger.error(f"Ошибка при загрузке данных в БД: {e}")
            raise
        
    def visualize(self):
        """Этап 5: Визуализация результатов аналитики"""
        logger.info("--- НАЧАЛО ЭТАПА VISUALIZE ---")
        df_agg = self.aggregated_data.copy()
        
        os.makedirs('report/graphs', exist_ok=True)
        sns.set_style("whitegrid")
        
        # Фильтруем неизвестные категории для чистоты графиков
        df_plot = df_agg[df_agg['category'] != 'Unknown']
        
        # 1. График выручки по категориям
        plt.figure(figsize=(10, 5))
        sns.barplot(data=df_plot, x='category', y='total_revenue', palette='viridis', hue='category', legend=False)
        plt.title('Общая выручка по категориям товаров (руб.)', fontsize=14, fontweight='bold')
        plt.xlabel('Категория товаров', fontsize=12)
        plt.ylabel('Выручка', fontsize=12)
        plt.tight_layout()
        plt.savefig('report/graphs/revenue_by_category.png', dpi=150)
        plt.close()
        
        # 2. Доля категорий в общей выручке (Pie Chart)
        plt.figure(figsize=(7, 7))
        pie_data = df_plot.groupby('category')['total_revenue'].sum()
        plt.pie(pie_data, labels=pie_data.index, autopct='%1.1f%%', colors=sns.color_palette('pastel'), startangle=140, textprops={'fontsize': 12})
        plt.title('Доля категорий в общей структуре выручки', fontsize=14, fontweight='bold')
        plt.tight_layout()
        plt.savefig('report/graphs/revenue_share_pie.png', dpi=150)
        plt.close()
        
        logger.info("Все графики успешно сгенерированы и сохранены в директорию report/graphs/")
        
    def run(self):
        """Запуск сквозного пайплайна"""
        logger.info("=" * 60)
        logger.info("ЗАПУСК СКВОЗНОГО ETL ПАЙПЛАЙНА ПРОДАЖ")
        logger.info("=" * 60)
        
        self.extract()
        self.transform()
        self.aggregate()
        self.load_to_sqlite()
        self.visualize()
        
        logger.info("ETL ПАЙПЛАЙН УСПЕШНО И БЕЗ ОШИБОК ЗАВЕРШИЛ СВОЮ РАБОТУ")
        logger.info("=" * 60)


if __name__ == "__main__":
    pipeline = SalesETLPipeline('sales.csv', 'sales.db')
    pipeline.run()