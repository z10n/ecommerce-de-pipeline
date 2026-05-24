import pandas as pd
import os
from datetime import datetime

def extract_data():
    """
    Имитация экстракции данных. 
    В реальности здесь был бы запрос к API или чтение из S3.
    """
    print("📥 Extracting data...")
    
    # Используем локальный CSV для примера (или сгенерируем фейковые данные)
    # Для демо можно использовать pandas read_csv с ссылкой на Kaggle dataset
    try:
        df_orders = pd.read_csv('https://raw.githubusercontent.com/olistbr/brazilian-ecommerce/master/olist_orders_dataset.csv')
        df_items = pd.read_csv('https://raw.githubusercontent.com/olistbr/brazilian-ecommerce/master/olist_order_items_dataset.csv')
        
        # Сохраняем во временные файлы или возвращаем DataFrame
        return df_orders, df_items
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None, None

if __name__ == "__main__":
    orders, items = extract_data()
    if orders is not None:
        print(f"Extracted {len(orders)} orders and {len(items)} items.")