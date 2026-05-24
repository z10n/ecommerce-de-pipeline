from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import random
import uuid

default_args = {
    'owner': 'de_student',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ecommerce_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for e-commerce data',
    schedule_interval='@daily',
    catchup=False,
)

def extract_data_task(**kwargs):
    print("📥 Extracting data from source...")
    return "Extracted"

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_task,
    dag=dag,
)

def load_to_staging(**kwargs):
    """Генерирует и загружает mock-данные в staging"""
    print("📦 Loading mock data to Staging...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Очищаем staging перед загрузкой (идемпотентность)
    cursor.execute("TRUNCATE TABLE stg_orders;")
    cursor.execute("TRUNCATE TABLE stg_order_items;")

    # Генерируем 50 заказов
    orders_data = []
    items_data = []
    for i in range(1, 51):
        oid = f"ORD_{uuid.uuid4().hex[:8].upper()}"
        cid = f"CUST_{random.randint(100, 999)}"
        status = random.choice(["delivered", "shipped", "processing"])
        orders_data.append((oid, cid, status, "2024-05-01 10:00:00", "2024-05-01 12:00:00", "{}"))
        
        # 1-3 товара в заказе
        for _ in range(random.randint(1, 3)):
            pid = f"PROD_{random.randint(1000, 9999)}"
            sid = f"SELL_{random.randint(1, 50)}"
            price = round(random.uniform(50.0, 500.0), 2)
            freight = round(random.uniform(5.0, 25.0), 2)
            items_data.append((oid, pid, sid, "2024-05-02", price, freight))

    # Вставка в Postgres
    from psycopg2.extras import execute_values
    execute_values(cursor, 
        "INSERT INTO stg_orders (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, raw_json) VALUES %s", 
        orders_data)
    execute_values(cursor, 
        "INSERT INTO stg_order_items (order_id, product_id, seller_id, shipping_limit_date, price, freight_value) VALUES %s", 
        items_data)
    
    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ Loaded {len(orders_data)} orders & {len(items_data)} items to Staging")

load_staging_task = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    dag=dag,
)

def transform_and_load_to_core(**kwargs):
    print("🔄 Transforming and loading to Core...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Агрегация заказов в fact_orders
    cursor.execute("""
        INSERT INTO fact_orders (order_id, customer_id, order_status, purchase_date, total_amount)
        SELECT 
            o.order_id,
            o.customer_id,
            o.order_status,
            o.order_purchase_timestamp::date,
            SUM(i.price + i.freight_value) as total_amount
        FROM stg_orders o
        JOIN stg_order_items i ON o.order_id = i.order_id
        GROUP BY o.order_id, o.customer_id, o.order_status, o.order_purchase_timestamp
        ON CONFLICT (order_id) DO UPDATE SET total_amount = EXCLUDED.total_amount;
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Transformed data to Core")

transform_core_task = PythonOperator(
    task_id='transform_and_load_to_core',
    python_callable=transform_and_load_to_core,
    dag=dag,
)

def check_data_quality(**kwargs):
    print("🔍 Checking data quality...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    df = pg_hook.get_pandas_df("SELECT COUNT(*) as bad_rows FROM fact_orders WHERE total_amount IS NULL OR total_amount < 0")
    bad_count = df['bad_rows'][0]

    if bad_count > 0:
        raise ValueError(f"❌ Data Quality Check FAILED! Found {bad_count} bad rows.")
    print("✅ Data Quality Check PASSED!")

quality_check_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    dag=dag,
)

# 🔗 Зависимости
extract_task >> load_staging_task >> transform_core_task >> quality_check_task