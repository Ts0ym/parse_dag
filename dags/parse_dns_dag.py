from dns_parser import DNSParser
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
from airflow.hooks.postgres_hook import PostgresHook
import logging
import os
import pickle
from datetime import datetime
import json


def create_table(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS products (
        id SERIAL PRIMARY KEY,
        product_link TEXT,
        price INTEGER,
        image_link TEXT,
        rating FLOAT,
        service_rating TEXT,
        name TEXT,
        stats TEXT,
        category TEXT
    );
    """

    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Table created successfully.")


def parse_products(catalog_path: str, **context):
    parser = DNSParser()
    try:
        parsed_items = parser.parse_catalog_category(catalog_path)

        tmp_file_path = f"/opt/airflow/db/parsed_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.pkl"
        with open(tmp_file_path, 'wb') as tmp_file:
            pickle.dump(parsed_items, tmp_file)

        context['ti'].xcom_push(key='tmp_file_path', value=tmp_file_path)

    except Exception as e:
        print(f"En error occurred {e}")


def load_products(**context):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    tmp_file_path = context['ti'].xcom_pull(task_ids='parse_products', key='tmp_file_path')
    with open(tmp_file_path, 'rb') as tmp_file:
        parsed_data = pickle.load(tmp_file)

    for product in parsed_data:
        cursor.execute(
            """
            INSERT INTO products (
            product_link, 
            price, 
            image_link, 
            rating, 
            service_rating, 
            name, 
            stats, 
            category
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (product.dns_product_link, product.price, product.image_link, product.rating, product.service_rating,
             product.name, json.dumps(product.stats), product.category)
        )

    conn.commit()
    cursor.close()
    conn.close()
    os.remove(tmp_file_path)
    logging.info("Data saved to PostgreSQL successfully.")


default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today(),
    'retries': 1,
}

dag = DAG(
    "parse_dns_dag",
    description='A DAG for parsing DNS catalog and saving results',
    default_args=default_args,
    schedule_interval="@daily"
)

create_table = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag
)

parse_products = PythonOperator(
    task_id="parse_products",
    python_callable=parse_products,
    op_kwargs={
        "catalog_path": "17a892f816404e77/noutbuki/"
    },
    dag=dag
)

load_products = PythonOperator(
    task_id="load_products",
    python_callable=load_products,
    dag=dag
)


create_table >> parse_products >> load_products
