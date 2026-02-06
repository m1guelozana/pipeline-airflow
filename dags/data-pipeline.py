from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import sqlite3
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'simple_etl_pipeline',
    default_args=default_args,
    description='A simple ETL pipeline example',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Database setup function
def setup_database():
    db_path = os.path.join(os.path.dirname(__file__), 'etl_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transformed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Truncate the table
    cursor.execute('DELETE FROM transformed_data')
    conn.commit()
    conn.close()

# Define the extract function
def extract_data():
    print("Extracting data from source...")
    # Simulate data extraction
    data = [1, 2, 3, 4, 5]
    return data

# Define the transform function
def transform_data(**context):
    print("Transforming data...")
    # Get data from previous task
    data = context['task_instance'].xcom_pull(task_ids='extract')
    # Simulate transformation
    transformed_data = [x * 2 for x in data]
    return transformed_data

# Define the load function
def load_data(**context):
    print("Loading data to SQLite database...")
    # Get transformed data from previous task
    data = context['task_instance'].xcom_pull(task_ids='transform')
    
    # Connect to database
    db_path = os.path.join(os.path.dirname(__file__), 'etl_data.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Insert data
    for value in data:
        cursor.execute('INSERT INTO transformed_data (value) VALUES (?)', (value,))
    
    conn.commit()
    conn.close()
    print(f"Successfully loaded {len(data)} records into the database")

# Define the tasks
start_task = EmptyOperator(task_id='start', dag=dag)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

end_task = EmptyOperator(task_id='end', dag=dag)

# Add database setup task
setup_db_task = PythonOperator(
    task_id='setup_database',
    python_callable=setup_database,
    dag=dag,
)

# Define the task dependencies
start_task >> setup_db_task >> extract_task >> transform_task >> load_task >> end_task
