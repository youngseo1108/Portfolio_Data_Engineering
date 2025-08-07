# from airflow.sdk import DAG
from airflow.sdk import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pandas as pd


def log_failure(context):
  dag_run = context.get("dag_run")
  task = context.get("task_instance")
  print(f"Warning: Task {task.task_id} failed during run {dag_run}")

# Setting default arguments
default_args = {
  'owner': 'airflow',
  'retries': 1,
  'retry_delay': timedelta(minutes=5)
}


# Define the DAG
@dag(
  dag_id='merchant_etl_dag',
  default_args=default_args,
  start_date=datetime(2023, 11, 1),
  schedule='@daily',
  catchup=False
)


def merchant_etl_workflow():

  start = EmptyOperator(task_id='start')
  end = EmptyOperator(task_id='end')

  def load_data_to_postgres():
    '''
    Extract and load csv to postgres
    '''
    file_path = '/opt/airflow/dags/data/merchant_transactions.csv'
    df = pd.read_csv(file_path)
        
    pg_hook = PostgresHook(postgres_conn_id='transactions_connection')
    engine = pg_hook.get_sqlalchemy_engine()

    with engine.connect() as connection:
      df.to_sql(
        name='raw_transactions',
        con=connection,
        if_exists='append',
        index=False,
        method='multi'
      )

  def create_merchant_profiles():
    '''
    Transform the data
    '''
    pg_hook = PostgresHook(postgres_conn_id='transactions_connection')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
      INSERT INTO merchant_profiles (merchant_id, total_volume, avg_amount, transaction_count)
      SELECT
        merchant_id,
        SUM(amount),
        AVG(amount),
        COUNT(*)
      FROM raw_transactions
      GROUP BY merchant_id
      ON CONFLICT (merchant_id)
      DO UPDATE SET
        total_volume = EXCLUDED.total_volume,
        avg_amount = EXCLUDED.avg_amount,
        transaction_count = EXCLUDED.transaction_count;
    """)
    conn.commit()

  task_load_csv = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_data_to_postgres,
    on_failure_callback=log_failure
  )

  def validate_raw_data():
    '''data validation'''
    pg_hook = PostgresHook(postgres_conn_id='transactions_connection')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Check for nulls
    cursor.execute("SELECT COUNT(*) FROM raw_transactions WHERE merchant_id IS NULL OR amount IS NULL;")
    null_count = cursor.fetchone()[0]
    if null_count > 0:
      raise ValueError(f"There are {null_count} null values in raw_transactions")

    # Check for negative amounts
    cursor.execute("SELECT COUNT(*) FROM raw_transactions WHERE amount < 0;")
    neg_count = cursor.fetchone()[0]
    if neg_count > 0:
      raise ValueError(f"There are {neg_count} negative amounts")

    conn.close()

  task_validate = PythonOperator(
    task_id='validate_raw_data',
    python_callable=validate_raw_data
  )

  task_create_profiles = PythonOperator(
    task_id='create_merchant_profiles',
    python_callable=create_merchant_profiles
  )

  # Chain operations
  start >> task_load_csv >> task_validate >> task_create_profiles >> end


dag = merchant_etl_workflow()