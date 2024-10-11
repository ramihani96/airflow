import datetime
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow_slack_notifications import airflow_notifications


def trigger_slack_alert(context):
    notifier = airflow_notifications()
    notifier.slack_alert(context=context)

default_args = {
   'owner': 'rami hani',
   'depends_on_past': False,
   'start_date': datetime(2024, 10, 10),
}

# Define the DAG
with DAG(
    dag_id='test_postgres_connection',
    default_args=default_args,
    schedule_interval=None,  # This DAG runs manually or via trigger
    catchup=False,
    tags=['test', 'postgresql'],
    on_success_callback=trigger_slack_alert,
    on_failure_callback=trigger_slack_alert
) as dag:

    # Define the SQL to create a table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        created_at TIMESTAMP DEFAULT NOW()
    );
    """

    # Define the PostgresOperator task to create the table
    create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='data_warehouse',  # Make sure this matches your Airflow connection ID
        sql=create_table_sql,
    )

    # Task to insert random data into the table using generate_series
    insert_random_data_sql = """
    INSERT INTO test_table (name)
    SELECT 'name_' || gs AS name
    FROM generate_series(1, 100000) AS gs;  -- Change 100,000 to the desired number of entries
    """

    # Task to insert random data
    insert_data_task = PostgresOperator(
        task_id='insert_random_data_task',
        postgres_conn_id='data_warehouse',
        sql=insert_random_data_sql,
    )

    # Set the task dependencies
    create_table_task >> insert_data_task