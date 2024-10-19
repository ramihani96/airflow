from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow_slack_notifications import airflow_notifications


def trigger_slack_alert(context):
    notifier = airflow_notifications()
    notifier.slack_alert(context=context)
    

with DAG(dag_id='airbyte_test',
         default_args={'owner': 'rami hani'},
         schedule_interval=None,
         tags=['test', 'airbyte'],
         on_success_callback=trigger_slack_alert,
         on_failure_callback=trigger_slack_alert

    ) as dag:

    test_airbyte = AirbyteTriggerSyncOperator(
        task_id='g_sheets_to_postgres',
        airbyte_conn_id='airbyte_conn',
        connection_id='5591dc67-807e-4dd9-93fd-2cb8aa4a4d4c',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )