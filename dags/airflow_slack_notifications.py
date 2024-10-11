import datetime
from datetime import timedelta
from airflow import DAG
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import json

class airflow_notifications:
    def __init__(self) -> None:
        pass

    def slack_alert(self, context=None, alerts='offline'):
        if context is None:
            # Handle the case where context is not provided
            # You can log or print a message indicating that context is missing
            print("Context is missing in slack_alert")
            return
        
        # Define the target slack channel
        channel = 'airflow-notifications'

        # Rest of your slack_alert method remains the same
        slack_webhook_token = BaseHook.get_connection('slack_con').password

        dag_run_state = context['dag_run'].get_state()
        print("dag run state: ", dag_run_state)

        # Slack Users Member IDs
        owners_mapping = json.loads(Variable.get("slack_member_ids"))
        
        airflow_owner_name = context.get('dag').owner.lower()  # Get DAG owner name and convert to lowercase
        # slack_owner_id = owners_mapping.get(airflow_owner_name, 'Unknown')  # Get corresponding Slack name from mapping
        slack_owner_ids = owners_mapping.get(airflow_owner_name, [])  # Get corresponding Slack names from mapping

        # Ensure slack_owner_ids is a list
        if not isinstance(slack_owner_ids, list):
            slack_owner_ids = [slack_owner_ids]

        # Construct mention string for each user in the list
        slack_mentions = "\n \t \t ".join([f"<@{user_id}>" for user_id in slack_owner_ids])

        # ti = context['ti']
        # task_state = ti.state
        
        if dag_run_state == 'success':
            slack_msg = f"""
            :white_check_mark: Dag Succeeded
            *Dag*: {context.get('task_instance').dag_id}
            *Owner*: {context.get('dag').owner}
            """
            
        elif dag_run_state =='failed':
            slack_msg = f"""
            :x: Dag Failed
            *Dag*: {context.get('task_instance').dag_id}
            *Owner*: {context.get('dag').owner}
            {slack_mentions}
            """        
        
        
        slack_alert = SlackWebhookOperator(
            task_id='airflow_alerts',
            # webhook_token=slack_webhook_token,
            message=slack_msg,
            channel=channel,
            username='airflow',
            slack_webhook_conn_id ='slack_con'
            )

        slack_alert.execute(context=context)