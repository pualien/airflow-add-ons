""" slack_operator.py
Possibly could use a better name. These are instead alert functions that
utilize the Slack operators. Extended from the medium article found in the README.
"""
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

# This should match the connection ID created in the Medium article
SLACK_CONN_ID = "slack"


def task_success_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of successful task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    conection = BaseHook.get_connection(SLACK_CONN_ID)
    slack_webhook_token = conection.password
    slack_msg = """*Status*: :white_check_mark: Task Succeeded!\n*Task*: {task}\n*Dag*: {dag}\n*Execution Time*: {exec_date}\n*Log Url*: {log_url}""".format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )

    if conection.extra_dejson.get('users'):
        slack_msg = slack_msg + '\n' + conection.extra_dejson.get('users')

    success_alert = SlackWebhookOperator(
        task_id="slack_task",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
        link_names=True
    )

    return success_alert.execute(context=context)


def task_fail_slack_alert(context):
    """
    Callback task that can be used in DAG to alert of failure task completion
    Args:
        context (dict): Context variable passed in from Airflow
    Returns:
        None: Calls the SlackWebhookOperator execute method internally
    """
    conection = BaseHook.get_connection(SLACK_CONN_ID)
    slack_webhook_token = conection.password
    slack_msg = """*Status*: :x: Task Failed\n*Task*: {task}\n*Dag*: {dag}\n*Execution Time*: {exec_date}\n*Log Url*: {log_url}""".format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        ti=context.get("task_instance"),
        exec_date=context.get("execution_date"),
        log_url=context.get("task_instance").log_url,
    )
    if conection.extra_dejson.get('users'):
        slack_msg = slack_msg + '\n' + conection.extra_dejson.get('users')

    failed_alert = SlackWebhookOperator(
        task_id="slack_task",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
        link_names=True
    )

    return failed_alert.execute(context=context)