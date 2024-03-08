import sys
sys.path.append('airflow_pipeline')
import os
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.utils.dates import days_ago

from operators.fake_twitter_operator import FakeTwitterOperator


with DAG(
    dag_id = 'fake_twitter_dag',
    start_date = days_ago(6),
    schedule_interval = '@daily'
) as dag:

    query = 'datascience'

    ft_operator = FakeTwitterOperator(
        filepath = os.path.join(
            'datalake', 
            'fake_twitter',
            'extract_date={{ ds }}',
            'datascience_{{ ds_nodash }}.json'
        ),
        start_time = '{{ data_interval_start.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}', 
        end_time = '{{ data_interval_end.strftime("%Y-%m-%dT%H:%M:%S.00Z") }}', 
        query = query, 
        task_id = 'extract_fake_tweets'
    )
