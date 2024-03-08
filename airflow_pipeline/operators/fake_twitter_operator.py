import sys
sys.path.append('airflow_pipeline')
import os
from pathlib import Path

import json
from datetime import datetime, timedelta

from airflow.models import BaseOperator, DAG, TaskInstance

from hooks.fake_twitter_hook import FakeTwitterHook

class FakeTwitterOperator(BaseOperator):

    # Define the fields of this class that could be accessed by Jinja template
    template_fields = ['query', 'filepath', 'start_time', 'end_time']

    def __init__(self, filepath:str, start_time: str, end_time: str, query: str, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.start_time = start_time
        self.end_time = end_time
        self.query = query

    def execute(self, context):
        self.__create_folder()
        with open(self.filepath, 'w') as output_file:
            for page in FakeTwitterHook(self.start_time, self.end_time, self.query).run():
                json.dump(page, output_file, ensure_ascii = False)
                output_file.write('\n')

    def __create_folder(self):
        (Path(self.filepath).parent).mkdir(parents = True, exist_ok = True)

if __name__ == '__main__':
    start_time = (datetime.now() - timedelta(days= 1)).date()
    end_time = datetime.now()
    query = 'data science'
    with DAG(
        dag_id = 'fake_twitter_test',
        start_date = datetime.now(),
    ) as dag:
        ft_operator = FakeTwitterOperator(
            filepath = os.path.join(
                'datalake', 
                'fake_twitter',
                f'extract_date={datetime.now().date()}',
                f'{query}_{datetime.now().date().strftime("%Y%m%d")}.json'
            ),
            start_time = start_time, 
            end_time = end_time, 
            query = query, 
            task_id = 'test_run'
        )
        task_instance = TaskInstance(task = ft_operator)
        ft_operator.execute(task_instance.task_id)