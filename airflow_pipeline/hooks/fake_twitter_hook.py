from typing import List
from datetime import datetime, timedelta
import json

from airflow.providers.http.hooks.http import HttpHook
import requests

class FakeTwitterHook(HttpHook):

    def __init__(self, start_time: str, end_time: str, query: str, conn_id = None) -> None:
        self.conn_id = conn_id or 'fake_twitter_default'
        super().__init__(http_conn_id = self.conn_id)

        self.end_time = end_time
        self.start_time = start_time
        self.query = query

    def run(self):
        session = self.get_conn()
        url = self.__create_url()
        return self.__paginate(session, url)

    def __create_url(self) -> str:
        tweet_fields = 'tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text'
        user_fields = 'expansions=author_id&user.fields=id,name,username,created_at'
        return f'{self.base_url}/2/tweets/search/recent?query={self.query}&{tweet_fields}&{user_fields}&start_time={self.start_time}&end_time={self.end_time}'
    
    def __connect_to_endpoint(self, session, url):
        request = requests.Request('GET', url)
        prep_req = session.prepare_request(request)
        self.log.info(f'URL: {url}')
        return self.run_and_check(session, prep_req, {})
    
    def __paginate(self, session, url) -> List[str]:
        responses = []
        response = self.__connect_to_endpoint(session, url)
        json_response = response.json()
        responses.append(json_response)
        while json_response.get('next_token'):
            next_token = json_response['meta']['next_token']
            next_url = f'{url}&next_token={next_token}'
            response = self.__connect_to_endpoint(session, next_url)
            json_response = response.json()
            responses.append(json_response)
        return responses

if __name__ == '__main__':
    start_time = (datetime.now() - timedelta(days= 1)).date()
    end_time = datetime.now()
    query = 'data science'
    for page in FakeTwitterHook(start_time, end_time, query).run():
        print(json.dumps(page, indent = 4, sort_keys = True))