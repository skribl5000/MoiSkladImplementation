from datetime import datetime, timedelta
import requests
import pandas as pd

class WBConnector:
    BASE_API_URL = 'https://suppliers-stats.wildberries.ru/api/v1/supplier/'

    def __init__(self, token, request_object):
        self.request_url = self._collect_url(request_object)
        self.params = {'key': token}

    def _collect_url(self, request_object):
        return self.BASE_API_URL + request_object

    def get_data_dict(self, date_from, params={}):
        if isinstance(date_from, datetime):
            date_from = self.format_date(date_from)

        if 'date_to' in params and isinstance(params['date_to'], datetime):
            params['date_to'] = self.format_date(params['date_to'])

        params.update(self.params)
        params.update({'dateFrom': date_from})


        response = requests.get(self.request_url, params=params)
        return response.json()

    def get_data_df(self, date_from, params={}):
        response_dict = self.get_data_dict(date_from, params=params)
        return pd.DataFrame(data=response_dict)

    @staticmethod
    def format_date(date):
        pattern = '%Y-%m-%d'
        return date.strftime(pattern)