import requests
import json
import pandas as pd
import base64

from requests.auth import HTTPBasicAuth
from exceptions import *


class MSResponseItem:
    def __init__(self, item_data: dict):
        self.data = item_data

    def get_meta(self):
        return self.data.get('meta', None)


class MSResponse:
    def __init__(self, response):
        self.response = response
        self.data = self._get_data_from_response(self.response)

    def __str__(self):
        return str(self.response.text)

    def get_meta(self):
        if self.response.json:
            return self.response.json().get('meta')

    @staticmethod
    def _get_data_from_response(response):
        if not response.json:
            return None
        return response.json().get('rows', None)

    def get_data(self):
        return self.data

    def find_item_by_attribute_value(self, attr, value):
        """
        Returns first matched element.
        """
        for item in self.data:
            if item.get(attr) == value:
                return MSResponseItem(item)

        return None

    def find_items_by_attribute_value(self, attr: str, value: str) -> list:
        """
        Returns all matched elements.
        """
        result_arr = []
        for item in self.data:
            if item.get(attr) == value:
                item = MSResponseItem(item)
                result_arr.append(item)

        return result_arr


class MSDict:
    """
    Native MS dictionary
    Встроенный справочник системы "Мой Склад"
    """
    BASE_URL = 'https://online.moysklad.ru/api/remap/1.2/entity/'

    def __init__(self, dict_name, login, password):
        self.auth = HTTPBasicAuth(login, password)
        self.response = self.get_response_by_dict_name(dict_name)
        self.data = self._get_data_from_response(self.response)
        self.name = dict_name

    def get_response_by_dict_name(self, dict_name):
        request_url = f'{self.BASE_URL}{dict_name}'
        r = requests.get(request_url, auth=self.auth)
        return r

    def __str__(self):
        return str(self.response.text)

    def get_meta(self):
        if self.response.json:
            return self.response.json().get('meta')

    @staticmethod
    def _get_data_from_response(response):
        if not response.json:
            return None
        return response.json().get('rows', None)

    def get_data(self):
        return self.data

    def find_item_by_attribute_value(self, attr, value) -> MSResponseItem:
        """
        Returns first matched element.
        """
        for item in self.data:
            if item.get(attr) == value:
                return MSResponseItem(item)

    def find_items_by_attribute_value(self, attr: str, value: str) -> list:
        """
        Returns all matched elements.
        """
        result_arr = []
        for item in self.data:
            if item.get(attr) == value:
                item = MSResponseItem(item)
                result_arr.append(item)

        return result_arr

    def create_item_by_name(self, item_name):
        request_url = f'{self.BASE_URL}{self.name}'
        headers = {
            'Content-Type': 'Application/json',
        }
        data = {
            'name': item_name,
        }
        r = requests.post(request_url, headers=headers, data=json.dumps(data), auth=self.auth)
        return r

    def create_or_get_item_by_name(self, item_name):
        item = self.find_item_by_attribute_value('name', item_name)
        if item is not None:
            return item

        new_item = self.create_item_by_name(item_name)
        return new_item


class MSUserDict:
    """
    Custom user dictionary
    Пользовательский справочник
    """
    def __init__(self, dict_id, login, password):
        self.id = dict_id
        self.auth = HTTPBasicAuth(login, password)

    def get_items(self):
        request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/customentity/{self.id}'
        r = requests.get(request_url, auth=self.auth)
        return r.json().get('rows', [])

    def create_item(self, item_name):
        if item_name == '':
            raise MSDictItemException('Name field of item cannot be empty')
        if self.is_item_exists(item_name):
            raise MSDictItemException(f'Item "{item_name}" is alreay exists in dictionary')

        request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/customentity/{self.id}'
        request_data = json.dumps({'name': item_name})
        headers = {
            'Content-Type': 'Application/json',
        }
        r = requests.post(request_url, data=request_data, headers=headers, auth=self.auth)
        return r.json()

    def find_item_by_name(self, item_name: str):
        item_name_lower_case = item_name.lower()
        items = self.get_items()
        for item in items:
            if item['name'].lower() == item_name_lower_case:
                return item
        return None

    def is_item_exists(self, item_name: str) -> bool:
        item = self.find_item_by_name(item_name)
        return item is not None

    def create_items_if_not_exists(self, items: list):
        for item_name in items:
            if item_name == '':
                continue
            if self.is_item_exists(item_name):
                continue
            self.create_item(item_name)

    def get_items_dict(self) -> dict:
        """
        Returns dict like {'name': {item_data}}
        """
        items = self.get_items()
        return {item['name']: item for item in items}

    def get_items_dict_filtered_by_names(self, names: list) -> dict:
        names = [name for name in names if name != '']
        items_dict = self.get_items_dict()
        for name in names:
            if name not in items_dict:
                raise MSDictItemException(f'Item "{name}" does not exists in dict')
        return {name: items_dict[name] for name in names}
