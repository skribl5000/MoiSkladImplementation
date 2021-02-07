import requests
import json

from exceptions import *
from collections import Iterable


class MSResponseItem:
    def __init__(self, item_data: dict):
        self.data = item_data

    def get_meta(self):
        return self.data.get('meta', None)

    def get_attribute(self, attr):
        return self.data.get(attr, None)


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

    def __init__(self, dict_name, token):
        self.auth = f'Basic {token}'
        self.response = None
        self.data = None
        self.name = dict_name
        self.URL = f'{self.BASE_URL}{dict_name}'

    def set_response_by_dict_name(self):
        request_url = self.URL
        r = requests.get(request_url, headers={'Authorization': self.auth})
        self.response = r

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

    def set_data(self):
        if self.response is None:
            self.set_response_by_dict_name()

        self.data = self._get_data_from_response(self.response)
        return self.data

    def get_data(self):
        if self.data is not None:
            return self.data
        if self.response is None:
            self.set_response_by_dict_name()

        self.data = self._get_data_from_response(self.response)
        return self.data

    def find_item_by_attribute_value(self, attr, value) -> MSResponseItem:
        """
        Returns first matched element.
        """
        if self.data is None:
            self.set_data()

        for item in self.data:
            if item.get(attr) == value:
                return MSResponseItem(item)

    def find_items_by_attribute_value(self, attr: str, value: str) -> list:
        """
        Returns all matched elements.
        """
        if self.data is None:
            self.set_data()

        result_arr = []
        for item in self.data:
            if item.get(attr) == value:
                item = MSResponseItem(item)
                result_arr.append(item)

        return result_arr

    def create_item_by_name(self, item_name):
        request_url = self.URL
        headers = {
            'Content-Type': 'Application/json',
            'Authorization': self.auth,
        }
        data = {
            'name': item_name,
        }
        r = requests.post(request_url, headers=headers, data=json.dumps(data))
        return r

    def create_or_get_item_by_name(self, item_name):
        item = self.find_item_by_attribute_value('name', item_name)
        if item is not None:
            return item

        new_item = self.create_item_by_name(item_name)
        return new_item


class MSAttributesList(MSDict):
    def __init__(self, dict_name, token):
        super().__init__(dict_name, token)
        self.URL = f'{self.URL}/metadata/attributes'


class MSAttribute:
    def __init__(self, attribute_data):
        self.data = attribute_data

    def get_meta(self):
        return self.data.get('meta')

    def get_name(self):
        return self.data.get('name')

    def get_id(self):
        return self.data.get('id')


class MSUserDict:
    """
    Custom user dictionary
    Пользовательский справочник
    """

    def __init__(self, dict_id, token):
        self.id = dict_id
        self.token = token

    def get_items(self):
        request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/customentity/{self.id}'
        headers = {'Authorization': f'Basic {self.token}'}
        r = requests.get(request_url, headers=headers)
        return r.json().get('rows', [])

    def create_item(self, item_name):
        if item_name == '':
            raise MSDictItemException('Name field of item cannot be empty')
        if self.is_item_exists(item_name):
            raise MSDictItemException(f'Item "{item_name}" is already exists in dictionary')

        request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/customentity/{self.id}'
        request_data = json.dumps({'name': item_name})
        headers = {
            'Content-Type': 'Application/json',
            'Authorization': f'Basic {self.token}'
        }
        r = requests.post(request_url, data=request_data, headers=headers)
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

    def create_items_if_not_exists(self, items: Iterable):
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

    def get_items_dict_filtered_by_names(self, names: Iterable) -> dict:
        names = [name for name in names if name != '']
        items_dict = self.get_items_dict()
        for name in names:
            if name not in items_dict:
                raise MSDictItemException(f'Item "{name}" does not exists in dict')
        return {name: items_dict[name] for name in names}


def get_all_single_product_codes(auth) -> list:
    codes = []
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/product'
    headers = {'Authorization': auth}
    response = requests.get(request_url, headers=headers)

    products = response.json().get('rows', None)
    if products is None:
        return []

    for product in products:
        if product.get('variantsCount', 0) == 0:
            codes.append(product['code'])
    return codes


def get_all_product_codes(auth) -> list:
    codes = []
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/product'
    response = requests.get(request_url, headers={'Authorization': auth})

    products = response.json().get('rows', None)
    if products is None:
        return []

    for product in products:
        codes.append(product['code'])
    return codes


def get_all_multi_product_codes(auth) -> list:
    codes = []
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/variant?offset=0'
    response = requests.get(request_url, headers={'Authorization': auth})

    variants = response.json().get('rows', None)
    if variants is None:
        return []

    for variant in variants:
        codes.append(variant['code'])

    size = response.json()['meta']['size']
    if size > 1000:
        for offset in range(1000, size, 1000):
            request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/variant?offset={offset}'
            response = requests.get(request_url, headers={'Authorization': auth})

            variants = response.json().get('rows', None)
            if variants is None:
                return list(set(codes))

            for variant in variants:
                codes.append(variant['code'])

    return list(set(codes))


def get_product_meta_by_code(product_code, auth):
    request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/product?filter=code={product_code}'
    response = requests.get(request_url, headers={'Authorization': auth})
    response_dict = response.json()

    if response_dict.get('meta', None) is not None and response_dict['meta'].get('size', 0) > 0:
        return response_dict['rows'][0]['meta']

    return None


def get_product_attributes(token):
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes'
    r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
    ms_r = MSResponse(r)
    return ms_r


class MSVariants(MSDict):
    def __init__(self, dict_name, token):
        # FIXME: Фтигня какая-то по логике. Класс очевидно dict_name==variant, но наследование требует явного задания.
        super().__init__(dict_name, token)

    def get_chars_id_dict_for_list(self, values: list):
        values = set(values)
        result = dict()

        data = self.get_data()
        for variant in data:
            chars = variant.get('characteristics')
            if chars is not None:
                for char in chars:
                    if char.get('name') in values:
                        result[char.get('name')] = char.get('id')
                        values.remove(char.get('name'))
            if len(values) == 0:
                return result
        return result
