import requests
import json

from exceptions import *
from collections import Iterable

import urllib
from functools import lru_cache

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

    def find_by_field(self, field, value, exact_search=False):
        headers = {
            'Content-Type': 'Application/json',
            'Authorization': self.auth,
        }
        if exact_search:
            request_url = f'{self.URL}?filter={field}={value}'
        else:
            request_url = f'{self.URL}?filter={field}~{value}'
        r = requests.get(request_url, headers=headers)
        try:
            return r.json().get('rows', [])
        except Exception as e:
            print(str(e))
            return []

    def get_all_codes(self, batch_size=100):
        r = requests.get(self.URL, headers={'Authorization': self.auth})
        response_data = r.json()
        size = response_data['meta']['size']
        total = batch_size

        codes = [item['code'] for item in list(filter(lambda item: 'code' in item, response_data.get('rows', [])))]

        while total < size:
            r = requests.get(f'{self.URL}?limit={batch_size}&offset={total}',
                             headers={'Authorization': self.auth})
            response_data = r.json()
            codes += [item['code'] for item in response_data.json().get('rows', [])]
            total += batch_size

        return codes

    def create(self, request_data):
        r = requests.post(self.URL,
                          headers={'Authorization': self.auth, 'Content-Type': 'Application/json'},
                          json=request_data)
        return r

    def strict_search_by_field_value(self, field, value):
        items = requests.get(f'https://online.moysklad.ru/api/remap/1.2/entity/store?filter={field}={value}',
                                  headers={'Authorization': self.auth}).json()['rows']
        if len(items) == 0:
            return
        return items[0]


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


def get_product_meta_by_code(product_code, token):
    request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/product?filter=code={urllib.parse.quote_plus(product_code)}'
    response = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
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


def get_all_single_products_code_id_info(token) -> dict:
    """
    :param token: moisklad_token
    :return: dict where: key - code, value - id.
    """
    products_result = {}

    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/product?offset=0'
    response = requests.get(request_url, headers={
        'Authorization': f'Basic {token}'
    })

    products = response.json().get('rows', None)
    if products is None:
        return dict()

    for product in products:
        if product.get('variantsCount', 0) == 0:
            products_result[product['code']] = product['id']

    size = response.json()['meta']['size']
    if size > 1000:
        for offset in range(1000, size, 1000):
            request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/product?offset={offset}'
            response = requests.get(request_url, headers={
                'Authorization': f'Basic {token}'
            })

            products = response.json().get('rows', None)
            if products is None:
                return products_result

            for product in products:
                if product.get('variantsCount', 0) == 0:
                    products_result[product['code']] = product['id']

    return products_result


def get_all_multi_product_info(token) -> dict:
    """
    :param token: moisklad_token
    :return: dict as {key:tuple} where: key - code, value[0] - id, value[1] - product_meta
    """
    variant_code_id_meta = dict()
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/variant?offset=0'
    response = requests.get(request_url,
                            headers={'Authorization': f'Basic {token}'})

    variants = response.json().get('rows', None)
    if variants is None:
        return dict()

    for variant in variants:
        variant_code_id_meta[variant['code']] = variant['id']

    size = response.json()['meta']['size']
    if size > 1000:
        for offset in range(1000, size, 1000):
            request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/variant?offset={offset}'
            response = requests.get(request_url,
                                    headers={'Authorization': f'Basic {token}'})

            variants = response.json().get('rows', None)
            if variants is None:
                return variant_code_id_meta

            for variant in variants:
                variant_code_id_meta[variant['code']] = variant['id']

    return variant_code_id_meta


@lru_cache(50)
def get_item_by_barcode_id(barcode, token):
    product_dict = MSDict('product', token)
    variant_dict = MSDict('variant', token)

    products = product_dict.find_by_field('code', f'{barcode}')
    if len(products) >= 1:
        if len(products) > 1:
            print(f'TOO MUCH PRODUCTS BY BARCODE {barcode}, got first.')
        return products[0]

    variants = variant_dict.find_by_field('code', f'{barcode}')
    if len(variants) >= 1:
        if len(variants) > 1:
            print(f'TOO MUCH PRODUCTS BY BARCODE {barcode}, got first.')
        return variants[0]

    print(f'BARCODE {barcode} NOT FOUND!')
    return None


@lru_cache(10000)
def get_barcode_meta(barcode, token):
    item = get_item_by_barcode_id(barcode, token)
    if item is None:
        return

    meta = item.get('meta')
    return meta


def get_ms_stocks_by_store_meta(store_meta, ms_token):
    store_id = store_meta['href'].split('/')[-1]
    def get_stock_from_data(data):
        count = data['stockByStore'][0]['stock']
        product_meta = data['meta']['href']
        r = requests.get(product_meta, headers=ms_headers)
        product_data = r.json()
        code = product_data['code']
        if 'base' in code:
            print('base item')
            return None

        barcodes = product_data['barcodes']
        if len(barcodes) > 0 and 'ean13' in barcodes[0]:
            return {barcodes[0]['ean13']: int(count)}

    limit = 1000

    STORE_URL = f'https://online.moysklad.ru/api/remap/1.2/entity/store/{store_id}'
    ms_request_url = 'https://online.moysklad.ru/api/remap/1.2/report/stock/bystore'
    ms_headers = {'Authorization': f'Basic {ms_token}'}
    ms_params = {'filter': f'store={STORE_URL}',
                 'limit': limit,
                 'offset': 0
                 }
    r_ms = requests.get(ms_request_url, headers=ms_headers, params=ms_params)

    if r_ms.status_code != 200:
        print('МС не отдал остатки!')

    all_ms_stocks = r_ms.json()['rows']

    while ms_params['limit'] + ms_params['offset'] < r_ms.json()['meta']['size']:
        ms_params['offset'] += limit
        r_ms = requests.get(ms_request_url, headers=ms_headers, params=ms_params)
        if r_ms.status_code != 200:
            print('МС не отдал остатки!')
            break

        all_ms_stocks += r_ms.json()['rows']
    ms_stocks = {}
    for stock_json in all_ms_stocks:
        stock = get_stock_from_data(stock_json)
        if stock is not None:
            ms_stocks = {**ms_stocks, **stock}

    return {barcode: count for barcode, count in ms_stocks.items() if count != 0}
