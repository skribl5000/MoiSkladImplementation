import requests
import json
import pandas as pd


class WBNomenclature:
    AUTH_LOGIN_URL = 'https://content-suppliers.wildberries.ru/passport/api/v2/auth/login'
    CARDS_URL = 'https://content-suppliers.wildberries.ru/card/list'

    def __init__(self, token, supplier_id):
        self.cookies = self.get_cookies(token)
        self.supplier_id = supplier_id

    def get_cookies(self, token):
        r = requests.post(
            self.AUTH_LOGIN_URL,
            headers={
                'Content-Type': 'Application/json',
                'Accept': 'Application/json'
            },
            data=json.dumps({
                "country": "RU",
                "device": "MacBookPro13",
                "token": token,
            })
        )
        return dict(r.cookies)

    def get_cards(self) -> list:
        r = requests.post(
            self.CARDS_URL,
            headers={
                'Content-Type': 'Application/json',
                'Accept': 'Application/json'
            },
            data=json.dumps({
                "id": 16041810,
                "jsonrpc": "2.0",
                "params": {
                    "supplierID": self.supplier_id,
                }
            }
            ),
            cookies=self.cookies)
        cards = r.json()['result']['cards']

        data = []
        for card in cards:
            for item in card['nomenclatures']:
                for variant in item['variations']:
                    item_data = self.get_all_params(card)
                    item_data.update(self.get_all_params(item))
                    item_data.update(self.get_all_params(variant))
                    data.append(item_data)
        return data

    def get_cards_dataframe(self) -> pd.DataFrame:
        data = self.get_cards()
        return pd.DataFrame(data)

    def get_cards_cleaned_dataframe(self) -> pd.DataFrame:
        df = self.get_cards_dataframe()

        df['Розница'] = 0

        new_columns = ['barcode', 'Бренд', 'vendorCode', 'object',
                       'Заголовок', 'countryProduction', 'Размер',
                       'Тнвэд', 'Розница', 'Комплектация', 'Основной цвет',
                       'supplierVendorCode', 'createdAt', 'Описание']

        columns_mapping = {'barcode': 'Баркод',
                           'Бренд': 'Бренд',
                           'vendorCode': 'Артикул цвета',
                           'object': 'Предмет',
                           'Заголовок': 'Наименование',
                           'countryProduction': 'Страна производитель',
                           'Размер': 'Размер на бирке',
                           'Тнвэд': 'ТНВЭД',
                           'Розница': 'Розничная цена',
                           'Комплектация': 'Комплектация',
                           'Основной цвет': 'Цвет',
                           'supplierVendorCode': 'Артикул поставщика',
                           'createdAt': 'Created'}

        df = df.filter(items=new_columns)
        df = df.rename(columns=columns_mapping)
        df = df.fillna('')
        return df

    def get_all_params(self, item):
        item_data = dict()
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, list) and key == 'addin':
                    for field in value:
                        field_value = self.get_values_from_params(field['params'])
                        item_data[field['type']] = field_value if field_value is not None else ''
                elif isinstance(value, list):
                    item_data.update(self.get_all_params(value))
                else:
                    item_data[key] = value

        elif isinstance(item, list):
            for element in item:
                item_data.update(self.get_all_params(element))

        return item_data

    @staticmethod
    def get_values_from_params(params):
        result = []
        for item in params:
            value = item.get('value')
            if value:
                result.append(value)

        return '/'.join(result)

    def get_card_photo_links(self) -> dict:
        """Returns dict like {'card_key':[photo_link1, photo_link2]}"""
        return dict()

    def get_single_items(self):
        df_nom = self.get_cards_cleaned_dataframe()
        df_single_items = df_nom[df_nom['Артикул поставщика'].isnull()].fillna('')
        df_single_items['Key'] = df_single_items['Артикул цвета'] + '_' + df_single_items['Баркод']
        return df_single_items

    def get_multi_items(self):
        df_nom = self.get_cards_cleaned_dataframe()
        df_multi_items = df_nom[~df_nom['Артикул поставщика'].isnull()].fillna('')
        df_multi_items['Key'] = df_multi_items["Артикул поставщика"] + '_' \
                                + df_multi_items["Артикул цвета"] \
                                + '_' + df_multi_items['Размер на бирке'] \
                                + '_' + df_multi_items['Баркод']
        return df_multi_items

    def get_multi_items_filtered_by_keys(self, keys):
        df_multi_items = self.get_multi_items()
        new_multi_items = df_multi_items[~df_multi_items['Key'].isin(keys)]
        return new_multi_items

    def get_single_items_filtered_by_keys(self, keys):
        df_single_items = self.get_single_items()
        new_single_items = df_single_items[~df_single_items['Key'].isin(keys)]
        return new_single_items