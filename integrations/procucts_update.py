import os
import requests
import json
from tqdm import tqdm
try:
    from libs.pywb.nomeclature import WBNomenclature
    from libs.pymysklad.pymyskald import MSDict, get_product_attributes, MSVariants, MSUserDict, \
        get_all_multi_product_codes, get_all_product_codes, get_all_single_product_codes, get_product_meta_by_code
except ImportError:
    from nomeclature import WBNomenclature
    from pymyskald import MSDict, get_product_attributes, MSVariants, MSUserDict, \
        get_all_multi_product_codes, get_all_product_codes, get_all_single_product_codes, get_product_meta_by_code

class ProductCreator:
    DEFAULT_META_DICT = {
        'currency': 'руб',
        'uom': 'шт',
        'counterparty': 'ООО "Поставщик"',
    }

    def __init__(self, token, metas):
        self.metas = metas
        self.token = token
        self.default_meta_dict = self.get_default_meta_dict_by_dict()

    @staticmethod
    def get_default_meta_dict_by_dict():
        # FIXME: hardcode.
        result = dict()

        result['currency'] = {
            'href': 'https://online.moysklad.ru/api/remap/1.2/entity/currency/82b987d0-58a3-11eb-0a80-022e00408604',
            'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/currency/metadata',
            'type': 'currency',
            'mediaType': 'application/json',
            'uuidHref': 'https://online.moysklad.ru/app/#currency/edit?id=82b987d0-58a3-11eb-0a80-022e00408604'}

        result['counterparty'] = {
            'href': 'https://online.moysklad.ru/api/remap/1.2/entity/counterparty/82b97670-58a3-11eb-0a80-022e00408600',
            'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/counterparty/metadata',
            'type': 'counterparty',
            'mediaType': 'application/json',
            'uuidHref': 'https://online.moysklad.ru/app/#company/edit?id=82b97670-58a3-11eb-0a80-022e00408600'}

        result['uom'] = {
            'href': 'https://online.moysklad.ru/api/remap/1.2/entity/uom/19f1edc0-fc42-4001-94cb-c9ec9c62ec10',
            'metadataHref': 'https://online.moysklad.ru/api/remap/1.2/entity/uom/metadata',
            'type': 'uom',
            'mediaType': 'application/json'}

        return result

    def upload_single_item_from_nom_row(self, row, brands_map):
        auth_header = f'Basic {self.token}'
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/product'

        request_data = {
            "name": row['Артикул цвета'] + ' ' + row['Бренд'] + ' ' + row['Предмет'],
            "code": row['Key'],
            "article": row['Артикул цвета'],
            "externalCode": row['Артикул цвета'],
            "description": row['Описание'],
            "uom": {
                "meta": self.default_meta_dict['uom'],
            },
            "supplier": {
                "meta": self.default_meta_dict['counterparty']
            },
            "attributes": [
                {
                    "meta": self.metas['Цвет'],
                    "name": "Цвет",
                    "value": row['Цвет']
                },
                {
                    "meta": self.metas['Размер'],
                    "name": "Размер",
                    "value": row['Размер на бирке']
                },
                {
                    "meta": {
                        'href': 'https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes/417996a3-600f-11eb-0a80-03a10001b489',
                        'type': 'attributemetadata', 'mediaType': 'application/json'},
                    "value": brands_map[row['Бренд']],
                },
                {
                    "meta": self.metas['Баркод'],
                    "name": 'Баркод',
                    "value": row['Баркод']
                }
            ],
            "barcodes": [
                {
                    "ean13": row['Баркод']
                },
            ],
        }

        country_dict = MSDict('country', self.token)
        country_meta = country_dict.find_item_by_attribute_value('name', row['Страна производитель'])
        if country_meta is not None:
            country_meta = country_meta.get_meta()
        if country_meta is not None:
            request_data['country'] = {'meta': country_meta}

        headers = {
            'Authorization': auth_header,
            'Content-Type': 'Application/json',
        }
        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))
        return r

    def upload_base_item_from_nom_row(self, row, brands_map):
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/product'

        request_data = {
            "name": row['Артикул поставщика'] + ' ' + row['Бренд'] + ' ' + row['Предмет'],
            "code": row['Артикул поставщика'] + '_base',
            "externalCode": row['Артикул поставщика'],
            "description": row['Описание'],
            "article": row['Артикул поставщика'],
            "uom": {
                "meta": self.default_meta_dict['uom'],
            },
            "supplier": {
                "meta": self.default_meta_dict['counterparty']
            },
            "attributes": [
                {
                    "meta": {
                        'href': 'https://online.moysklad.ru/api/remap/1.2/entity/product/metadata/attributes/417996a3-600f-11eb-0a80-03a10001b489',
                        'type': 'attributemetadata', 'mediaType': 'application/json'},
                    "value": brands_map[row['Бренд']],
                },
            ],
            "barcodes": [
                {
                    "ean8": str(20000011),
                },
            ],
        }
        country_dict = MSDict('country', self.token)
        country_meta = country_dict.find_item_by_attribute_value('name', row['Страна производитель'])
        if country_meta is not None:
            country_meta = country_meta.get_meta()
        if country_meta is not None:
            request_data['country'] = {'meta': country_meta}

        headers = {
            'Content-Type': 'Application/json',
            'Authorization': f'Basic {self.token}'
        }
        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))
        return r

    def add_modification_to_product(self, product_meta, row, char_dict):
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/variant'
        variants = []
        if row['Артикул цвета'] != '' and row['Артикул цвета'] is not None:
            variants.append({
                "id": char_dict['Цвет'],
                "value": row['Артикул цвета'],
            })

        if row['Размер на бирке'] != '' and row['Размер на бирке'] is not None:
            variants.append({
                "id": char_dict['Размер'],
                "value": row['Размер на бирке'],
            })
        variants.append({
            "id": char_dict['Баркод'],
            "value": row['Баркод'],
        })

        request_data = {
            "name": f"{row['Размер на бирке']} {row['Артикул цвета']}",
            "barcodes": [
                {
                    "ean13": row['Баркод']
                },
            ],
            "product": {
                "meta": product_meta
            }
        }

        if len(variants) > 0:
            request_data["characteristics"] = variants
            request_data["code"] = row['Key']

        headers = {
            'Content-Type': 'Application/json',
            'Authorization': f'Basic {self.token}'
        }

        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))
        return r


def main():
    print('Script starts')
    ms_token = os.getenv('MS_TOKEN')
    wb_token = os.getenv('WB_TOKEN')
    supplier_id = os.getenv('SUPPLIER_ID')

    ms_auth = f'Basic {ms_token}'
    nom = WBNomenclature(wb_token, supplier_id)
    print('Get meta data from MS')
    # TODO: функционал получения айдишников по имени, хардкод - плохо.
    brand_dict_id = '3f3169c7-600f-11eb-0a80-069d0001bb3c'
    producer_country_dict_id = '214623e3-600f-11eb-0a80-07c20001a5c4'
    brands_dict = MSUserDict(brand_dict_id, ms_token)
    producer_countries_dict = MSUserDict(producer_country_dict_id, ms_token)

    product_attrs = get_product_attributes(ms_token)

    color_meta = product_attrs.find_item_by_attribute_value('name', 'Основной цвет').get_meta()
    size_meta = product_attrs.find_item_by_attribute_value('name', 'Размер').get_meta()
    brand_meta = product_attrs.find_item_by_attribute_value('name', 'Бренд').get_meta()
    barcode_meta = product_attrs.find_item_by_attribute_value('name', 'Баркод').get_meta()

    metas = {
        'Цвет': color_meta,
        'Размер': size_meta,
        'Brand': brand_meta,
        'Баркод': barcode_meta,
    }

    variants = MSVariants('variant', ms_token)
    char_names = ['Размер', 'Цвет', 'Баркод']
    char_dict = variants.get_chars_id_dict_for_list(char_names)

    print('Get codes from MS')
    multi_products_codes = get_all_multi_product_codes(ms_auth)
    single_product_codes = get_all_single_product_codes(ms_auth)
    product_codes = get_all_product_codes(ms_auth)

    print('Get nomenclature from WB')
    new_single_items = nom.get_single_items_filtered_by_keys(single_product_codes)
    new_multi_items = nom.get_multi_items_filtered_by_keys(multi_products_codes)

    print('Add new values to MS dicts')
    brands = set(list(new_single_items['Бренд']) + list(new_multi_items['Бренд']))
    countries = set(list(new_single_items['Страна производитель']) + list(new_multi_items['Страна производитель']))

    brands_dict.create_items_if_not_exists(brands)
    producer_countries_dict.create_items_if_not_exists(countries)

    brands_map = brands_dict.get_items_dict_filtered_by_names(brands)

    creator = ProductCreator(ms_token, metas)
    print('Creating new items..')
    error_rows = []
    for index, row in tqdm(new_single_items.iterrows(), total=len(new_single_items)):
        try:
            r = creator.upload_single_item_from_nom_row(row, brands_map)
            if r.status_code != 200:
                print(r.json())
        except Exception as e:

            print(str(e))
            error_rows.append(row)

    for article in tqdm(new_multi_items['Артикул поставщика'].unique(),
                        total=len(new_multi_items['Артикул поставщика'].unique())):
        df_item = new_multi_items[new_multi_items['Артикул поставщика'] == article]
        item_row = df_item.iloc[0]

        if item_row['Артикул поставщика'] + '_base' in product_codes:
            product_meta = get_product_meta_by_code(item_row['Артикул поставщика'] + '_base', ms_token)
        else:
            r = creator.upload_base_item_from_nom_row(item_row, brands_map)
            if 'errors' in r.json():
                print(f'error to upload row {item_row}')
                print(r.text)
                print(r.json()['errors'])
                continue

            product = r.json()
            product_meta = product['meta']

        for index, row in df_item.iterrows():
            if row['Баркод'] == '':
                print(f'Пустой баркод у {row["Артикул цвета"]}. Предмет не создан')
                continue
            r = creator.add_modification_to_product(product_meta, row, char_dict)
            if r.status_code != 200:
                if product_meta is None:
                    print(f'Product meta is None у {row["Артикул цвета"]}. Предмет не создан')
                else:
                    raise Exception(str(r.json()))

if __name__ == "__main__":
    main()
