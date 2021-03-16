import pandas as pd
import json
import requests
from datetime import datetime, timedelta
import os
from pywb import WBConnector
ms_token = os.getenv('MS_TOKEN')
wb_token_64 = os.getenv('WB_TOKEN_64')



def get_reporting_date_by_gap(days: int) -> str:
    reporting_date = datetime.today() - timedelta(days=days)

    #FIXME: temporary solution for the beginning of usage.
    if reporting_date < datetime(2021, 3, 14):
        reporting_date = datetime(2021, 3, 14)

    pattern = '%Y-%m-%d'
    return reporting_date.strftime(pattern)


reporting_date = get_reporting_date_by_gap(3)

sales = WBConnector(wb_token_64, 'sales')
df_sales = sales.get_data_df(reporting_date)
df_sales['date'] = df_sales['date'].apply(lambda x: x.replace('T', ' ') + '.000')


def get_return_request_data(row, token):
    product_meta = get_product_meta(row['barcode'], token)
    if product_meta is None:
        return

    request_data = {
        "name": row['saleID'],
        "description": "",
        "code": row['saleID'],
        "moment": row['date'],
        "applicable": True,
        "organization": {
            "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/organization/82b868f6-58a3-11eb-0a80-022e004085fd",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/organization/metadata",
                "type": "organization",
                "mediaType": "application/json",
                "uuidHref": "https://online.moysklad.ru/app/#mycompany/edit?id=82b868f6-58a3-11eb-0a80-022e004085fd"
            }
        },
        "store": {
            "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/store/96fc2b3f-5905-11eb-0a80-050f00464994",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/store/metadata",
                "type": "store",
                "mediaType": "application/json",
                "uuidHref": "https://online.moysklad.ru/app/#warehouse/edit?id=96fc2b3f-5905-11eb-0a80-050f00464994"
            }
        },
        "agent": {
            "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/82b97cc6-58a3-11eb-0a80-022e00408602",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/metadata",
                "type": "counterparty",
                "mediaType": "application/json",
                "uuidHref": "https://online.moysklad.ru/app/#company/edit?id=82b97cc6-58a3-11eb-0a80-022e00408602"
            }
        },
        "positions": [
            {
                "quantity": -1 * row['quantity'],
                "price": -1 * row['forPay'] * 100,
                "discount": 0,
                "vat": 0,
                "assortment": {
                    "meta": product_meta,
                }
            }
        ],
    }
    return request_data

def get_product_meta(barcode, token):
    barcode = str(barcode)

    request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/product?search={barcode}'
    r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
    try:
        request_data = r.json()
    except Exception as e:
        return None

    if len(request_data.get('rows', [])) == 0:
        return get_variant_meta(barcode, token)

    for product in request_data['rows']:
        for code in product.get('barcodes', []):
            if code.get('ean13') == barcode:
                return product['meta']


def get_variant_meta(barcode, token):
    request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/variant?search={barcode}'
    r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
    try:
        request_data = r.json()
    except Exception as e:
        return None

    for variant in request_data['rows']:
        for code in variant.get('barcodes', []):
            if code.get('ean13') == barcode:
                return variant['meta']


def get_request_data_for_sale(sale_row, token):
    product_meta = get_product_meta(sale_row['barcode'], token)
    request_data = {
        "name": f"{sale_row['saleID']}",
        "organization": {
            "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/organization/82b868f6-58a3-11eb-0a80-022e004085fd",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/organization/metadata",
                "type": "organization",
                "mediaType": "application/json",
                "uuidHref": "https://online.moysklad.ru/app/#mycompany/edit?id=82b868f6-58a3-11eb-0a80-022e004085fd"
            }
        },
        "store": {
            "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/store/96fc2b3f-5905-11eb-0a80-050f00464994",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/store/metadata",
                "type": "store",
                "mediaType": "application/json",
                "uuidHref": "https://online.moysklad.ru/app/#warehouse/edit?id=96fc2b3f-5905-11eb-0a80-050f00464994"
            }
        },
        "agent": {
            "meta": {
                "href": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/82b97cc6-58a3-11eb-0a80-022e00408602",
                "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/counterparty/metadata",
                "type": "counterparty",
                "mediaType": "application/json",
                "uuidHref": "https://online.moysklad.ru/app/#company/edit?id=82b97cc6-58a3-11eb-0a80-022e00408602"
            }
        },
        "code": f"{sale_row['saleID']}",
        "moment": f"{sale_row['date']}",
        "applicable": True,
        "vatEnabled": True,
        "vatIncluded": True,
        "positions": [
            {
                "quantity": sale_row['quantity'],
                "price": sale_row['forPay'] * 100,
                "discount": 0,
                "vat": 0,
                "assortment": {
                    "meta": product_meta
                },
            }
        ]
    }
    if product_meta is not None:
        return request_data


def get_all_sale_codes(token):
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'
    r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
    request_data = r.json()
    result = [row['name'] for row in request_data['rows']]
    if request_data['meta']['size'] > 1000:
        size = request_data['meta']['size']
        for offset in range(1000, size - 1000, 1000):
            request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/demand?offset={offset}'
            r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
            request_data = r.json()
            result += [row['name'] for row in request_data['rows']]
    return result


def get_all_return_codes(token):
    request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/salesreturn'
    r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
    request_data = r.json()
    result = [row['name'] for row in request_data['rows']]
    if request_data['meta']['size'] > 1000:
        size = request_data['meta']['size']
        for offset in range(1000, size - 1000, 1000):
            request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/salesreturn?offset={offset}'
            r = requests.get(request_url, headers={'Authorization': f'Basic {token}'})
            request_data = r.json()
            result += [row['name'] for row in request_data['rows']]
    return result


exists_sales = get_all_sale_codes(ms_token)
exists_returns = get_all_return_codes(ms_token)
df_sales = df_sales[(~df_sales['saleID'].isin(exists_sales))&(~df_sales['saleID'].isin(exists_returns))]

error_barcodes = set()
for index, row in df_sales.iterrows():
    if ('R' in row['saleID'] or 'D' in row['saleID']) and int(row['quantity']) < 0\
            and row['saleID'] not in exists_returns:
        request_data = get_return_request_data(row, ms_token)
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/salesreturn'
        headers={'Authorization': f'Basic {ms_token}', 'Content-Type': 'application/json'}
        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))
    else:
        request_data = get_request_data_for_sale(row, ms_token)
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'
        headers = {'Authorization': f'Basic {ms_token}', 'Content-Type': 'application/json'}
        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))

    if r.status_code != 200:
        error_barcodes.add(row['barcode'])
print('Barcodes was not found:')
for barcode in error_barcodes:
    # TODO: Create simple modification for existed article.
    print(barcode)
