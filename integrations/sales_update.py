import json
import requests
from datetime import datetime, timedelta
import os
from pywb import WBConnector
from pymyskald import get_barcode_meta, MSDict

ms_token = os.getenv('MS_TOKEN')
wb_token_64 = os.getenv('WB_TOKEN_64')

with open('config.json') as config_file:
    config = json.loads(config_file.read())

def get_reporting_date_by_gap(days: int = 90) -> str:
    MAX_DAYS = 90
    DATE_PATTERN = '%Y-%m-%d'

    if days > MAX_DAYS:
        print(f'Dyas cannot be geather than {MAX_DAYS} ({days} got). Set max value - {MAX_DAYS}')
        days = MAX_DAYS

    reporting_date_from = datetime.today() - timedelta(days=days)

    return reporting_date_from.strftime(DATE_PATTERN)


reporting_date = get_reporting_date_by_gap(3)

sales = WBConnector(wb_token_64, 'sales')
df_sales = sales.get_data_df(reporting_date)
df_sales['date'] = df_sales['date'].apply(lambda x: x.replace('T', ' ') + '.000')


def get_return_request_data(row, config, token):
    product_meta = get_barcode_meta(row['barcode'], token)
    if product_meta is None:
        return

    request_data = {
        "name": row['saleID'],
        "description": "",
        "code": row['saleID'],
        "moment": row['date'],
        "applicable": True,
        "organization": {
            "meta": config['ORGANIZATIONS']['DEFAULT']
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
            "meta": config['AGENTS']['WBAgent']
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

def get_request_data_for_sale(sale_row, config, token):
    product_meta = get_barcode_meta(sale_row['barcode'], token)
    if product_meta is None:
        return

    request_data = {
        "name": f"{sale_row['saleID']}",
        "organization": {
            "meta": config['ORGANIZATIONS']['DEFAULT']
        },
        "store": {
            "meta": config['WAREHOUSES']['WB_FBO']
        },
        "agent": {
            "meta": config['AGENTS']['WBAgent']
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

    return request_data


exists_sales = MSDict('demand', token=ms_token).get_all_codes()
exists_returns = MSDict('salesreturn', token=ms_token).get_all_codes()
df_sales = df_sales[(~df_sales['saleID'].isin(exists_sales))&(~df_sales['saleID'].isin(exists_returns))]

error_barcodes = set()
for index, row in df_sales.iterrows():
    headers = {'Authorization': f'Basic {ms_token}', 'Content-Type': 'application/json'}

    if ('R' in row['saleID'] or 'D' in row['saleID']) and int(row['quantity']) < 0:
        request_data = get_return_request_data(row, config, ms_token)
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/salesreturn'
        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))
    elif 'S' in row['saleID'] and int(row['quantity']) > 0:
        request_data = get_request_data_for_sale(row, config, ms_token)
        request_url = 'https://online.moysklad.ru/api/remap/1.2/entity/demand'
        r = requests.post(request_url, headers=headers, data=json.dumps(request_data))
    else:
        r = None

    if r.status_code != 200:
        error_barcodes.add(row['barcode'])

print('Barcodes was not found:')
for barcode in error_barcodes:
    print(barcode)
