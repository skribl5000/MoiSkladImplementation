import os
from collections import defaultdict

from pymyskald import get_ms_stocks_by_store_meta, MSDict, get_barcode_meta
from pywb import WBConnector
from datetime import datetime, timedelta
from tqdm import tqdm
import json


def generate_losses_data(losses_dict: dict, config, token, store_meta):
    positions = []
    for barcode, quantity in tqdm(losses_dict.items()):
        position_meta = get_barcode_meta(barcode, token)

        if position_meta is None:
            print(f'{barcode} not found in MS. sync item failed')
            continue

        position_data = {"quantity": -quantity, "assortment": {"meta": position_meta}}
        positions.append(position_data)

    request_data = {
        "store": {
            "meta": store_meta
        },
        "organization": {
            "meta": config['ORGANIZATIONS']['DEFAULT']
        },
        "positions": positions
    }
    return request_data


def generate_supplies_data(supplies_dict: dict, config, token, store_meta):
    positions = []
    for barcode, quantity in tqdm(supplies_dict.items()):

        position_meta = get_barcode_meta(barcode, token)

        if position_meta is None:
            print(f'{barcode} not found in MS. sync item failed')
            continue

        position_data = {"quantity": quantity,
                         "assortment": {"meta": position_meta},
                         "price": 0,
                         "discount": 0,
                         "vat": 0}
        positions.append(position_data)

    request_data = {
        "applicable": True,
        "vatEnabled": True,
        "vatIncluded": True,
        "organization": {
            "meta": config['ORGANIZATIONS']['DEFAULT']
        },
        "agent": {
            "meta": config['AGENTS']['WBAgent']
        },
        "store": {
            "meta": store_meta
        },
        "positions": positions
    }
    return request_data


def get_reporting_date_by_gap(days: int = 90) -> str:
    MAX_DAYS = 365
    DATE_PATTERN = '%Y-%m-%d'

    if days > MAX_DAYS:
        print(f'Days cannot be more than {MAX_DAYS} ({days} got). Set max value - {MAX_DAYS}')
        days = MAX_DAYS

    reporting_date_from = datetime.today() - timedelta(days=days)

    return reporting_date_from.strftime(DATE_PATTERN)


if __name__ == '__main__':
    print('Sync started...')
    ms_token = os.getenv('MS_TOKEN')
    wb_token_64 = os.getenv('WB_TOKEN_64')
    with open('../config.json') as config_file:
        config = json.loads(config_file.read())
    store_dict = MSDict('store', ms_token)

    wb_connector = WBConnector(wb_token_64, 'stocks')

    print('Read WB data...')
    wb_stocks_df = wb_connector.get_data_df(get_reporting_date_by_gap(365)).fillna('')
    wb_stocks_df = wb_stocks_df[wb_stocks_df['barcode'] != '']
    for store in wb_stocks_df['warehouseName'].unique():
        print('STORE:', store)
        wb_stocks_store_df = wb_stocks_df[wb_stocks_df['warehouseName'] == store]

        wb_stocks = wb_stocks_store_df.groupby('barcode').agg({'quantityNotInOrders': 'sum'})['quantityNotInOrders'].to_dict()
        wb_stocks = defaultdict(int, wb_stocks)

        print('Read MS Data...')
        store_object = store_dict.strict_search_by_field_value('name', f'[WB] {store}')
        if store_object is None:
            print(store, 'Not found')
            continue
        else:
            store_meta = store_object['meta']

        ms_stocks = get_ms_stocks_by_store_meta(store_meta, ms_token)
        ms_stocks = defaultdict(int, ms_stocks)

        all_barcodes = set(ms_stocks.keys()) | set(wb_stocks.keys())
        compare_dict = {
            barcode: wb_stocks[barcode] - ms_stocks[barcode]
            for barcode in all_barcodes
        }

        supplies = {barcode: difference for barcode, difference in compare_dict.items() if difference > 0}
        losses = {barcode: difference for barcode, difference in compare_dict.items() if difference < 0}

        print('Generating supply request')
        supplies_data = generate_supplies_data(supplies, config, ms_token, store_meta)
        print('Generating losses request')
        losses_data = generate_losses_data(losses, config, ms_token, store_meta)

        supply_ms_dict = MSDict('supply', ms_token)
        losses_ms_dict = MSDict('loss', ms_token)
        if len(supplies_data['positions']) > 0:
            if len(supplies_data['positions']) < 500:
                result_supplies = supply_ms_dict.create(supplies_data)
            if len(supplies_data['positions']) >= 500:
                data = supplies_data.copy()
                data['positions'] = data['positions'][:500]
                result_supplies = supply_ms_dict.create(data)
                print(f'Incomes result:', result_supplies)

                data = supplies_data.copy()
                data['positions'] = data['positions'][500:1000]
                result_supplies = supply_ms_dict.create(data)
                print(f'Incomes result:', result_supplies)

                if len(supplies_data['positions']) > 1000:
                    data = supplies_data.copy()
                    data['positions'] = data['positions'][1000:1500]
                    result_supplies = supply_ms_dict.create(data)
                    print(f'Incomes result:', result_supplies)

        if len(losses_data['positions']) > 0:
            result_loses = losses_ms_dict.create(losses_data)
            print(f'Losses result:', result_loses.status_code)
