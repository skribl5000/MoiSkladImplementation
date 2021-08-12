import os
from collections import defaultdict

from pymyskald import get_ms_stocks_by_store_id, MSDict, get_barcode_meta
from pywb import WBConnector
from datetime import datetime, timedelta
from tqdm import tqdm
import json


def generate_losses_data(losses_dict: dict, config, token):
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
            "meta": config['MS_WAREHOUSES']['WB_FBO']
        },
        "organization": {
            "meta": config['ORGANIZATIONS']['DEFAULT']
        },
        "positions": positions
    }
    return request_data


def generate_supplies_data(supplies_dict: dict, config, token):
    positions = []
    counter=0
    for barcode, quantity in tqdm(supplies_dict.items()):
        counter += 1
        if counter > 500:
            break

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
            "meta": config['MS_WAREHOUSES']['WB_FBO']
        },
        "positions": positions
    }
    return request_data


def get_reporting_date_by_gap(days: int = 90) -> str:
    MAX_DAYS = 90
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
    with open('config.json') as config_file:
        config = json.loads(config_file.read())

    WB_FBO_STORE_ID = config['MS_WAREHOUSES']['WB_FBO']['href'].split('/')[-1]

    wb_connector = WBConnector(wb_token_64, 'stocks')

    print('Read WB data...')
    wb_stocks_df = wb_connector.get_data_df(get_reporting_date_by_gap()).fillna('')
    wb_stocks_df = wb_stocks_df[wb_stocks_df['barcode'] != '']
    wb_stocks = wb_stocks_df.groupby('barcode').agg({'quantity': 'sum'})['quantity'].to_dict()
    wb_stocks = defaultdict(int, wb_stocks)

    print('Read MS Data...')
    ms_stocks = get_ms_stocks_by_store_id(WB_FBO_STORE_ID, ms_token)
    ms_stocks = defaultdict(int, ms_stocks)

    print('Calculate corrections...')
    all_barcodes = set(ms_stocks.keys()) | set(wb_stocks.keys())
    compare_dict = {
        barcode: wb_stocks[barcode] - ms_stocks[barcode]
        for barcode in all_barcodes
    }

    supplies = {barcode: difference for barcode, difference in compare_dict.items() if difference > 0}
    losses = {barcode: difference for barcode, difference in compare_dict.items() if difference < 0}

    print('Generating supply request')
    supplies_data = generate_supplies_data(supplies, config, ms_token)
    print('Generating losses request')
    losses_data = generate_losses_data(losses, config, ms_token)

    supply_ms_dict = MSDict('supply', ms_token)
    losses_ms_dict = MSDict('loss', ms_token)

    print('Pushing corrections...')
    result_supplies = supply_ms_dict.create(supplies_data)
    result_loses = losses_ms_dict.create(losses_data)

    print(f'Incomes result:',  result_supplies)
    print(f'Losses result:', result_loses.status_code)
