import requests
import os
import json

token = os.getenv('MS_TOKEN')
moves_url = 'https://online.moysklad.ru/api/remap/1.2/entity/move/'
moves = requests.get(moves_url, headers={'Authorization': f'Basic {token}'}).json()['rows']
moves_to_move = []
for move in moves:
    if move['targetStore']['meta'][
        'href'] == 'https://online.moysklad.ru/api/remap/1.2/entity/store/be5071a7-61ae-11eb-0a80-06ae0001c060':
        to_move = False
        for attr in move.get('attributes', []):
            if attr['name'] == 'ID поставки':
                to_move = True
        if to_move:
            moves_to_move.append(move)

for move in moves_to_move:
    wb_store_meta = {
        "href": "https://online.moysklad.ru/api/remap/1.2/entity/store/96fc2b3f-5905-11eb-0a80-050f00464994",
        "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/store/metadata",
        "type": "store", "mediaType": "application/json",
        "uuidHref": "https://online.moysklad.ru/app/#warehouse/edit?id=96fc2b3f-5905-11eb-0a80-050f00464994"}

    move_meta = move['meta']
    move_id = move['id']
    request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/move/{move_id}'
    headers = {
        'Authorization': f'Basic {token}',
        'Content-Type': 'application/json'
    }
    request_data = {
        "targetStore": {"meta": {
            "href": "https://online.moysklad.ru/api/remap/1.2/entity/store/96fc2b3f-5905-11eb-0a80-050f00464994",
            "metadataHref": "https://online.moysklad.ru/api/remap/1.2/entity/store/metadata",
            "type": "store", "mediaType": "application/json",
            "uuidHref": "https://online.moysklad.ru/app/#warehouse/edit?id=96fc2b3f-5905-11eb-0a80-050f00464994"}}
    }

    r = requests.put(request_url, data=json.dumps(request_data), headers=headers)


