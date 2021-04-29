import os
import requests
import json
import base64
import io
import logging

import pandas as pd
from tqdm import tqdm
from nomeclature import WBNomenclature
from pymyskald import get_all_single_products_code_id_info, get_all_multi_product_info


class ImageFormatException(Exception):
    pass


class Image:
    CORRECT_IMAGE_EXTENTIONS = ['img', 'jpg', 'jpeg']

    def __init__(self, url):
        self.url = url
        self.filename = self.get_filename_from_url(url)
        self.validate_filename(self.filename)

    def validate_filename(self, filename):
        if filename.split('.')[-1] not in self.CORRECT_IMAGE_EXTENTIONS:
            raise ImageFormatException(f"""incorrect extention for Image object: {filename.split(".")[-1]} """)

    @staticmethod
    def get_filename_from_url(url):
        url_parts = url.split('/')
        if len(url_parts) > 0:
            return url_parts[-1]
        return ''

    def __str__(self):
        return self.filename

    def __repr__(self):
        return self.filename

    @property
    def file_data(self):
        r = requests.get(self.url)
        image_bytes = io.BytesIO(r.content)
        return base64.b64encode(image_bytes.read())


def get_code_photo_dict_from_df_ph(df_ph: pd.DataFrame) -> dict:
    key_photo_dict_all = dict()
    for _, row in df_ph.iterrows():
        photos = []
        for url in row['Фото'].split(';'):
            if url != '':
                try:
                    photos.append(Image(url))
                except ImageFormatException as e:
                    print(str(e))

        key_photo_dict_all[row['Key']] = photos
    return key_photo_dict_all


def main():
    print('Script starts')
    ms_token = os.getenv('MS_TOKEN')
    wb_token = os.getenv('WB_TOKEN')
    supplier_id = os.getenv('SUPPLIER_ID')

    nom = WBNomenclature(wb_token, supplier_id)

    logging.info('Get nomenclature from WB')
    df_single_items = nom.get_single_items()
    df_single_items['Key'] = df_single_items['chrtId'] + '_' + df_single_items['Баркод']

    df_multi_items = nom.get_multi_items()
    df_multi_items['Key'] = df_multi_items['chrtId'] + '_' + df_multi_items['Баркод']

    df_s = df_single_items[['Key', 'Фото']]
    df_m = df_multi_items[['Key', 'Фото']]
    df_ph = df_s.append(df_m)

    if len(df_ph.Key) == len(set(df_ph.Key)):
        logging.info('Keys are unique')
    else:
        df_ph = df_ph.drop_duplicates()
        logging.warning('Duplicates in keys')

    key_photo_dict_all = get_code_photo_dict_from_df_ph(df_ph)
    prods_inf = get_all_single_products_code_id_info(ms_token)

    key_photo_dict = {code: photos for code, photos in key_photo_dict_all.items() if code in prods_inf}

    # upload photos for single items
    for code, photos in tqdm(key_photo_dict.items(), total=len(key_photo_dict)):
        product_id = prods_inf[code]
        request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/product/{product_id}/images'

        exists_images = requests.get(request_url, headers={'Authorization': f'Basic {ms_token}'}).json()
        exists_images = [image.get('filename', '') for image in exists_images.get('rows', [])]
        product_images = [image for image in key_photo_dict[code] if image.filename not in exists_images]

        for image in product_images:
            print(image.url, 'uploading')
            request_data = {"filename": image.filename,
                            "content": image.file_data.decode('utf-8')}
            r = requests.post(request_url, headers={
                'Authorization': f'Basic {ms_token}',
                'Content-type': 'Application/json'
            }, data=json.dumps(request_data))

            if r.status_code != 200:
                logging.error(r.status_code, r.text)

    # upload photos for multi items
    variant_code_id_meta = get_all_multi_product_info(ms_token)

    key_photo_dict = {code: photos for code, photos in key_photo_dict_all.items() if code in variant_code_id_meta}
    for code, photos in tqdm(key_photo_dict.items(), total=len(key_photo_dict)):
        variant_id = variant_code_id_meta[code]
        request_url = f'https://online.moysklad.ru/api/remap/1.2/entity/variant/{variant_id}/images'

        exists_images = requests.get(request_url, headers={
            'Authorization': f'Basic {ms_token}'}).json()
        exists_images = exists_images.get('rows', [])
        exists_images_filenames = [image.get('filename', '') for image in exists_images]
        product_images = [image for image in key_photo_dict[code] if image.filename not in exists_images_filenames]

        for image in product_images:
            request_data = {"filename": image.filename,
                            "content": image.file_data.decode('utf-8')}
            r = requests.post(request_url, headers={
                'Authorization': f'Basic {ms_token}',
                'Content-type': 'Application/json'
            }, data=json.dumps(request_data))

            if r.status_code != 200:
                logging.error(r.status_code, r.text)


if __name__ == "__main__":
    main()
