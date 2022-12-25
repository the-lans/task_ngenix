from os import listdir
from os.path import join, isfile
import csv
import zipfile
from uuid import uuid4
from random import randint
from multiprocessing import Pool, cpu_count
from bs4 import BeautifulSoup

from backend.utils import random_string
from config import DATA_DIR, ZIP_NAME, XML_COUNT, ZIP_COUNT


class ZipWork:
    def __init__(self, name_levels: str = 'levels.csv', name_objects: str = 'objects.csv'):
        self.name_levels = join(DATA_DIR, name_levels)
        self.name_objects = join(DATA_DIR, name_objects)

    @staticmethod
    def create_xml() -> (str, BeautifulSoup):
        file_id = str(uuid4())
        soup = BeautifulSoup(features='xml')
        tag_root = soup.new_tag('root')

        tag_var = soup.new_tag('var')
        tag_var['name'], tag_var['value'] = 'id', file_id
        tag_root.append(tag_var)

        tag_var = soup.new_tag('var')
        tag_var['name'], tag_var['value'] = 'level', str(randint(1, 100))
        tag_root.append(tag_var)

        tag_objects = soup.new_tag('objects')
        for i in range(randint(1, 10)):
            tag_object = soup.new_tag('object')
            tag_object['name'] = random_string(30)
            tag_objects.append(tag_object)
        tag_root.append(tag_objects)

        soup.append(tag_root)
        return file_id, soup

    @staticmethod
    def create_zip(idx: int):
        zf = zipfile.ZipFile(join(DATA_DIR, f'{ZIP_NAME}_{idx}.zip'), mode='w', compression=zipfile.ZIP_DEFLATED)
        try:
            for xml_index in range(XML_COUNT):
                file_id, soup = ZipWork.create_xml()
                zf.writestr(f'{file_id}.xml', str(soup))
        finally:
            # print(f'{ZIP_NAME}_{idx}.zip')
            zf.close()

    @staticmethod
    def create_data():
        with Pool(cpu_count()) as pool:
            pool.map(ZipWork.create_zip, range(1, ZIP_COUNT + 1))

    @staticmethod
    def get_zip_names() -> list:
        return [join(DATA_DIR, f) for f in listdir(DATA_DIR) if isfile(join(DATA_DIR, f)) and f.endswith('.zip')]

    @staticmethod
    def parse_zip(name: str) -> list:
        zip_file = zipfile.ZipFile(name, 'r')
        return [ZipWork.parse_xml(zip_file.read(name)) for name in zip_file.namelist()]

    @staticmethod
    def parse_xml(data: bytes) -> dict:
        soup = BeautifulSoup(data, features='xml')
        tag_root = soup.root
        tag_id = tag_root.find('var', {'name': 'id'})
        tag_level = tag_root.find('var', {'name': 'level'})
        objects_list = [item['name'] for item in tag_root.objects.find_all('object')]
        return {'id': tag_id['value'], 'level': tag_level['value'], 'objects': objects_list}

    def parse_zip_files(self):
        zip_names_list = ZipWork.get_zip_names()
        with open(self.name_levels, 'w') as levels, open(self.name_objects, 'w') as objects:
            levels_writer = csv.writer(levels, lineterminator='\n', delimiter=';')
            objects_writer = csv.writer(objects, lineterminator='\n', delimiter=';')
            levels_writer.writerow(['id', 'level'])
            objects_writer.writerow(['id', 'object_name'])

            with Pool(cpu_count()) as pool:
                for result in pool.map(ZipWork.parse_zip, zip_names_list):
                    for xml in result:
                        levels_writer.writerow([xml['id'], xml['level']])
                        objects_writer.writerows([(xml['id'], obj) for obj in xml['objects']])
