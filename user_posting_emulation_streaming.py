import requests
import random
import json
import sqlalchemy

from bson import json_util
from sqlalchemy import text
from sqlalchemy.engine.base import Engine
from time import sleep

from database_utils import FileReader


random.seed(100)


class AWSDBConnector:
    '''
    This class is used to represent an AWS database connector.
    '''
    db_creds = FileReader.read('credentials')

    def __init__(self):
        self.HOST = self.db_creds['HOST']
        self.USER = self.db_creds['USER']
        self.PASSWORD = self.db_creds['PASSWORD']
        self.DATABASE = self.db_creds['DATABASE']
        self.PORT = self.db_creds['PORT']
        
    def create_db_connector(self) -> Engine:
        '''
        This method initialises the SQLAlchemy engine with the database
        credentials.

        Returns:
            engine (Engine): the database engine.
        '''
        engine = sqlalchemy.create_engine(
                                f"mysql+pymysql://{self.USER}:
                                {self.PASSWORD}@{self.HOST}:
                                {self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    '''
    This method infinitely posts data from database to AWS Kinesis.
    '''
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        config = FileReader.read('config')
        PIN_KINESIS_INVOKE_URL = config['PIN_KINESIS_INVOKE_URL']
        GEO_KINESIS_INVOKE_URL = config['GEO_KINESIS_INVOKE_URL']
        USER_KINESIS_INVOKE_URL = config['USER_KINESIS_INVOKE_URL']

        with engine.connect() as connection:

            pin_string = text(
                f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                payload = json.dumps({
                    'StreamName': 'streaming-0a966c04ad33-pin',
                    'Data': {
                        'index': pin_result['index'], 
                        'unique_id': pin_result['unique_id'],
                        'title': pin_result['title'],
                        'description': pin_result['description'],
                        'poster_name': pin_result['poster_name'], 
                        'follower_count': pin_result['follower_count'],
                        'tag_list': pin_result['tag_list'],
                        'is_image_or_video': 
                            pin_result['is_image_or_video'],
                        'image_src': pin_result['image_src'], 
                        'downloaded': pin_result['downloaded'],
                        'save_location': pin_result['save_location'],
                        'category': pin_result['category'],
                    },
                    'PartitionKey': 'partition-1',
                }, default='json_serial')
                headers = {'Content-Type': 'application/json'}
                response = requests.request(
                    'PUT', PIN_KINESIS_INVOKE_URL, headers=headers, data=payload)
                print(response.status_code)
                print(response.text)

            geo_string = text(
                f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                payload = json.dumps({
                    'StreamName': 'streaming-0a966c04ad33-geo',
                    'Data': {
                        'ind': geo_result['ind'], 
                        'timestamp': geo_result['timestamp'],
                        'latitude': geo_result['latitude'],
                        'longitude': geo_result['longitude'],
                        'country': geo_result['country'],
                    },
                    'PartitionKey': 'partition-1',
                }, default=json_util.default)
                headers = {'Content-Type': 'application/json'}
                response = requests.request(
                    "PUT", GEO_KINESIS_INVOKE_URL, headers=headers, data=payload)
                print(response.status_code)
                print(response.text)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                payload = json.dumps({
                    'StreamName': 'streaming-0a966c04ad33-user',
                    'Data': {
                        'ind': user_result['ind'], 
                        'first_name': user_result['first_name'],
                        'last_name': user_result['last_name'],
                        'age': user_result['age'],
                        'date_joined': user_result['date_joined'],
                    },
                    'PartitionKey': 'partition-1',
                }, default=json_util.default)
                headers = {'Content-Type': 'application/json'}
                response = requests.request(
                    "PUT", USER_KINESIS_INVOKE_URL, headers=headers, data=payload)
                print(response.status_code)
                print(response.text)
            
            print(pin_result)
            print(geo_result)
            print(user_result)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


