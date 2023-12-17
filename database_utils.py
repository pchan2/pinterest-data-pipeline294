import json
import requests
import yaml

from bson import json_util
from sqlalchemy import text
from sqlalchemy.engine.base import Engine


class FileReader:
    '''
    This class is used to represent a yaml file reader.
    '''
    @staticmethod
    def read(filename: str) -> dict:
        '''
        This static method reads the yaml file.

        Args:
            filename (str): the yaml filename.

        Returns:
            db_creds (dict): the database credentials.
        '''
        with open(f'{filename}.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds


class Emulator:
    '''
    This class contains static helper methods that emulate the Pinterest
    data ingestion.
    '''
    @staticmethod
    def send_data(
        content_type: str, 
        http_method: str, 
        invoke_url: str, 
        data: json) -> None:
        '''
        This static method is used to send data by invoking a REST API
        in Amazon API Gateway.

        Args:
            content_type (str): the content type of HTTP headers.
            http_method (str): the HTTP method.
            invoke_url (str): the invoke URL.
            data (json): the payload.
        '''
        headers = {'Content-Type': content_type}
        response = requests.request(
            http_method, invoke_url, headers=headers, data=data)
        print(response.status_code)
        print(response.text)

    @staticmethod
    def map_pin_result(pin_result: dict) -> dict:
        '''
        This static method is used to map the pinterest data to the
        DataFrame headers.

        Args:
            pin_result (dict): the dictionary of a pinterest table row.
        
        Returns:
            _ (dict): the dictionary of a pinterest table row.
        '''
        return {
            'index': pin_result['index'],
            'unique_id': pin_result['unique_id'],
            'title': pin_result['title'],
            'description': pin_result['description'],
            'poster_name': pin_result['poster_name'],
            'follower_count': pin_result['follower_count'],
            'tag_list': pin_result['tag_list'],
            'is_image_or_video': pin_result['is_image_or_video'],
            'image_src': pin_result['image_src'],
            'downloaded': pin_result['downloaded'],
            'save_location': pin_result['save_location'],
            'category': pin_result['category'],
        }

    @staticmethod
    def map_geo_result(geo_result: dict) -> dict:
        '''
        This static method is used to map the geolocation data to the
        DataFrame headers.

        Args:
            geo_result (dict): the dictionary of a geolocation table
            row.
        
        Returns:
            _ (dict): the dictionary of a geolocation table row.
        '''
        return {
            'ind': geo_result['ind'],
            'timestamp': geo_result['timestamp'],
            'latitude': geo_result['latitude'],
            'longitude': geo_result['longitude'],
            'country': geo_result['country'],
        }

    @staticmethod
    def map_user_result(user_result: dict) -> dict:
        '''
        This static method is used to map the user data to the DataFrame
        headers.

        Args:
            user_result (dict): the dictionary of a user table row.
        
        Returns:
            _ (dict): the dictionary of a user table row.
        '''
        return {
            'ind': user_result['ind'],
            'first_name': user_result['first_name'],
            'last_name': user_result['last_name'],
            'age': user_result['age'],
            'date_joined': user_result['date_joined'],
        }

    @staticmethod
    def ingest_data(table_name: str,
                    random_row: int,
                    connection: Engine._connection_cls,
                    content_type: str,
                    invoke_url: str,
                    is_stream: bool = False,
                    stream_name: str = '',
                    partition_key: str = '') -> None:
        '''
        This static method is used to ingest data, importing data from a
        data source to a cloud-based storage medium e.g. Kafka or Kinesis, for 
        batch or stream processing.

        Args:
            table_name (str): the table name of the data source.
            random_row (int): the limit of the ``SELECT`` query.
            connection (Engine._connection_cls): the engine connection object.
            content_type (str): the content type of HTTP headers.
            invoke_url (str): the invoke URL.
            is_stream (bool): whether the data is a stream or a batch.
            partition_key (str) = the name of the partition key.
        '''
        query = text(
            f"SELECT * FROM {table_name} LIMIT {random_row}, 1")
        selected_row = connection.execute(query)

        for row in selected_row:
            result = dict(row._mapping)
            data = {}
            match table_name:
                case 'pinterest_data':
                    data = Emulator.map_pin_result(result)
                case 'geolocation_data':
                    data = Emulator.map_geo_result(result)
                case 'user_data':
                    data = Emulator.map_user_result(result)
                case _:
                    print('Invalid table name.')

            payload = {}
            http_method = ''
            if is_stream:
                payload = json.dumps({
                    'StreamName': stream_name,
                    'Data': data,
                    'PartitionKey': partition_key,
                }, default=json_util.default)
                http_method = 'PUT'
            else:
                payload = json.dumps({
                    'records': [{'value': data}]
                }, default=json_util.default)
                http_method = 'POST'

            Emulator.send_data(content_type, http_method, invoke_url, payload)

            print(result)