import random
import sqlalchemy

from sqlalchemy.engine.base import Engine
from time import sleep

from database_utils import FileReader, Emulator


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
        engine = sqlalchemy.create_engine(f'mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4')
        return engine


new_connector = AWSDBConnector()

def run_infinite_post_data_loop() -> None:
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
            
            CONTENT_TYPE = 'application/json'

            Emulator.ingest_data(
                'pinterest_data', random_row, connection,
                CONTENT_TYPE, PIN_KINESIS_INVOKE_URL,
                True, 'streaming-0a966c04ad33-pin', 'partition-1')

            Emulator.ingest_data(
                'geolocation_data', random_row, connection,
                CONTENT_TYPE, GEO_KINESIS_INVOKE_URL,
                True, 'streaming-0a966c04ad33-geo', 'partition-1')

            Emulator.ingest_data(
                'user_data', random_row, connection,
                CONTENT_TYPE, USER_KINESIS_INVOKE_URL,
                True, 'streaming-0a966c04ad33-user', 'partition-1')


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')