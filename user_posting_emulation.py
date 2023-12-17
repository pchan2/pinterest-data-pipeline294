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
        engine = sqlalchemy.create_engine(
            f'mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4')
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop() -> None:
    '''
    This method infinitely posts data from database to AWS Kafka topics.
    '''
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()
        config = FileReader.read('config')
        PIN_INVOKE_URL = config['PIN_INVOKE_URL']
        GEO_INVOKE_URL = config['GEO_INVOKE_URL']
        USER_INVOKE_URL = config['USER_INVOKE_URL']

        with engine.connect() as connection:

            CONTENT_TYPE = 'application/vnd.kafka.json.v2+json'

            Emulator.ingest_data(
                'pinterest_data', random_row, connection,
                CONTENT_TYPE, PIN_INVOKE_URL)

            Emulator.ingest_data(
                'geolocation_data', random_row, connection,
                CONTENT_TYPE, GEO_INVOKE_URL)

            Emulator.ingest_data(
                'user_data', random_row, connection,
                CONTENT_TYPE, USER_INVOKE_URL)


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
