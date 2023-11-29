import yaml


class CredentialsReader:
    '''
    This class is used to represent a credentials reader.
    '''
    @staticmethod
    def read_db_creds(credentials_filename: str) -> dict:
        '''
        This static method reads the yaml file.

        Args:
            credentials_filename (str): the filename of the database
            credentials.

        Returns:
            db_creds (dict): the database credentials.
        '''
        with open(f'{credentials_filename}.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds