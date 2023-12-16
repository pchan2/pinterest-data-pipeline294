import yaml


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