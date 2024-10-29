from pathlib import Path

from airflow.configuration import conf

# Get the DAGs folder path from Airflow config
dags_folder = Path(conf.get("core", "dags_folder"))


class DIRECTORIES:
    """
    Shared directories for all dags to use.
    """
    DATA = dags_folder / 'data'
    DUMPS = dags_folder / 'dumps'
    TEMP = dags_folder / 'tmp'

    @classmethod
    def create_directories(cls):
        """
        Creates directories if they don't exist at class import.
        """
        for d in [cls.DATA, cls.DUMPS, cls.TEMP]:
            Path(d).mkdir(parents=True, exist_ok=True)


DIRECTORIES.create_directories()
