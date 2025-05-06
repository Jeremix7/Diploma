"""
Модуль подключения к базе данных postgres
"""
from configs import settings
from sqlalchemy.orm import declarative_base


class DatabaseClient:
    """
    Набор основных компонентов SQLAlchemy для подключения к базе данных
    """
    def __init__(self, database_name: str = settings.POSTGRES_DB):

        from sqlalchemy import MetaData
        self.database_name = database_name
        self.engine = self._connect_db()
        metadata = MetaData(schema=settings.POSTGRES_SCHEMA)
        self.Base = declarative_base(metadata=metadata)

    def _connect_db(self):
        """
            Подключение к postgres.
        """
        from sqlalchemy import create_engine

        self.engine = create_engine(
            f'postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASSWORD}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{self.database_name}?application_name={settings.POSTGRES_APP_NAME}'
        )
        return self.engine


database_client = DatabaseClient()

