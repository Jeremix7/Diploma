import logging
import sys,os
from typing import Union
try:
    from airflow.models import Variable
except:
    pass
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Base settings."""
    DAG_NAME: str = 'credit_scoring'
    LOG_LEVEL: int = logging.INFO

    ENV_TYPE: str
    
    # postgres
    
    POSTGRES_APP_NAME: str = DAG_NAME

    class Config:
        env_file_encoding = 'utf-8'

  
class LocalSettings(Settings):
    '''
    Локальные настройки
    '''
    ENV_TYPE: str = 'local'
    
    # postgres
    POSTGRES_USER: str = 'user'
    POSTGRES_PASSWORD: str = 'user'
    POSTGRES_DB: str = 'user'
    POSTGRES_SCHEMA: str = 'public'
    POSTGRES_HOST: str = 'localhost'
    POSTGRES_PORT: int = 5431

   
def get_settings() -> Union[LocalSettings]:

    try:
        env_type = Variable.get("stand_name")
    except:
        print('Variable "stand_name" is empty. Check system env.')
        env_type = "local"

    # локальные заглушки
    env_type = 'local'
    # env_type = 'dev'
    print('====================================')
    print(f'* Set stand to {env_type}')
    print('====================================')

    config_cls_dict = {
        'local': LocalSettings
    }
    config_cls = config_cls_dict[env_type]
    return config_cls()

settings = get_settings()
