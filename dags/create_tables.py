from base import Base, engine
from models import PprRawAll
from sqlalchemy_utils import database_exists, create_database
from airflow.decorators import task
@task
def create_if_not_exist_db():
    if not database_exists(engine.url):
        create_database(engine.url)
@task
def create_dbtable():
    Base.metadata.create_all(engine)
Base.metadata.create_all(engine)