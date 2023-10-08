from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.engine.url import URL

CONNECTION_STRING = "postgresql://airflow:airflow@localhost:5432/airflow"

connection_url = URL.create(CONNECTION_STRING)
engine = create_engine(CONNECTION_STRING , echo=True)
Base = declarative_base()
Session = sessionmaker(bind = engine)
session = Session()