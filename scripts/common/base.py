from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base

CONNECTION_STRING = "Driver={ODBC Driver 17 for SQL Server};Server=SERAPHINE\\SQLEXPRESS;database=db_ETL;Trusted_Connection=Yes;MultipleActiveResultSets=true"

connection_url = URL.create(
    "mssql+pyodbc", query={"odbc_connect": CONNECTION_STRING})
engine = create_engine(connection_url, echo=True)
Base = declarative_base()
