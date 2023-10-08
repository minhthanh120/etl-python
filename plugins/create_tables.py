from base import Base, engine
from models import PprRawAll

def create_dbtable():
    Base.metadata.create_all(engine)
Base.metadata.create_all(engine)