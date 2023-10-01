from base import Base, engine
from models import PprRawAll

if __name__ == "__main__":
    Base.metadata.create_all(engine)