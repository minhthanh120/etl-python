from base import Base, engine
from models import PprRawAll

for table in Base.metadata.table:
    print(table)

if __name__ == "__main__":
    Base.metadata.create_all(engine)