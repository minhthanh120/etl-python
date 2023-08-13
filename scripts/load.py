from common.base import session
from common.tables import PprRawAll, PprCleanAll

from sqlalchemy import cast, Integer, Date
from sqlalchemy.dialects.postgresql import insert
def insert_transactions():
    """
    Insert operation: add new data
    """
    # Retrieve all the transaction ids from the clean table
    clean_transaction_ids = session.query(PprCleanAll.transaction_id)

    # date_of_sale and price needs to be casted as their
    # datatype is not string but, respectively, Date and Integer
    transactions_to_insert = session.query(
        cast(PprRawAll.date_of_sale, Date),
        PprRawAll.address,
        PprRawAll.postal_code,
        PprRawAll.county,
        cast(PprRawAll.price, Integer),
        PprRawAll.description,
    ).filter(~PprRawAll.transaction_id.in_(clean_transaction_ids))
	
    # Print total number of transactions to insert
    print("Transactions to insert:", transactions_to_insert.count())
    
    # Insert the rows from the previously selected transactions
    stm = insert(PprCleanAll).from_select(
        ["date_of_sale", "address", "postal_code", "county", "price", "description"],
        transactions_to_insert,
    )

    # Execute and commit the statement to make changes in the database.
    session.execute(stm)
    session.commit()

def main():
    print("[Load] Start")
    print("[Load] Inserting new rows")
    insert_transactions()
    print("[Load] Deleting rows not available in the new transformed data")
    delete_transactions()
    print("[Load] End")