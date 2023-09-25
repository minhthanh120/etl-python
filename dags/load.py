from plugins.base import session
from plugins.tables import PprRawAll, PprCleanAll
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
import logging
from sqlalchemy import cast, Integer, Date
from sqlalchemy.dialects.postgresql import insert

@task
def insert_transactions():
    clean_transaction_ids = session.query(PprCleanAll.transaction_id)
    transactions_to_insert = session.query(
        cast(PprRawAll.date_of_sale, Date),
        PprRawAll.address,
        PprRawAll.postal_code,
        PprRawAll.county,
        cast(PprRawAll.price, Integer),
        PprRawAll.description,
    ).filter(~PprRawAll.transaction_id.in_(clean_transaction_ids))
    logging.info("Transactions to insert:", transactions_to_insert.count())
    stm = insert(PprCleanAll).from_select(
        ["date_of_sale", "address", "postal_code", "county", "price", "description"],
        transactions_to_insert,
    )
    session.execute(stm)
    session.commit()

@task
def delete_transactions():
    raw_transaction_ids = session.query(PprRawAll.transaction_id)
    transactions_to_delete = session.query(PprCleanAll).filter(
        ~PprCleanAll.transaction_id.in_(raw_transaction_ids)
    )
    logging.info("Transactions to delete:", transactions_to_delete.count())
    transactions_to_delete.delete(synchronize_session=False)
    session.commit()
   