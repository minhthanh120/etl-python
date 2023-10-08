import extract, transform, load
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from create_tables import create_dbtable
import logging

with DAG(dag_id="transform_new_data", start_date=datetime(2023, 1, 9), catchup=False, tags=["model"]) as transform_src:
    with TaskGroup("extract", tooltip="Extract from source data") as ex_from_src:
        logging.info("[Extract] Start")
        logging.info("[Extract] Downloading snapshot")
        download_snapshot = extract.download_snapshot()
        logging.info(f"[Extract] Saving data from '{extract.source_path}' to '{extract.raw_path}'")
        save_new_raw_data = extract.save_new_raw_data()
        logging.info(f"[Extract] End")
        download_snapshot >> save_new_raw_data
    
    with TaskGroup("transform", tooltip="Transform and stage data") as tf_and_stage:
        logging.info("[Transform] Start")
        logging.info("[Transform] Remove any old data from ppr_raw_all table")
        truncate_table = transform.truncate_table()
        logging.info("[Transform] Transform new data available in ppr_raw_all table")
        transform_new_data = transform.transform_new_data()
        logging.info("[Transform] End")
        truncate_table >> transform_new_data

    with TaskGroup("load", tooltip="Load into db") as load_into_db:
        logging.info("[Load] Start")
        logging.info("[Load] Inserting new rows")
        insert_transactions = load.insert_transactions()
        logging.info("[Load] Deleting rows not available in the new transformed data")
        delete_transactions = load.delete_transactions()
        logging.info("[Load] End")
        insert_transactions >> delete_transactions

        ex_from_src >> tf_and_stage >> load_into_db

with DAG(dag_id="create_table", catchup=False, tags=["model"]) as create_table:
    with TaskGroup("create", tooltip="Create Data lake") as create_table:
        create_dbtable = create_dbtable
        logging.info("Created tables")
        create_dbtable