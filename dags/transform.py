import os
import csv
from datetime import datetime
import logging
from models import PprRawAll
from base import session
from sqlalchemy import text
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
# Settings
base_path = os.path.abspath(__file__ + "/../../")

raw_path = f"{base_path}/data/raw/downloaded_at=2021-02-01/ppr-all.csv"

@task
def transform_case(input_string):
    return input_string.lower()

@task
def update_date_of_sale(date_input):
    current_format = datetime.strptime(date_input, "%d/%m/%Y")
    new_format = current_format.strftime("%Y-%m-%d")
    return new_format

@task
def update_description(description_input):
    description_input = transform_case(description_input)
    if "new" in description_input:
        return "new"
    elif "second-hand" in description_input:
        return "second-hand"
    return description_input

@task
def update_price(price_input):
    price_input = price_input.replace("€", "")
    price_input = float(price_input.replace(",", ""))
    return int(price_input)

@task
def truncate_table():
    session.execute(
        text("TRUNCATE TABLE ppr_raw_all;ALTER SEQUENCE ppr_raw_all_id_seq RESTART;")
    )
    session.commit()

@task
def transform_new_data():
    with open(raw_path, mode="r", encoding="windows-1252") as csv_file:
        reader = csv.DictReader(csv_file)
        ppr_raw_objects = []
        for row in reader:
            ppr_raw_objects.append(
                PprRawAll(
                    date_of_sale=update_date_of_sale(row["date_of_sale"]),
                    address=transform_case(row["address"]),
                    postal_code=transform_case(row["postal_code"]),
                    county=transform_case(row["county"]),
                    price=update_price(row["price"]),
                    description=update_description(row["description"]),
                )
            )
        session.bulk_save_objects(ppr_raw_objects)
        session.commit()

