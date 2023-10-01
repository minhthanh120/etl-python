from datetime import datetime
import os

from base import session
from models import PprCleanAll
import xlsxwriter


base_path = os.path.abspath(__file__ + "/../../")
ref_month = datetime.today().strftime("%Y-%m")

if __name__ == "__main__":
    data = session.execute("SELECT * FROM insights").all()
    ref_month = datetime.today().strftime("%Y-%m")

    workbook = xlsxwriter.Workbook(
        f"{base_path}/insights_export/InsightsExport_202102.xlsx"
    )
    
    worksheet = workbook.add_worksheet()
    worksheet.set_column("B:G", 12)
    
    worksheet.add_table(
        "B3:E20",
        {
            "data": data,
            "columns": [
                {"header": "County"},
                {"header": "Number of Sales 3 month"},
                {"header": "Tot sales 3 months"},
                {"header": "Max sales 3 months"},
                {"header": "Min sales 3 months"},
                {"header": "Avg sales 3 months"},
            ],
        },
    )
    workbook.close()
    print("Data exported:", f"{base_path}/insights_export/InsightsExport_202102.xlsx")