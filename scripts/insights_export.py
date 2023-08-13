from datetime import datetime
import os

from common.base import session
from common.tables import PprCleanAll
import xlsxwriter


# Settings
base_path = os.path.abspath(__file__ + "/../../")
ref_month = datetime.today().strftime("%Y-%m")

if __name__ == "__main__":
    data = session.execute("SELECT * FROM insights").all()
    ref_month = datetime.today().strftime("%Y-%m")
    
    # Create the workbook
    workbook = xlsxwriter.Workbook(
        f"{base_path}/insights_export/InsightsExport_202102.xlsx"
    )
    
    # Add a new worksheet
    worksheet = workbook.add_worksheet()
    worksheet.set_column("B:G", 12)
    
    # Add the table with all results in the newly created worksheet
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