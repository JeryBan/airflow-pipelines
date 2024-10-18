"""
Utility functions to manipulate data.
"""
import csv
import os
from pathlib import Path


CWD = Path(os.getcwd())
DATA_DIR = CWD / 'dags/data'


def query_to_csv(query,
                 save_path) -> None:
    """Saves to a csv file the result of a query."""
    with open(save_path, "x") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(row for row in query)


def export_xls_from_base64(data, filename=None):
    """
    Creates xls from data encoded in base64.
    """
    import base64

    save_dir = DATA_DIR / 'tmp'
    path = save_dir / filename
    Path.mkdir(save_dir, parents=True, exist_ok=True)

    decoded_data = base64.b64decode(data)
    with open(path, 'wb') as f:
        f.write(decoded_data)
        f.close()

    return str(path)
