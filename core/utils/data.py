"""
Utility functions to manipulate data.
"""
import csv
import os
from pathlib import Path


<<<<<<< HEAD
CWD = Path(os.getcwd())
DATA_DIR = CWD / 'core/data'


=======
def query_to_csv(query,
                 save_path) -> None:
    """Saves to a csv file the result of a query."""
    with open(save_path, "x") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(row for row in query)
>>>>>>> 33da5cb (wip)
