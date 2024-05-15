"""
Utility functions to manipulate data.
"""
import csv
from pathlib import Path
from airflow.providers.postgres.hooks.postgres import CursorType


def query_to_csv(cursor: CursorType,
                 save_path: Path) -> None:
    """Saves to a csv file the result of a query."""
    with open(save_path, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
