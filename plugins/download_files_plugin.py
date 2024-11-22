import os

from flask import Blueprint, Response
from werkzeug.utils import safe_join
from airflow.plugins_manager import AirflowPlugin

from core.share import DIRECTORIES

DATA_DIR = DIRECTORIES.DATA

download_bp = Blueprint("download_bp", __name__, url_prefix="/download")


@download_bp.route("/<filename>", methods=["GET"])
def download_file(filename):
    """
    Serve a file download using Flask
    """
    file_path = safe_join(DATA_DIR, filename)
    if os.path.exists(file_path):
        with open(file_path, "rb") as f:
            file_data = f.read()

        response = Response(file_data, content_type='application/octet-stream')
        response.headers['Content-Disposition'] = 'attachment; filename="{}"'.format(filename)
        return response
    else:
        return Response("File not found", status=404)


class DownloadFilesPlugin(AirflowPlugin):
    name = "download_files_plugin"
    flask_blueprints = [download_bp]
