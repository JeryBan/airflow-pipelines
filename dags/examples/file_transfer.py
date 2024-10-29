from datetime import timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import paramiko  #Connect to SSH servers
import os

# Define file server 

FILE_SERVER_HOST = 'file-server-ip'
FILE_SERVER_PORT = 21
FILE_SERVER_USER = 'username'
FILE_SERVER_PASSWORD = 'password'
FILE_SERVER_DIR = '/path/to/files'  #Foreign directory on the file server

#Synology NAS settings

NAS_HOST = 'synology-nas-ip'
NAS_USER = 'nas-username'
NAS_PASSWORD = 'nas-password'
NAS_DIR = '/path/to/nas'  #Local directory on the Synology NAS


@dag(
    tags=['example'],
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=1),  #Available options: 0, 1, 2, 3, 4, 5, etc
    },
    description='A simple DAG to transfer files from a file server to Synology NAS',
    schedule_interval='@hourly',
    # Adjust this to your desired frequency -> Available options: @yearly, @monthly, @weekly, @daily, @hourly, etc.
    start_date=days_ago(1),
    catchup=False,
)
def file_transfer_dag():
    @task
    def detect_and_transfer_files():
        # Connect to the file server
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(FILE_SERVER_HOST, port=FILE_SERVER_PORT, username=FILE_SERVER_USER, password=FILE_SERVER_PASSWORD)

        # Get list of files from file server
        stdin, stdout, stderr = ssh.exec_command(f'ls {FILE_SERVER_DIR}')
        files = stdout.read().decode().splitlines()

        # Connect to Synology NAS
        transport = paramiko.Transport((NAS_HOST, 22))
        transport.connect(username=NAS_USER, password=NAS_PASSWORD)
        nas = paramiko.SFTPClient.from_transport(transport)

        # Transfer files
        for file_name in files:
            file_path = os.path.join(FILE_SERVER_DIR, file_name)
            local_file_path = f'/tmp/{file_name}'
            nas_file_path = os.path.join(NAS_DIR, file_name)

            # Download file from file server
            sftp = ssh.open_sftp()
            sftp.get(file_path, local_file_path)
            sftp.close()

            # Upload file to Synology NAS
            nas.put(local_file_path, nas_file_path)

            # Clean up local file
            os.remove(local_file_path)

        # Close connections
        ssh.close()
        nas.close()
        transport.close()

    # Define task dependencies
    detect_and_transfer_files()


# Instantiate the DAG
file_transfer_dag = file_transfer_dag()
