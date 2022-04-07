import pandas as pd
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
import pyarrow as pa
import pyarrow.parquet as pq

storage_account_name = "<nome da sua conta de armazenamento>"
storage_account_key = "<chave de acesso da conta de armazenamento>"

def initialize_storage_account(storage_account_name: str, storage_account_key: str) -> DataLakeServiceClient:
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
        
        print("success connection!")
    
    except Exception as e:
        print(e)

initialize_storage_account(storage_account_name, storage_account_key)

def upload_file_to_directory():
    try:

        file_system_client = service_client.get_file_system_client(file_system="my-file-system")

        directory_client = file_system_client.get_directory_client("my-directory")
        
        file_client = directory_client.create_file("file-to-download.txt")
        
        local_file = open('C:\\file-to-download.txt','r', encoding='latin1')

        file_contents = local_file.read()

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))
        
        print("Finished!")

    except Exception as e:
        print(e)
    

def download_file_from_directory():
    try:
        file_system_client = service_client.get_file_system_client(file_system="my-file-system")

        directory_client = file_system_client.get_directory_client("my-directory")
        
        local_file = open("C:\\file-to-download.txt",'wb')

        file_client = directory_client.get_file_client("uploaded-file.txt")

        download = file_client.download_file()

        downloaded_bytes = download.readall()

        local_file.write(downloaded_bytes)

        local_file.close()

    except Exception as e:
     print(e)

# Faz a cópia do objeto para a memória como Arrow buffer
read_parquet_file_in_buffer = pa.BufferReader(download_file_from_directory())

# Lê os dados da memória como tabela parquet
read_table_from_parquet = pq.read_table(read_parquet_file_in_buffer)

# Converte a tabela parquet em dataframe pandas
df = read_table_from_parquet.to_pandas()

print(df)

