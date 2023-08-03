import logging
import os
import pandas as pd
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.storage.blob import BlobServiceClient
import azure.functions as func

app = func.FunctionApp()

def main()->None:
    AAD_TENANT_ID = os.environ["AAD_TENANT_ID"]
    KUSTO_CLUSTER = os.environ["KUSTO_CLUSTER"]
    KUSTO_DATABASE = os.environ["KUSTO_DATABASE"]
    BLOB_CONNECTION_STRING = os.environ["BLOB_CONNECTION_STRING"]

    KCSB = KustoConnectionStringBuilder.with_aad_device_authentication(
        KUSTO_CLUSTER)
    KCSB.authority_id = AAD_TENANT_ID

    KUSTO_CLIENT = KustoClient(KCSB)
    KUSTO_QUERY = "IncidentTable"
    RESPONSE = KUSTO_CLIENT.execute(KUSTO_DATABASE, KUSTO_QUERY)

    df = dataframe_from_result_table(RESPONSE.primary_results[0])

    for c, row in df.iterrows():
        # Create a unique filename based on the index or any other unique identifier in the row
        c += 1
        filename = f"chunks/issue_{c}.txt"
        print(filename)
        # Open the file in write mode
        with open(filename, "w") as file:
            # Write the values from 'column1' and 'column2' to the file
            file.write(f"IncidentId: {row['IncidentId']}\n")
            file.write(f"OwningTeamName: {row['OwningTeamName']}\n")
            file.write(f"Issue: {row['Issue']}\n")
            file.write(f"Resolution: {row['Resolution']}\n")
            file.write(f"UserClarification: {row['UserClarification']}")

    # Upload the txt files to blob storage.
    container_name = "vlc-data-schedule-store"
    blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container_client = blob_client.get_container_client(container_name)
    for file_name in os.listdir("chunks"):
        file_path = os.path.join("chunks", file_name)
        blob_client = container_client.get_blob_client(file_name)
        with open(file_path, "rb") as file:
            blob_client.upload_blob(file)
    logging.info("The process is complete.")

@app.function_name(name="mytimer")
@app.schedule(schedule="0 */5 * * * *", arg_name="myTimer", run_on_startup=True,use_monitor=True) 
def vlc_data_schedule_job(myTimer: func.TimerRequest) -> None:
    print("Timer is working")
    if myTimer.past_due:
        logging.info('The timer is past due!')

main()

logging.info('Python timer trigger function executed.')

