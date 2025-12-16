# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "77865f89-8d73-465e-bdd5-92e7de9e75f7",
# META       "default_lakehouse_name": "LH_Monitoring",
# META       "default_lakehouse_workspace_id": "6131c083-52ac-4c68-8b31-bcf2f02e378c"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

pipeline_rund_id = "1caf67a5-2bba-4085-920c-6b3f07c90523"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## import all needed libraries

# CELL ********************

from pyspark.sql.functions import col, max
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    fabric_logs_main_df = spark.read.format("delta").load(f'Tables/fabric_logs')
    start_time = fabric_logs_main_df.agg(max("jobStartTimeUtc")).collect()[0][0]
except:
    print('No Table Found - setting yesterday as time to get the logs.')
    start_time = datetime.today() - timedelta(hours=24, minutes=0)

end_time = datetime.today()

print(start_time)
print(end_time)






# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.today() - timedelta(hours=24, minutes=0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

url = "<YOUR_URL>"
params = {
    "limit": 10000,
    "endTime": f"{end_time}",
    "startTime": f"{start_time}",
    "status":"4,6,2,3,5,7,8", # All processed statuses.
    #"folderIds": "b3ff67d3-2cd3-4e8f-bd51-ea0f95bd3451,bcf4296c-158f-4dcb-a41b-8684a19bd01c,48e33894-4845-4d4f-b8aa-441fbe83d434,2fd13535-9cdc-4492-b983-556d3b4b1b46,30059f25-2da3-44a4-97db-e77dfcb88ee0,caaea7b7-4fb4-4e9e-a0c1-2d8a721688ce,c447fbed-35d4-4a21-bfb9-c6820e9b0302,c02d537b-3916-42f7-b1b0-40ab2c05fe76,6c1bf4da-4417-416e-94da-18820eba3416,d21e4e77-3e31-41e2-90d0-4032e9d2e0e6",
    "Accept": "application/json",
    "accept-encoding": "gzip, deflate, br, zstd",
    "activityid": "d6678b61-1c72-4e43-9498-54b715064543",
    "accept-language": "en-US,en;q=0.9"
}
headers = {
    "Authorization": f"Bearer {token}"
}

response = requests.get(url, headers=headers, params=params)


data = response.json()
df = pd.json_normalize(data)
# spark_df = spark.createDataFrame(df)

display(df)
df['UniqueIdentifier'] = [uuid.uuid4() for _ in range(len(df))]
df['pipeline_run_id'] = pipeline_rund_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df.empty:
    df_processing = True
else:
    df_processing = False

if df_processing:
    for col_name in df.columns:
        df[col_name] = df[col_name].astype('str')
    df = spark.createDataFrame(df)
    df.write.format("delta").mode("append").option('mergeSchema', 'true').save('Tables/fabric_logs')




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_failures = df.filter(df.isSuccessful == False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not df_failures.rdd.isEmpty():
    df_failures_processing = True
else:
    df_failures_processing = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if df_failures_processing:
    from pyspark.sql import functions as F

    result_df = df_failures.select(
        df_failures["workspaceName"],
        df_failures["workspaceObjectId"].alias("WorkspaceId"),
        df_failures["artifactName"].alias("FailedItemName"),
        df_failures["artifactType"].alias("FailedItemType"),
        df_failures["serviceExceptionJson"].alias("ErrorMessage"),
        df_failures["`ownerUser.name`"].alias("OwnerUserName"),
        df_failures["`ownerUser.userPrincipalName`"].alias("OwnerUserEmail"),
        df_failures["jobScheduleTimeUtc"],
        df_failures["jobStartTimeUtc"],
        df_failures["jobEndTimeUtc"]
    )
    display(result_df)
    result_json = result_df.toJSON().collect()
    result_array = [json.loads(item) for item in result_json]
else:
    result_array=[]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(json.dumps(result_array))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
