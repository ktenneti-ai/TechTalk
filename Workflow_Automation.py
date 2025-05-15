# dbx notebook source
# MAGIC %md
# MAGIC
# MAGIC **This notebook automates the process of backing up, creating, or resetting dbx jobs and pipelines.**
# MAGIC
# MAGIC Follow the steps below each time you use this notebook:
# MAGIC
# MAGIC Set Required Widgets: 
# MAGIC - `job_name`: (string) Name of the dbx job to target.
# MAGIC - `pipeline_name`: (string, optional) Name filter for DLT pipelines.
# MAGIC - `mode`: "single" — for one job; "all" — to process all jobs.
# MAGIC - `operation`: Choose one of:
# MAGIC     - "backup": Export current job config to ADLS.
# MAGIC     - "create": Recreate job from stored config in ADLS.
# MAGIC     - "reset": Update existing job using the stored config.
# MAGIC     - "restore"`: Recover job config from archive
# MAGIC

# COMMAND ----------

# DBTITLE 1,imports
import os
import requests
import json
import datetime
from azure.identity import ClientSecretCredential

# COMMAND ----------

# DBTITLE 1,Configuration
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id", dbutils.secrets.get(scope="dbricks-keyvault-scope",key="dbx-service-principal-clientid"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbricks-keyvault-scope",key="dbx-service-principal-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", "https://login.microsoftonline.com/f9b2a137-cc77-4d92-897a-5ae6d8b470ec/oauth2/token")

# COMMAND ----------

# DBTITLE 1,Load widgets
dbutils.widgets.text("job_name", "")
job_name = dbutils.widgets.get("job_name")

dbutils.widgets.text("pipeline_name", "")
pipeline_name = dbutils.widgets.get("pipeline_name")

dbutils.widgets.text("restore_timestamp", "")
restore_timestamp = dbutils.widgets.get("restore_timestamp")

dbutils.widgets.dropdown("mode", "single", ["single", "all"])
mode = dbutils.widgets.get("mode").lower()

dbutils.widgets.dropdown("tag_filter", "Application:Kicks", [
    "Application:Kicks"])
tag_filter_raw = dbutils.widgets.get("tag_filter")
try:
    tag_key, tag_value = tag_filter_raw.split(":", 1)
    tag_filter_dict = {tag_key.strip(): tag_value.strip()}
except ValueError:
    tag_filter_dict = {}
    print(f"Invalid tag filter format: '{tag_filter_raw}'. Expected format: 'key:value'")

dbutils.widgets.dropdown("operation", "backup", ["backup", "create", "reset", "restore"])
operation = dbutils.widgets.get("operation").lower()

# COMMAND ----------

# DBTITLE 1,Load environment variables
dbx_host = os.getenv("Host")
cluster_id = os.getenv("UC_ID")
storage_path = os.getenv("Path")
catalog_name = os.getenv("Catalog")
warehouse_id = os.getenv("SQL_Pro_ID")
trigger_state = os.getenv("wf_Trigger_State", "PAUSED")

# COMMAND ----------

# DBTITLE 1,Token Generation
# Service Principal details
tenant_id = dbutils.secrets.get('dbricks-keyvault-scope', 'dbxTenant')
client_id = dbutils.secrets.get('dbricks-keyvault-scope', 'dbx-src-principal-clientid')
client_secret = dbutils.secrets.get('dbricks-keyvault-scope', 'dbx-src-principal-secret')

# dbx resource ID
dbx_resource_id = "a3d9c45b-52f2-4e0f-a0e6-9bf1a1c0f7d3/.default"
# Get Azure AD Token
credential = ClientSecretCredential(tenant_id, client_id, client_secret)
token = credential.get_token(dbx_resource_id)
azure_ad_token = token.token
#azure_ad_token

# COMMAND ----------

# DBTITLE 1,Get PAT Token
def create_pat_token(azure_ad_token, dbx_host):
    # API endpoint for token creation
    url = f"{dbx_host}/api/2.0/token/create"
    # Headers
    headers = {
        "Authorization": f"Bearer {azure_ad_token}",
        "Content-Type": "application/json"
    }
    # Request payload
    payload = {
        "comment": "Token for service principal",
        "lifetime_seconds": 7200  # Token expiry time in seconds
    }
    # Make the request
    response = requests.post(url, headers=headers, json=payload)
    # Check the response
    if response.status_code == 200:
        return response.json().get("token_value")
    else:
        raise Exception(f"Failed to generate PAT Token: {response.status_code}, {response.text}")

# Example usage
access_token = create_pat_token(azure_ad_token, dbx_host)
# print("Generated PAT Token:", access_token)

# COMMAND ----------

# DBTITLE 1,list jobs
endpoint_list_jobs = "/api/2.0/jobs/list"

def list_dbx_jobs(access_token, job_name=None):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    url = f"{dbx_host}{endpoint_list_jobs}"

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        jobs_list = response.json().get("jobs", [])
        
        if job_name:
            jobs_list = [job for job in jobs_list if job_name.lower() in job['settings']['name'].lower()]
        
        job_data = [(job["job_id"], job["settings"]["name"]) for job in jobs_list]
        return spark.createDataFrame(job_data, ["job_id", "job_name"])
    else:
        raise Exception(f"Error fetching jobs: {response.status_code}, {response.text}")

# Example usage
# jobs_df = list_dbx_jobs(access_token, job_name)
# display(jobs_df)

# COMMAND ----------

# DBTITLE 1,Export Job Config
def export_job_config(access_token, job_id):
    url = f"{dbx_host}/api/2.1/jobs/get?job_id={job_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get job config: {response.status_code}, {response.text}")

# job_config_json = export_job_config(access_token, job_id)
# display(job_config_json)

# COMMAND ----------

# DBTITLE 1,Save to Storage
def export_and_store_job_config(access_token, job_id, job_name, dir_path,archive_path):
    
    job_config = export_job_config(access_token, job_id)
    file_path = f"{dir_path}{job_name}.json"
    try:
        # Archive if existing config is found
        if dbutils.fs.ls(file_path):
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_file = f"{archive_path}{job_name}_{timestamp}.json"
            dbutils.fs.mkdirs(archive_path)
            json_string = dbutils.fs.head(file_path)
            dbutils.fs.put(archive_file, json_string, overwrite=True)
            print(f"Archived previous config to: {archive_file}")
    except:
        pass
    dbutils.fs.put(file_path, json.dumps(job_config, indent=2), overwrite=True)
    print(f"Job config saved to: {file_path}")
# Example usage
#export_and_store_job_config(access_token, job_id, job_name, dir_path)

# COMMAND ----------

# DBTITLE 1,Get JobConfig
def load_json_string_from_storage(job_name):
    file_path = f"{dir_path}{job_name}.json"
    try:
        json_string = dbutils.fs.head(file_path)
        return json.loads(json_string)
    except Exception as e:
        print(f"Failed to read JSON from: {file_path}")
        raise e    

# COMMAND ----------

# DBTITLE 1,Convert Json to string
def convert_boolean_strings(obj):
    if isinstance(obj, dict):
        return {k: convert_boolean_strings(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_boolean_strings(i) for i in obj]
    elif isinstance(obj, str):  # Convert "true"/"false" strings to actual booleans
        if obj.lower() == "true":
            return True
        elif obj.lower() == "false":
            return False
    return obj

# job_config_json = load_json_string_from_storage(job_name)
# job_config = convert_boolean_strings(job_config_json)
# display(job_config)

# COMMAND ----------

# DBTITLE 1,get_ DLT pipeline
def get_dlt_pipelines(access_token, pipeline_name=None):
    url = f"{dbx_host}/api/2.0/pipelines"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.text}")
        return None
    di = response.json()
    pipelines = di.get('statuses') or di.get('pipelines')  
    if not pipelines:
        print("No pipelines found.")
        return None
    pipeline_info = [{"name": p.get("name"), "pipeline_id": p.get("pipeline_id")} for p in pipelines if not pipeline_name or pipeline_name.lower() in p.get("name", "").lower()]
    return pipeline_info

pipelines = get_dlt_pipelines(access_token, pipeline_name)
pipeline_map = {pipeline["name"]: pipeline["pipeline_id"] for pipeline in pipelines} if pipelines else {}

# Display the results
#display(pipeline_map)

# COMMAND ----------

# DBTITLE 1,Permission Config
PERMISSIONS_CONFIG = {
    "access_control_list": [
        {
            "group_name": "DBX-eastus2-kicks-dev-developer",
            "permission_level": "CAN_MANAGE"
        },
        {
            "group_name": "DBX-eastus2-kicks-dev-admin",
            "permission_level": "CAN_MANAGE"
        },
        {
            "group_name": "DBX-eastus2-kicks-dev-reader",
            "permission_level": "CAN_VIEW"
        },
        {
            "service_principal_name": "6c1e7b9a-8f24-4f98-a1d2-5b9de0c774aa",
            "permission_level": "IS_OWNER"
        }
    ]
}

# COMMAND ----------

# DBTITLE 1,Set Job Permission
def set_job_permissions(access_token, job_id):
    """ Sets permissions for the created job """
    url = f"{dbx_host}/api/2.0/permissions/jobs/{job_id}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    response = requests.put(url, headers=headers, json=PERMISSIONS_CONFIG)

    if response.status_code == 200:
        print(f"Permissions applied successfully to Job {job_id}.")
    else:
        print(f"Failed to set permissions: {response.status_code} - {response.text}")

#Apply Permissions to all jobs
# for job_id in jobs_df.select("job_id").collect():
#     set_job_permissions(access_token, job_id["job_id"])

# COMMAND ----------

# DBTITLE 1,Create workflow
def create_workflow(access_token, job_config,cluster_id,storage_path,pipeline_map=None):
    print("Creating job with the following settings:")
    #print(json.dumps(job_config, indent=4))

    url = f"{dbx_host}/api/2.1/jobs/create"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    for task in job_config.get("tasks", []):
        task_key = task.get("task_key")
        # Assign cluster
        if "existing_cluster_id" in task:
            task["existing_cluster_id"] = cluster_id 
        # Assign pipeline_id based on task_key
        if "pipeline_task" in task:
            if pipeline_map and task_key in pipeline_map:
                task["pipeline_task"]["pipeline_id"] = pipeline_map[task_key]
        # Notebook path and parameters
        if "notebook_task" in task:
            if "base_parameters" in task["notebook_task"]:
                if "Path" in task["notebook_task"]["base_parameters"]:
                    task["notebook_task"]["base_parameters"]["Path"] = storage_path
                    task["notebook_task"]["base_parameters"]["storage_account_name"] = storage_path   

        if "notebook_task" in task and "notebook_path" in task["notebook_task"]:
            notebook_path = task["notebook_task"]["notebook_path"]
            if not notebook_path.startswith("/Workspace/kicks/"):
                task["notebook_task"]["notebook_path"] = notebook_path.replace(
                    "/Workspace/", "/Workspace/kicks/", 1
                )
        # Remove email notifications
        if "email_notifications" in task:
            del task["email_notifications"]
    # Remove run_as user
    if "run_as" in job_config:
        del job_config["run_as"]

    job_config["tags"] = {
        "Application": "kicks"
    }
    
    if "schedule" in job_config:
        job_config["schedule"]["pause_status"] = trigger_state

    response = requests.post(url, headers=headers, json=job_config)
    
    if response.status_code == 200:
        job_id = response.json().get("job_id")
        print(f"Job {job_id} updated successfully!")
        # Apply Permissions to the newly created job
        set_job_permissions(access_token, job_id)
    else:
        print(f"Failed to Create job: {response.text}")

# COMMAND ----------

# DBTITLE 1,reset workflow
def reset_job(job_id, access_token, updated_config,pipeline_map=None):
    print("Resetting job with the following settings:")
    print(json.dumps(updated_config, indent=4))

    url = f"{dbx_host}/api/2.1/jobs/reset"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    for task in updated_config["new_settings"]["tasks"]:
        if "existing_cluster_id" in task:
            task["existing_cluster_id"] = cluster_id 

        # Assign pipeline ID if matched in map
        task_key = task.get("task_key")
        if "pipeline_task" in task and pipeline_map:
                if task_key in pipeline_map:
                    task["pipeline_task"]["pipeline_id"] = pipeline_map[task_key]
    
    response = requests.post(url, headers=headers, json=updated_config)
    
    if response.status_code == 200:
        print(f"Job {job_id} updated successfully!")
    else:
        print(f"Failed to update job: {response.text}")

# COMMAND ----------

# DBTITLE 1,Restore Job Config from Archive
def restore_job_config_from_archive(archive_path, job_name, restore_timestamp=None):
    """
    Restores the most recent or a specific archived config for the given job.

    Parameters:
        archive_path (str): The archive directory path for the job.
        job_name (str): Job name whose config should be restored.
        restore_timestamp (str, optional): Format YYYYmmdd_HHMMSS. Restores the specific version if provided.
    """
    try:
        files = dbutils.fs.ls(archive_path)
        archived_files = [f.path for f in files if f.name.startswith(job_name) and f.name.endswith(".json")]

        if not archived_files:
            print(f"No archived configs found in: {archive_path}")
            return

        # Determine target file
        if restore_timestamp:
            match = f"{job_name}_{restore_timestamp}.json"
            target_file = next((f for f in archived_files if match in f), None)
        else:
            target_file = sorted(archived_files)[-1]  

        if not target_file:
            print(f"No archive found for timestamp: {restore_timestamp}")
            return

        # Read archive content
        restored_content = dbutils.fs.head(target_file)

        # Derive destination path from archive path structure
        ingest_file = f"abfss://ingest@{storage_path}.dfs.core.windows.net/Workflow/json/{job_name}.json"

        dbutils.fs.put(ingest_file , restored_content, overwrite=True)
        print(f"Restored config from:\n{target_file}\nto:\n{ingest_file}")

    except Exception as e:
        print(f"Failed to restore archived config: {e}")


# COMMAND ----------

def filter_jobs_by_tag(access_token, tag_filter):
    """
    Returns a list of (job_id, job_name) tuples for jobs that match all tag criteria.

    Parameters:
        access_token (str): Valid dbx PAT.
        tag_filter (dict): Tag key-value pairs to match, e.g. {"Application": "Internal Analytics"}

    Returns:
        List[Tuple[job_id, job_name]]
    """
    filtered = []
    all_jobs_df = list_dbx_jobs(access_token)
    for row in all_jobs_df.collect():
        job_id = row["job_id"]
        job_name = row["job_name"]
        try:
            config = export_job_config(access_token, job_id)
            tags = config.get("settings", {}).get("tags", {})
            if all(tags.get(k) == v for k, v in tag_filter.items()):
                filtered.append((job_id, job_name))
        except Exception as e:
            print(f"Skipping job {job_id} ({job_name}) due to error: {e}")
    return filtered


# COMMAND ----------

# DBTITLE 1,Process Job
def process_job(job_id, job_name, dir_path,archive_path, operation):
    if operation in {"create", "reset"}:
        export_and_store_job_config(access_token, job_id, job_name, dir_path,archive_path)    
    
    if operation == "backup":
        export_and_store_job_config(access_token, job_id, job_name, dir_path,archive_path)
    elif operation == "create":
        job_config_json = load_json_string_from_storage(job_name)
        job_config = convert_boolean_strings(job_config_json["settings"])
        job_config["name"] = job_name
        if not job_config.get("tasks"):
            raise ValueError(f"No tasks found in job config for job: {job_name}")
        create_workflow(access_token, job_config, cluster_id, pipeline_map)
    elif operation == "reset":
        job_config_json = load_json_string_from_storage(job_name)
        reset_payload = {
            "job_id": job_config_json["job_id"],
            "new_settings": job_config_json["settings"]
        }
        reset_job(reset_payload["job_id"], access_token, reset_payload, pipeline_map)
    elif operation == "restore":
        restore_job_config_from_archive(archive_path, job_name, restore_timestamp if restore_timestamp else None)    

if mode == "single":
    print(f"Running in single mode for job: {job_name}")
    if operation != "create":
        jobs_df = list_dbx_jobs(access_token, job_name)
        if jobs_df.count() == 0:
            raise ValueError(f"No job found with name: {job_name}")
        job_row = jobs_df.first()
        job_id = job_row["job_id"]
    curr_dt_yyyyMMdd = datetime.datetime.today().strftime('%Y%m%d')
    dir_path = f"abfss://ingest@{storage_path}.dfs.core.windows.net/Workflow/json/{job_name}/"
    archive_path = f"abfss://archive@{storage_path}.dfs.core.windows.net/Workflow/json/{curr_dt_yyyyMMdd}/{job_name}/"

    paths = [dir_path, archive_path]
    for path in paths:
        try:
            dbutils.fs.ls(path)
            print('Directory already exists:', path)
        except:
            dbutils.fs.mkdirs(path)
            print('Directory is created:', path)

    process_job(job_id if operation != "create" else None, job_name, dir_path,archive_path, operation)

elif mode == "all":
    print("Running in ALL mode for all jobs")
    all_jobs_df = list_dbx_jobs(access_token)

    filtered_jobs = filter_jobs_by_tag(access_token, tag_filter_dict)

    if not filtered_jobs:
        print("No jobs found with tag 'Internal Analytics'.")
    else:
        for job_id, job_name in filtered_jobs:
            curr_dt_yyyyMMdd = datetime.datetime.today().strftime('%Y%m%d')
            dir_path = f"abfss://ingest@{storage_path}.dfs.core.windows.net/Workflow/json/{job_name}/"
            archive_path = f"abfss://archive@{storage_path}.dfs.core.windows.net/Workflow/json/{curr_dt_yyyyMMdd}/{job_name}/"

            paths = [dir_path, archive_path]
            for path in paths:
                if not path or " " in path or "%" in path:
                    print(f"Skipping invalid path: '{path}'")
                continue
                try:
                    dbutils.fs.ls(path)
                    print('Directory already exists:', path)
                except:
                    dbutils.fs.mkdirs(path)
                    print('Directory is created:', path)

            process_job(job_id, job_name, dir_path,archive_path, operation)
