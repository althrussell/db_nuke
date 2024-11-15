import argparse
from databricks.sdk import AccountClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk.service import catalog
from databricks.sdk.service.iam import Group, ComplexValue,WorkspacePermission
from databricks.sdk.service.compute import AwsAttributes,AwsAvailability,DataSecurityMode,RuntimeEngine
import base64, requests
import datetime
import re

def get_token(client_id, client_secret, account_id):
    url = f'https://accounts.cloud.databricks.com/oidc/accounts/{account_id}/v1/token'
    data = {'grant_type': 'client_credentials', 'scope': 'all-apis'}
    creds = base64.b64encode(f'{client_id}:{client_secret}'.encode()).decode()
    headers = {'Authorization': f'Basic {creds}'}
    response = requests.post(url, headers=headers, data=data)
    access_token = None
    if response.status_code == 200:
        response_json = response.json()
        if 'access_token' in response_json:
            access_token = response_json['access_token']
            print("Access Token Retrieved")
        else:
            print("Access token not found in the response.")
    else:
        print("Failed to retrieve token. Status code:", response.status_code)
        print("Response:", response.text)
    
    return access_token

def build_account_client(token, account_id) -> AccountClient:
    ws_host = f"https://accounts.cloud.databricks.com"
    return AccountClient(host=ws_host, token=token, account_id=account_id)


def build_workspace_client(token, account_id, workspace_id) -> WorkspaceClient:
    ws_host = f"https://{workspace_id}.cloud.databricks.com"
    return WorkspaceClient(host=ws_host, token=token)


def update_workspace_permission(account_client: AccountClient, dry_run: bool):
    workspaces = account_client.workspaces.list()

    for workspace in workspaces:
        if dry_run:
            print(f"Would Update Workspace Permission: {workspace.workspace_id}")
        else:
            print(f"Workspace ID: {workspace.workspace_id}")
            print("Update")
            permmison_level = [WorkspacePermission.ADMIN]
            account_client.workspace_assignment.update(workspace_id=workspace.workspace_id,principal_id='7698820785010707',permissions=permmison_level)
            #response = account_client.workspace_assignment.get(workspace_id=workspace.workspace_id)
            #print(response)

def get_instance_profile(workspace_client: WorkspaceClient):
    
    for instanceprofile in workspace_client.instance_profiles.list():
        return instanceprofile

def update_clusters(workspace_client: WorkspaceClient,dry_run: bool, deployment_name: str):
    clusters = workspace_client.clusters.list()
    
    for cluster in clusters:
        if dry_run:
            print(f"Would Update Cluster: {cluster.cluster_id}")
        else:
            instance_p = get_instance_profile(workspace_client)
            
            aws_id = number = re.search(r'\d+', deployment_name).group()
            defaultCatalog = "catalog_" + aws_id
            print(f"Default Catalog: {defaultCatalog}")
            aws_att = AwsAttributes(availability=AwsAvailability.ON_DEMAND,ebs_volume_count=0,instance_profile_arn=instance_p.instance_profile_arn)

            spark_conf = {
                                    "spark.master": "local[*, 4]",
                                    "spark.databricks.sql.initial.catalog.name" : defaultCatalog
                                }
            workspace_client.clusters.edit(cluster_id=cluster.cluster_id,spark_version="15.4.x-cpu-ml-scala2.12",node_type_id="r6id.xlarge",driver_node_type_id="r6id.xlarge",num_workers=0,data_security_mode=DataSecurityMode.SINGLE_USER,runtime_engine=RuntimeEngine.STANDARD,aws_attributes=aws_att,cluster_name="Workshop Cluster",single_user_name="labuser+1@awsbricks.com",spark_conf=spark_conf,autotermination_minutes=120)
    
def start_clusters(workspace_client: WorkspaceClient,dry_run: bool):
    clusters = workspace_client.clusters.list()
    for cluster in clusters:
        if dry_run:
            print(f"Would Start Cluster: {cluster.cluster_id}")
        else:
            workspace_client.clusters.start(cluster_id=cluster.cluster_id)


def main(databricks_account_id, client_id, client_secret,dry_run: bool):
    if dry_run:
        print(f"DRY RUN")
    
    print(f"Databricks Account ID: {databricks_account_id}")

    access_token = get_token(client_id, client_secret, databricks_account_id)
    account_client = build_account_client(access_token, databricks_account_id)
    #update_workspace_permission(account_client,dry_run)
    workspaces = account_client.workspaces.list()
    for workspace in workspaces:
        print(f"Workspace:{workspace.deployment_name}")
        if workspace.deployment_name == 'apj-aws-lab-096215581514':
            print("Update Clusters")
            workspace_client = build_workspace_client(access_token,databricks_account_id,workspace.deployment_name)
            update_clusters(workspace_client,dry_run,workspace.deployment_name)

    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Databricks Account details")
    parser.add_argument("--databricks_account_id", required=True, help="Databricks Account ID")
    #parser.add_argument("--databricks_workspace_id", required=True, help="Databricks Workspace name")
    parser.add_argument("--client_id", required=True, help="Client ID")
    parser.add_argument("--client_secret", required=True, help="Client Secret")
    parser.add_argument("--dry_run", action='store_true', help="Run the script in dry run mode")
    args = parser.parse_args()
    
    main(args.databricks_account_id, args.client_id, args.client_secret, args.dry_run)
