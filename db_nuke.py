import argparse
from databricks.sdk import AccountClient
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk.service import catalog
from databricks.sdk.service.iam import Group, ComplexValue
import base64, requests
import datetime

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

def nuke_users(account_client: AccountClient, prefix: str, dry_run: bool):
    users = account_client.users.list()
    for user in users:
        if '@awsbricks.com' in user.user_name:
            if dry_run:
                print(f"DRY RUN: Would delete user {user.user_name}")
            else:
                print(f"Deleting user {user.user_name}")
                account_client.users.delete(user_id=user.id)  # Uncomment to perform actual deletion

def nuke_metastore(account_client: AccountClient, prefix: str, dry_run: bool):
    metastore_list = account_client.metastores.list()  # Assuming this method lists all metastore items
    for metastore in metastore_list:
        
        if metastore.name.startswith(prefix):
            if dry_run:
                print(f"DRY RUN: Would delete metastore {metastore.name}")
            else:
                print(f"Deleting metastore {metastore.name}")
                account_client.metastores.delete(metastore_id=metastore.metastore_id,force=True)  # Uncomment to perform actual deletion



def nuke_workspaces(account_client: AccountClient, prefix: str, dry_run: bool):
    workspaces_list = account_client.workspaces.list()  # Assuming this method lists all workspaces
    two_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)

    for workspace in workspaces_list:
        # Convert epoch milliseconds to a datetime object
        creation_time = datetime.datetime.fromtimestamp(workspace.creation_time / 1000.0)

        if prefix:
            # If a prefix is provided, use it to filter workspaces
            if workspace.workspace_name.startswith(prefix):
                if dry_run:
                    print(f"DRY RUN: Would delete workspace {workspace.workspace_name} with prefix {prefix}")
                else:
                    print(f"Deleting workspace {workspace.workspace_name} with prefix {prefix}")
                    account_client.workspaces.delete(workspace_id=workspace.workspace_id)  # Uncomment to perform actual deletion
        else:
            # If no prefix is provided, use creation time to filter workspaces older than 2 weeks
            if creation_time < two_weeks_ago:
                if dry_run:
                    print(f"DRY RUN: Would delete workspace {workspace.workspace_name} created on {creation_time}")
                else:
                    print(f"Deleting workspace {workspace.workspace_name} created on {creation_time}")
                    account_client.workspaces.delete(workspace_id=workspace.workspace_id)  # Uncomment to perform actual deletion



def nuke_network_config(account_client: AccountClient, prefix: str, dry_run: bool):
    network_list = account_client.networks.list()  # Assuming this method lists all network configurations
    two_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)

    for network in network_list:
        # Convert epoch milliseconds to a datetime object
        creation_time = datetime.datetime.fromtimestamp(network.creation_time / 1000.0)

        if prefix:
            # If a prefix is provided, use it to filter network configurations
            if network.network_name.startswith(prefix):
                if dry_run:
                    print(f"DRY RUN: Would delete network configuration {network.network_name} with prefix {prefix}")
                else:
                    print(f"Deleting network configuration {network.network_name} with prefix {prefix}")
                    account_client.networks.delete(network_id=network.network_id)  # Uncomment to perform actual deletion
        else:
            # If no prefix is provided, use creation time to filter network configurations older than 2 weeks
            if creation_time < two_weeks_ago:
                if dry_run:
                    print(f"DRY RUN: Would delete network configuration {network.network_name} created on {creation_time}")
                else:
                    print(f"Deleting network configuration {network.network_name} created on {creation_time}")
                    account_client.networks.delete(network_id=network.network_id)  # Uncomment to perform actual deletion




def nuke_storage_config(account_client: AccountClient, prefix: str, dry_run: bool):
    storage_list = account_client.storage.list()  # Assuming this method lists all storage configurations
    two_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)

    for storage in storage_list:
        # Convert epoch milliseconds to a datetime object
        creation_time = datetime.datetime.fromtimestamp(storage.creation_time / 1000.0)

        if prefix:
            # If a prefix is provided, use it to filter storage configurations
            if storage.storage_configuration_name.startswith(prefix):
                if dry_run:
                    print(f"DRY RUN: Would delete storage configuration {storage.storage_configuration_name} with prefix {prefix}")
                else:
                    print(f"Deleting storage configuration {storage.storage_configuration_name} with prefix {prefix}")
                    account_client.storage.delete(storage_configuration_id=storage.storage_configuration_id)  # Uncomment to perform actual deletion
        else:
            # If no prefix is provided, use creation time to filter storage configurations older than 2 weeks
            if creation_time < two_weeks_ago:
                if dry_run:
                    print(f"DRY RUN: Would delete storage configuration {storage.storage_configuration_name} created on {creation_time}")
                else:
                    print(f"Deleting storage configuration {storage.storage_configuration_name} created on {creation_time}")
                    account_client.storage.delete(storage_configuration_id=storage.storage_configuration_id)  # Uncomment to perform actual deletion



def nuke_credential_config(account_client: AccountClient, prefix: str, dry_run: bool):
    credential_list = account_client.credentials.list()  # Assuming this method lists all credential configurations
    two_weeks_ago = datetime.datetime.now() - datetime.timedelta(weeks=1)

    for credential in credential_list:
        # Convert epoch milliseconds to a datetime object
        creation_time = datetime.datetime.fromtimestamp(credential.creation_time / 1000.0)

        if prefix:
            # If a prefix is provided, use it to filter credential configurations
            if credential.credentials_name.startswith(prefix):
                if dry_run:
                    print(f"DRY RUN: Would delete credential {credential.credentials_name} with prefix {prefix}")
                else:
                    print(f"Deleting credential {credential.credentials_name} with prefix {prefix}")
                    account_client.credentials.delete(credentials_id=credential.credentials_id)  # Uncomment to perform actual deletion
        else:
            # If no prefix is provided, use creation time to filter credential configurations older than 2 weeks
            if creation_time < two_weeks_ago:
                if dry_run:
                    print(f"DRY RUN: Would delete credential {credential.credentials_name} created on {creation_time}")
                else:
                    print(f"Deleting credential {credential.credentials_name} created on {creation_time}")
                    account_client.credentials.delete(credentials_id=credential.credentials_id)  # Uncomment to perform actual deletion


def main(databricks_account_id, client_id, client_secret, prefix, dry_run: bool):
    print(f"Databricks Account ID: {databricks_account_id}")

    access_token = get_token(client_id, client_secret, databricks_account_id)
    account_client = build_account_client(access_token, databricks_account_id)
    
    #nuke_users(account_client, prefix, dry_run)
    nuke_metastore(account_client, prefix, dry_run)
    nuke_workspaces(account_client, prefix, dry_run)
    nuke_network_config(account_client, prefix, dry_run)
    nuke_storage_config(account_client, prefix, dry_run)
    nuke_credential_config(account_client, prefix, dry_run)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process Databricks Account details")
    parser.add_argument("--databricks_account_id", required=True, help="Databricks Account ID")
    parser.add_argument("--client_id", required=True, help="Client ID")
    parser.add_argument("--client_secret", required=True, help="Client Secret")
    parser.add_argument("--prefix", required=False, default=None, help="Prefix to filter resources to delete")
    parser.add_argument("--dry_run", action='store_true', help="Run the script in dry run mode")

    args = parser.parse_args()
    
    main(args.databricks_account_id, args.client_id, args.client_secret, args.prefix, args.dry_run)