# Databricks Resource Nuke Script

This script allows you to clean up various configurations in a Databricks account by deleting resources such as users, workspaces, network configurations, storage configurations, and credential configurations. You can filter resources by a specified prefix or delete them based on their age (older than 2 weeks). The script supports a "dry run" mode to preview the actions without making any changes.

## Features

- **Token Authentication:** Fetches an access token using OAuth 2.0 client credentials.
- **Resource Deletion:** Supports deletion of users, metastores, workspaces, network configurations, storage configurations, and credential configurations.
- **Prefix Filtering:** Optionally filter and delete resources whose names start with a specified prefix.
- **Age Filtering:** Delete resources older than 2 weeks if no prefix is provided.
- **Dry Run Mode:** Preview the actions that would be taken without actually deleting any resources.

## Requirements

- Python 3.x
- `databricks-sdk` Python package

## Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/databricks-nuke-script.git
   cd databricks-nuke-script

2. **Install the required Python packages:**

```bash
    pip install databricks-sdk
```


Usage
Run the script using the following command-line arguments:

--databricks_account_id: Your Databricks Account ID (required)
--client_id: Your OAuth Client ID (required)
--client_secret: Your OAuth Client Secret (required)
--prefix: Optional prefix to filter resources by name
--dry_run: Run the script in dry run mode to preview actions
Example Commands
To run the script with a specified prefix and in dry run mode: