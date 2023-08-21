import os
import subprocess
from config import PROJECT_ID,  GC_KEY


gc_key = GC_KEY
project_id = PROJECT_ID

# Function to execute given command
def execute_command(cmd):
    """
    Execute the given command and return its output.
    """
    try:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        out, err = process.communicate()
        if process.returncode != 0:
            print(f"Error executing command: {cmd}\nError message: {err.decode()}")
            return None
        return out.decode().strip()
    except Exception as e:
        print(f"Error while executing command {cmd}. Error: {str(e)}")
        return None

def set_google_credentials(gc_key):
    """
    Set GOOGLE_APPLICATION_CREDENTIALS environment variable for authentication.
    """
    try:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = gc_key
    except Exception as e:
        print(f"Failed to set GOOGLE_APPLICATION_CREDENTIALS. Error: {str(e)}")



def gcp_setup():

    # Set Project
    project_name = project_id
    execute_command(f'gcloud config set project {project_name}')

    # Set Google credentials
    set_google_credentials(gc_key)

    # Get current project's project number
    list_cmd = f'gcloud projects list --filter="{project_name}" --format="value(PROJECT_NUMBER)" --limit=1'
    project_number = execute_command(list_cmd)
    if project_number is None:
        return

    # Add the Cloud Composer v2 API Service Agent Extension role
    add_iam_cmd = (f'gcloud iam service-accounts add-iam-policy-binding '
                   f'{project_number}-compute@developer.gserviceaccount.com '
                   f'--member serviceAccount:service-{project_number}@cloudcomposer-accounts.iam.gserviceaccount.com '
                   f'--role roles/composer.ServiceAgentV2Ext')
    
    result = execute_command(add_iam_cmd)
    if result:
        print(result)
