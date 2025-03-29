import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()


def dbks_api_oauth_service_principal(uri, graph_token, management_token, resource_id):
    headers = {
      'Authorization':'Bearer ' + graph_token, 
      'X-Databricks-Azure-SP-Management-Token': management_token, 
      'X-Databricks-Azure-Workspace-Resource-Id' : resource_id
    }
    payload = {
        "lifetime_seconds": 300  # 5 minutes = 300 seconds
    }
    try:
        response = requests.post(uri, headers=headers)
        response.raise_for_status()
        return response.json()['token_value']
    except requests.exceptions.HTTPError as error:
        raise error
    except requests.ConnectionError as error:
        raise error
    
def get_token_microsof_graph_oauth(tenant_id, client_id, client_secret, resource_dbks_id='2ff814a6-3304-4ab8-85cb-cd0e6f879c1d'): # donot change resource_dbks_id
  headers = {'Content-Type': 'application/x-www-form-urlencoded'}
  payload = {'grant_type': 'client_credentials', 'client_id': client_id, 'resource' : resource_dbks_id, 'client_secret': client_secret, 'scope': 'https://graph.microsoft.com/.default'}

  try:
    response = requests.post('https://login.microsoftonline.com/'+tenant_id+'/oauth2/token', headers=headers, data=payload)
    response.raise_for_status()
    access_token = response.json()["access_token"]
    return access_token
  except requests.exceptions.HTTPError as error:
      print(error)
      print(response.json())
  except requests.ConnectionError as error:
      print(error)

def get_token_service_management_oauth(tenant_id, client_id, client_secret, management_resource_endpoint):
  headers = {'Content-Type': 'application/x-www-form-urlencoded'}
  payload = {'grant_type': 'client_credentials', 'client_id': client_id, 'resource': management_resource_endpoint, 'client_secret': client_secret}
  try:
    response = requests.post('https://login.microsoftonline.com/'+tenant_id+'/oauth2/token', headers=headers, data=payload)
    response.raise_for_status()
    access_token = response.json()["access_token"]
    return access_token
  except requests.exceptions.HTTPError as error:
    print(error)
    print(response.json())
  except requests.ConnectionError as error:
    print(error) 

# Azure AD credentials               
tenant_id = os.getenv('TENANT_ID')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
resource_id = os.getenv('RESOURCE_ID')

# get databricks domain
databricks_domain = os.getenv('DATABRICKS_HOST')

# get graph token
graph_token= get_token_microsof_graph_oauth(tenant_id, client_id, client_secret)

# get management token
management_resource_endpoint = 'https://management.core.windows.net/'
management_token=get_token_service_management_oauth(tenant_id, client_id, client_secret, management_resource_endpoint)

# use databricks API

uri=f'https://{databricks_domain}/api/2.0/token/create'

dbks_api_with_sp_token = dbks_api_oauth_service_principal(uri, graph_token, management_token, resource_id)
print("Databricks connection credentials:", "\nusername: token","\nPWD:", dbks_api_with_sp_token) 