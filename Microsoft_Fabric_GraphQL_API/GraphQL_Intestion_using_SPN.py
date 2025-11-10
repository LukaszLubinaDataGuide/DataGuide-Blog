from azure.identity import ClientSecretCredential
import requests
import json

# Define your Microsoft Entra credentials
tenant_id = "" # Your Tenant ID
client_id = "" # Service Principal Client ID
client_secret = "" # Service principal secret value

# The scope of the token to access Microsoft Fabric
scope = "https://api.fabric.microsoft.com/.default"

# Create a credential object with service principal details
credential = ClientSecretCredential(tenant_id, client_id, client_secret)

# Function to retrieve the token
def get_token():
    try:
        token = credential.get_token(scope)
        auth_token = token.token
        return auth_token
    except Exception as e:
        print("Error retrieving token:", str(e))

# Call the function and assign token to variable
auth_token = get_token()

# Prepare headers
headers = {
    'Authorization': f'Bearer {auth_token}',
    'Content-Type': 'application/json'
}

endpoint = '' # GraphQL Endpoint URL
query = """
    query {
  green_tripdata_2017s(first: 10) {
     items {
        VendorID
     }
  }
}
"""

variables = {

  }
  

# Issue GraphQL request
try:
    response = requests.post(endpoint, json={'query': query, 'variables': variables}, headers=headers)
    response.raise_for_status()
    data = response.json()
    print(json.dumps(data, indent=4))
except Exception as error:
    print(f"Query failed with error: {error}")
