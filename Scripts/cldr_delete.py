import os
import json
import requests, urllib3
from requests.auth import HTTPBasicAuth

# === ENVIRONMENT CONFIGURATION ===
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Cloudera Schema Registry
CLOUDERA_URL = "https://ccycloud.cdpy.root.comops.site:7790/api/v1/schemaregistry"
CLOUDERA_AUTH = HTTPBasicAuth("", "")
VERIFY_SSL = False  # or False to skip verification

def delete_schema(schema_name):
    url = f"{CLOUDERA_URL}/schemas/{schema_name}"
    resp = requests.delete(url, auth=CLOUDERA_AUTH, verify=VERIFY_SSL)
    
    if resp.status_code == 200:
        print(f"[✓] Successfully deleted schema: {schema_name}")
    elif resp.status_code == 404:
        print(f"[!] Schema not found: {schema_name}")
    else:
        print(f"[!] Error deleting schema {schema_name}: {resp.status_code} - {resp.text}")
        

def delete_all_schemas():
    url = f"{CLOUDERA_URL}/schemas"
    resp = requests.get(url, auth=CLOUDERA_AUTH, verify=VERIFY_SSL)
    
    if resp.status_code != 200:
        print(f"[!] Error fetching schemas: {resp.status_code} - {resp.text}")
        return
    
    for schema in resp.json()['entities']:
        name = schema['schemaMetadata']['name']
        delete_schema(name)
        
delete_all_schemas()