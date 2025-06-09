import os
import json
import requests, urllib3
from requests.auth import HTTPBasicAuth

# === ENVIRONMENT CONFIGURATION ===
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Confluent Schema Registry (Cloud)
CONFLUENT_URL = 'https://psrc-6ood18.us-east1.gcp.confluent.cloud'
CONFLUENT_API_KEY = os.environ.get('sr_api_key')
CONFLUENT_API_SECRET = os.environ.get('sr_api_secret')
CONFLUENT_AUTH = HTTPBasicAuth(CONFLUENT_API_KEY, CONFLUENT_API_SECRET)

# Cloudera Schema Registry
CLOUDERA_URL = "https://ccycloud.cdpy.root.comops.site:7790/api/v1/schemaregistry"
VERIFY_SSL = False  # or False to skip verification

EXPORT_DIR = "exported_schemas"
SCHEMA_GROUP = "Kafka"  # or your desired schema group

# === EXPORT FROM CONFLUENT ===

def export_confluent_schemas():
    os.makedirs(EXPORT_DIR, exist_ok=True)
    subjects_url = f"{CONFLUENT_URL}/subjects"
    resp = requests.get(subjects_url, auth=CONFLUENT_AUTH)
    resp.raise_for_status()
    subjects = resp.json()

    schema_list = [topic.strip() for topic in open(os.path.join(".", "migrated_schema_list.txt"), "r")]
    print(f"🔍 Found {len(subjects)} subjects in Confluent.")
    for subject in schema_list:
        schema_url = f"{CONFLUENT_URL}/subjects/{subject}/versions/latest"
        schema_resp = requests.get(schema_url, auth=CONFLUENT_AUTH)
        schema_resp.raise_for_status()
        schema_data = schema_resp.json()

        file_path = os.path.join(EXPORT_DIR, f"{subject}.json")
        with open(file_path, "w") as f:
            json.dump(schema_data, f, indent=2)

        print(f"[✓] Exported schema for subject: {subject}")

# === IMPORT INTO CLOUDERA ===

def import_to_cloudera():
    for file in os.listdir(EXPORT_DIR):
        if not file.endswith(".json"):
            continue
        path = os.path.join(EXPORT_DIR, file)
        with open(path, "r") as f:
            schema_info = json.load(f)

        name = schema_info["subject"]
        schema_text = schema_info["schema"]
        schema_type = schema_info.get("schemaType", "avro")
        #schema_type = "avro"

        payload = {
            "name": name,
            "type": schema_type,
            "compatibility": "BACKWARD",
            "evolve": True,
            "schemaGroup": SCHEMA_GROUP,
            "description": f"Imported from Confluent Cloud: {name}"
        }
        url = f"{CLOUDERA_URL}/schemas"
        headers = {"Content-Type": "application/json"}

        resp = requests.post(url, json=payload, headers=headers,
                                  verify=VERIFY_SSL)
        if resp.status_code == 409:
            print(f"[!] Schema {name} already exists in Cloudera.")
        elif resp.ok:
            print(f"[✓] Imported {name} into Cloudera.")
        else:
            print(f"[✗] Failed to import {name}. Status: {resp.status_code}, Msg: {resp.text}")            
    
        if schema_type.lower() == "avro":
            schema_payload = {
                "schemaText": schema_text,
                "description": f"Version of {name} imported from Confluent Cloud",
            }
        else:
            # For non-Avro schemas, we assume JSON format
            # Adjust this logic if you have other schema types
            schema_payload = {
                "schemaText": json.loads(schema_text),
                "description": f"Version of {name} imported from Confluent Cloud",
            }
          
        schema_url = f"{CLOUDERA_URL}/schemas/{name}/versions"
        resp = requests.post(schema_url, json=schema_payload, headers=headers,
                                  verify=VERIFY_SSL)
        if resp.ok:
            print(f"[✓] Successfully added version for {name}.")
        else:
            print(f"[✗] Failed to add version for {name}. Status: {resp.status_code}, Msg: {resp.text}")

# === MAIN ===

if __name__ == "__main__":
    print("📤 Exporting schemas from Confluent Cloud...")
    export_confluent_schemas()

    print("\n📥 Importing schemas into Cloudera Schema Registry...")
    import_to_cloudera()