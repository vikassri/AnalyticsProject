import requests
import urllib3
import avro.schema as avro_schema
import json
from config import CLOUDERA_SCHEMA_REGISTRY_CONFIG

class ClouderaSchemaManager:
    def __init__(self, registry_url=CLOUDERA_SCHEMA_REGISTRY_CONFIG['url'], verify_ssl=False):
        self.registry_url = registry_url.rstrip("/")
        self.verify_ssl = verify_ssl
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def get_latest_schema(self, schema_name):
        url = f"{self.registry_url}/schemas/{schema_name}/versions/latest"
        response = requests.get(url, verify=self.verify_ssl)
        response.raise_for_status()
        schema_json = response.json()["schemaText"]
        if type(schema_json) is str:
            schema_json = schema_json.replace("'", '"')
            return schema_json
        return avro_schema.parse(schema_json)

    def get_all_versions(self, schema_name):
        url = f"{self.registry_url}/schemas/{schema_name}/versions"
        response = requests.get(url, verify=self.verify_ssl)
        response.raise_for_status()
        return response.json()

    def get_schema_version(self, schema_name, version_number):
        url = f"{self.registry_url}/schemas/{schema_name}/versions/{version_number}"
        response = requests.get(url, verify=self.verify_ssl)
        response.raise_for_status()
        schema_json = response.json()["schemaText"]
        return avro_schema.parse(schema_json)

    def register_schema(self, type, schema_name, schema_group, schema_text, compatibility="BACKWARD", description="Registered via API"):
        # Step 1: Register schema metadata
        metadata_url = f"{self.registry_url}/schemas"
        if type.lower() == "avro":
            metadata_payload = {
                "name": schema_name,
                "type": type.lower(),
                "schemaGroup": schema_group,
                "compatibility": compatibility,
                "description": description,
                "evolve": True
            }
        elif type.lower() == "json":
            metadata_payload = {
                "name": schema_name,
                "type": type.lower(),
                "schemaGroup": schema_group,
                "description": description,
            }
        else:
            raise ValueError(f"Unsupported schema type: {type}. Only 'avro' and 'json' are supported.")
            exit(1)
            
        response = requests.post(metadata_url, json=metadata_payload, verify=self.verify_ssl)
        if response.status_code == 409:
            print(f"Schema '{schema_name}' already exists. Continuing...")
        elif not response.ok:
            raise Exception(f"Failed to register schema metadata: {response.text}")

        # Step 2: Register actual schema version
        version_url = f"{self.registry_url}/schemas/{schema_name}/versions"
        version_payload = {
            "schemaText": schema_text,
            "description": f"Version registered via API"
        }

        version_resp = requests.post(version_url, json=version_payload, verify=self.verify_ssl)
        version_resp.raise_for_status()
        print(f"✅ Registered version for schema '{schema_name}'")
        return version_resp.json()

    def check_compatibility(self, schema_name, new_schema_text):
        url = f"{self.registry_url}/compatibility/{schema_name}/latest"
        payload = {
            "schemaText": new_schema_text
        }
        response = requests.post(url, json=payload, verify=self.verify_ssl)
        response.raise_for_status()
        return response.json()

    def delete_schema(self, schema_name):
        url = f"{self.registry_url}/schemas/{schema_name}"
        response = requests.delete(url, verify=self.verify_ssl)
        if response.status_code == 200:
            print(f"🗑️ Deleted schema '{schema_name}'")
        else:
            print(f"⚠️ Failed to delete schema '{schema_name}': {response.status_code} {response.text}")

    def pretty_print_schema(self, schema_name, version="latest"):
        if version == "latest":
            schema_obj = self.get_latest_schema(schema_name)
        else:
            schema_obj = self.get_schema_version(schema_name, version)
        print(json.dumps(json.loads(str(schema_obj)), indent=2))
        
