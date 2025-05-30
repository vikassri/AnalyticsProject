import os
import json
import requests
from requests.auth import HTTPBasicAuth

class ClouderaSchemaRegistryKnox:
    def __init__(self, knox_url, topology, user, password, verify_ssl=True):
        self.base_url = f"{knox_url}/gateway/{topology}/schema-registry/api/v1"
        self.auth = HTTPBasicAuth(user, password)
        self.verify = verify_ssl
        self.headers = {"Content-Type": "application/json"}

    def list_schemas(self):
        resp = requests.get(f"{self.base_url}/schemaregistry/schemas", auth=self.auth, verify=self.verify)
        resp.raise_for_status()
        return resp.json()

    def get_schema(self, name):
        resp = requests.get(f"{self.base_url}/schemaregistry/schemas/{name}", auth=self.auth, verify=self.verify)
        resp.raise_for_status()
        return resp.json()

    def export_all(self, output_dir="schemas_knox_export"):
        os.makedirs(output_dir, exist_ok=True)
        for schema in self.list_schemas():
            name = schema["name"]
            full_schema = self.get_schema(name)
            with open(f"{output_dir}/{name}.json", "w") as f:
                json.dump(full_schema, f, indent=2)
            print(f"[✓] Exported: {name}")

    def import_schema(self, file_path):
        with open(file_path, "r") as f:
            schema_data = json.load(f)
        resp = requests.post(f"{self.base_url}/schemaregistry/schemas", 
                             auth=self.auth, headers=self.headers, 
                             json=schema_data, verify=self.verify)
        if resp.status_code == 409:
            print(f"[!] Schema exists: {schema_data['name']}")
        else:
            resp.raise_for_status()
            print(f"[✓] Imported: {schema_data['name']}")

    def import_all(self, input_dir="schemas_knox_export"):
        for file in os.listdir(input_dir):
            if file.endswith(".json"):
                self.import_schema(os.path.join(input_dir, file))


# === Example Usage ===
if __name__ == "__main__":
    registry = ClouderaSchemaRegistryKnox(
        knox_url="https://ccycloud.cdpy.root.comops.site:8443",
        topology="cdp-proxy",
        user="vikas",
        password="Password1",
        verify_ssl=False  # Or False to disable
    )

    registry.delete("Exported/user-profiles-value.json")