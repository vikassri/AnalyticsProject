# schema_client.py
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from config import CONFLUENT_SCHEMA_REGISTRY_CONFIG
import json

class SchemaManager:
    def __init__(self):
        self.schema_registry_client = SchemaRegistryClient(CONFLUENT_SCHEMA_REGISTRY_CONFIG)
        
    def register_schema(self, subject, schema_str):
        try:
            schema = Schema(schema_str=json.dumps(schema_str), schema_type="JSON")
            schema_id = self.schema_registry_client.register_schema(subject,schema)
            return schema_id
        except Exception as e:
            print(f"Error registering schema for {subject}: {e}")
            return None
    
    def get_avro_serializer(self, schema_str):
        return AvroSerializer(self.schema_registry_client, schema_str)
    
    def get_avro_deserializer(self):
        return AvroDeserializer(self.schema_registry_client)
    
    def export_schema(self, subject):
        try:
            schema = self.schema_registry_client.get_latest_version(subject)
            return schema
        except Exception as e:
            print(f"Error exporting schema for {subject}: {e}")
            return None
        
    def export_all_schemas(self):
        try:
            subjects = self.schema_registry_client.get_subjects()
            schemas = {}
            print(f"Exporting schemas for subjects: {subjects}")
            for subject in subjects:
                schema = self.export_schema(subject)
                if schema:
                    schemas[subject] = schema
            return schemas
        except Exception as e:
            print(f"Error exporting all schemas: {e}")
            return None
        
    def delete_schema(self, subject):
        try:
            self.schema_registry_client.delete_subject(subject)
            print(f"Schema for {subject} deleted successfully.")
        except Exception as e:
            print(f"Error deleting schema for {subject}: {e}")
    
    def list_schemas(self):
        try:
            subjects = self.schema_registry_client.get_subjects()
            return subjects
        except Exception as e:
            print(f"Error listing schemas: {e}")
            return None
        
    def get_schema_by_id(self, schema_id):
        try:
            schema = self.schema_registry_client.get_schema(schema_id)
            return schema.schema_str
        except Exception as e:
            print(f"Error getting schema by ID {schema_id}: {e}")
            return None
        
        
scm = SchemaManager()
schemas = scm.list_schemas()
for schema in schemas:
    print(scm.export_schema(schema))
# Example usage