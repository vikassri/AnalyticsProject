from schema_client import SchemaManager
import json

# This script registers all schemas for the Kafka topics used in the project.
def register_all_schemas():
    schema_manager = SchemaManager()
    
    schemas = {
        'customer-events-value': f'{open("../schemas/CustomerEvent.json").read()}',
        'order-events-value':    f'{open("../schemas/OrderEvent.json").read()}',
        'inventory-events-value': f'{open("../schemas/InventoryEvent.json").read()}',
        'user-profiles-value':   f'{open("../schemas/UserProfile.json").read()}',
    }
    
    for subject, schema_str in schemas.items():
        try:
            print(f"Registering schema for subject: {subject}")
            schema_registered = schema_manager.register_schema(subject,schema_str)
            print(f"Schema registered successfully with ID: {schema_registered}")
        except Exception as e:
            print(f"Error registering schema: {e}")

if __name__ == "__main__":
    register_all_schemas()
