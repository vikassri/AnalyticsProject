import os

CONFLUENT_CONFIG = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('api_key',''),
    'sasl.password': os.environ.get('api_secret',''),
}

CONFLUENT_SCHEMA_REGISTRY_CONFIG = {
    'url': 'https://psrc-6ood18.us-east1.gcp.confluent.cloud',
    'user': os.environ.get('sr_api_key',''),
    'password' : os.environ.get('sr_api_secret',''),
    'basic.auth.user.info': f"{os.environ.get('sr_api_key','')}:{os.environ.get('sr_api_secret','')}"
}