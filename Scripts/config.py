import  os
from pathlib import Path

CONFLUENT_CONFIG = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('api_key',''),
    'sasl.password': os.environ.get('api_secret',''),
}

CONFLUENT_SCHEMA_REGISTRY_CONFIG = {
    'url': 'https://psrc-6ood18.us-east1.gcp.confluent.cloud',
    'basic.auth.user.info': f"{os.environ.get('sr_api_key','')}:{os.environ.get('sr_api_secret','')}"
}

 
CLOUDERA_SCHEMA_REGISTRY_CONFIG = {
    'url':'https://ccycloud.cdpy.root.comops.site:7790/api/v1/schemaregistry',
    'verify_ssl': False,  # Set to True if you want to verify SSL certificates
    'auth': {
        'username': os.environ.get('ldap_user', 'admin'),
        'password': os.environ.get('ldap_pass', 'Password1')
    }
}

CLOUDERA_KAFKA_BOOTSTRAP = {
    'bootstrap.servers': 'ccycloud.cdpy.root.comops.site:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('ldap_user', 'admin'),
    'sasl.password': os.environ.get('ldap_pass', 'Password1'),
    'ssl.ca.location': str(Path.home()) + '/certs/cm-auto-global_cacerts.pem',
    'group.id': 'cloudera_consumer_group',
    'auto.offset.reset': 'latest',
}

