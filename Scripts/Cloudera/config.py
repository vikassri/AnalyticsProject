import os 

CLOUDERA_SCHEMA_REGISTRY_CONFIG = {
    'url': 'https://ccycloud.cdpy.root.comops.site:7790/api/v1/schemaregistry',
    'verify_ssl': False,  # Set to True if you want to verify SSL certificates
    'auth': {
        'username': os.environ.get('ldap_user', 'admin'),
        'password': os.environ.get('ldap_pass', 'admin')
    }
}

CLOUDERA_KAFKA_BOOTSTRAP = {
    'bootstrap.servers': 'ccycloud.cdpy.root.comops.site:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('ldap_user', 'admin'),
    'sasl.password': os.environ.get('ldap_pass', 'admin'),
    'ssl.ca.location': '/tmp/certs/cm-auto-global_cacerts.pem',
    'group.id': 'cloudera_consumer_group',
    'auto.offset.reset': 'earliest',
}

