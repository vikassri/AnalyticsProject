import constants, os
from pathlib import Path
 
CLOUDERA_SCHEMA_REGISTRY_CONFIG = {
    'url': constants.cloudera_constants.sr_url,
    'verify_ssl': False,  # Set to True if you want to verify SSL certificates
    'auth': {
        'username': os.environ.get('ldap_user', 'admin'),
        'password': os.environ.get('ldap_pass', 'admin')
    }
}

CLOUDERA_KAFKA_BOOTSTRAP = {
    'bootstrap.servers': constants.cloudera_constants.kafka_brokers,
    'security.protocol': constants.cloudera_constants.kafka_security_protocol,
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.environ.get('ldap_user', 'admin'),
    'sasl.password': os.environ.get('ldap_pass', 'admin'),
    'ssl.ca.location': str(Path.home()) + '/certs/cm-auto-global_cacerts.pem',
    'group.id': 'cloudera_consumer_group',
    'auto.offset.reset': 'earliest',
}

