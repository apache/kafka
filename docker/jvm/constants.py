SCHEMA_REGISTRY_URL="http://localhost:8081"
CONNECT_URL="http://localhost:8083/connectors"
CLIENT_TIMEOUT=40
SCHEMA_REGISTRY_TEST_TOPIC="test_topic_schema"
CONNECT_TEST_TOPIC="test_topic_connect"
CONNECT_SOURCE_CONNECTOR_CONFIG="@fixtures/source_connector.json"

SSL_TOPIC="test_topic_ssl"
SSL_CA_LOCATION="./fixtures/secrets/ca-cert"
SSL_CERTIFICATE_LOCATION="./fixtures/secrets/client_python_client.pem"
SSL_KEY_LOCATION="./fixtures/secrets/client_python_client.key"
SSL_KEY_PASSWORD="abcdefgh"

BROKER_RESTART_TEST_TOPIC="test_topic_broker_restart"

SCHEMA_REGISTRY_ERROR_PREFIX="SCHEMA_REGISTRY_ERR"
CONNECT_ERROR_PREFIX="CONNECT_ERR"
SSL_ERROR_PREFIX="SSL_ERR"
BROKER_RESTART_ERROR_PREFIX="BROKER_RESTART_ERR"