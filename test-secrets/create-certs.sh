#!/bin/bash
set -e

# Configuration - Use same password for simplicity
STORE_PASS="kafkapass"
KEY_PASS="kafkapass"

# Clean up old certificates and auth files
rm -f *.crt *.key *.csr *.jks *.srl *_creds auth-ssl.json

# Generate CA private key and certificate
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 \
  -subj '/CN=kafka-ca' -nodes

# Generate server private key
openssl genrsa -out server.key 2048

# Generate server certificate signing request
openssl req -new -key server.key -out server.csr \
  -subj '/CN=localhost'

# Generate server certificate signed by CA
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 365 \
  -extfile <(echo -e "subjectAltName=DNS:localhost,IP:127.0.0.1")

# Clean up CSR
rm server.csr

# Convert to PKCS12 format first
openssl pkcs12 -export -in server.crt -inkey server.key -out server.p12 \
  -name localhost -CAfile ca.crt -caname ca -password pass:${KEY_PASS}

# Use Docker container to run keytool commands
KAFKA_IMAGE="apache/kafka:3.8.0"
CURRENT_DIR=$(pwd)

# Fix permissions for Docker container access
chmod 644 server.p12 ca.crt

# Create JKS keystore from PKCS12 using Kafka container
docker run --rm -v "${CURRENT_DIR}:/workspace" -w /workspace --user "$(id -u):$(id -g)" ${KAFKA_IMAGE} \
  keytool -importkeystore -deststorepass ${STORE_PASS} -destkeypass ${STORE_PASS} \
  -destkeystore kafka.keystore.jks -srckeystore server.p12 \
  -srcstoretype PKCS12 -srcstorepass ${KEY_PASS} -alias localhost

# Create truststore and import CA certificate using Kafka container
docker run --rm -v "${CURRENT_DIR}:/workspace" -w /workspace --user "$(id -u):$(id -g)" ${KAFKA_IMAGE} \
  keytool -keystore kafka.truststore.jks -alias ca -import -file ca.crt \
  -storepass ${STORE_PASS} -keypass ${KEY_PASS} -noprompt

# Create credential files
echo ${STORE_PASS} > kafka_keystore_creds
echo ${STORE_PASS} > kafka_ssl_key_creds  
echo ${STORE_PASS} > kafka_truststore_creds

# Clean up intermediate files
rm server.p12

# Generate JAAS configuration for Kafka server
cat > kafka_server_jaas.conf << 'EOF'
KafkaServer {
  org.apache.kafka.common.security.plain.PlainLoginModule required
  username="admin"
  password="admin-secret"
  user_admin="admin-secret"
  user_testuser="testpass";
};
EOF

# Generate auth-ssl.json file for SASL_SSL authentication
cat > auth-ssl.json << EOF
{
  "mode": "SASL_SSL",
  "ca-certificate": "test-secrets/ca.crt",
  "sasl_user": "testuser",
  "sasl_password": "testpass"
}
EOF

echo "Generated certificates and keystores:"
echo "- ca.crt (CA certificate)"
echo "- server.crt (Server certificate)"  
echo "- server.key (Server private key)"
echo "- kafka.keystore.jks (Server keystore)"
echo "- kafka.truststore.jks (CA truststore)"
echo "- kafka_keystore_creds (Keystore password file)"
echo "- kafka_ssl_key_creds (Key password file)"
echo "- kafka_truststore_creds (Truststore password file)"
echo "- kafka_server_jaas.conf (SASL JAAS configuration)"
echo "- auth-ssl.json (SASL_SSL authentication config)"
