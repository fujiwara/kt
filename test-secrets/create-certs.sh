#!/bin/bash
set -e

# Clean up old certificates
rm -f *.crt *.key *.csr *.jks *.srl *_creds

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

echo "Generated certificates:"
echo "- ca.crt (CA certificate)"
echo "- server.crt (Server certificate)"  
echo "- server.key (Server private key)"
