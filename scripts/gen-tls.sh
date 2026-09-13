#!/usr/bin/env bash
# Generate the self-signed test CA + broker cert committed under docker/.
# Only needed if regenerating fixtures; the checked-in files are used as-is.
set -euo pipefail
cd "$(dirname "$0")/.."

mkdir -p docker/tls docker/secrets
cd docker/tls

openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt \
    -days 3650 -nodes -subj "/CN=kite-test-ca"
openssl req -newkey rsa:2048 -keyout broker.key -out broker.csr \
    -nodes -subj "/CN=localhost"
openssl x509 -req -in broker.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out broker.crt -days 3650 \
    -extensions SAN -extfile <(printf "[SAN]\nsubjectAltName=DNS:localhost,IP:127.0.0.1")
rm -f broker.csr ca.srl

# apache/kafka's docker entrypoint expects a keystore file + password files
# under /etc/kafka/secrets (keystore password can't be set for PEM, so PKCS12).
cd ../secrets
openssl pkcs12 -export -in ../tls/broker.crt -inkey ../tls/broker.key \
    -certfile ../tls/ca.crt -name broker -out broker.p12 \
    -password pass:kite-tls
printf 'kite-tls' > key_creds
printf 'kite-tls' > keystore_creds
echo "wrote docker/tls/{ca.crt,ca.key,broker.key,broker.crt}, docker/secrets/{broker.p12,key_creds,keystore_creds}"
