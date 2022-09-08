The current cert and private key checked in this directory expire Sept 1, 2023.
By then hopefully they aren't checked in anymore! But, if you want to create a
new self-signed cert/key pair, here's how:

```
openssl \
  req -x509 -newkey rsa:4096 \
  -keyout server.key -out ssl_certs/server.crt \
  -days 365 -sha256 -nodes \
  -subj '/CN=abq.rwx' \
  -extensions san \
  -config <( \
    echo '[req]'; \
    echo 'distinguished_name=req'; \
    echo '[san]'; \
    echo 'subjectAltName=DNS:abq.rwx' )
```
