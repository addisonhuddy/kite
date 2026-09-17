# Security policy

## Reporting a vulnerability

Report vulnerabilities through GitHub private vulnerability reporting:
[Security → Report a vulnerability](https://github.com/addisonhuddy/kite/security/advisories/new).

Please do not open a public issue for a vulnerability. Include the affected
version, reproduction steps, impact, and any relevant configuration details.

## Supported versions

Only the latest release is supported with security fixes.

## Sensitive configuration

kite handles SASL credentials and TLS configuration. Treat properties files
containing credentials as secrets and restrict them to the owner:

```sh
chmod 600 ./kite.properties
```

Redact credentials from logs, issue reports, and diagnostic output.
