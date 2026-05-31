# Security Policy

## Reporting a Vulnerability

Apache Kafka follows the [Apache Software Foundation security process](https://www.apache.org/security/).
Please report suspected vulnerabilities **privately** to `security@apache.org` (the Kafka PMC is reachable
at `private@kafka.apache.org`). Do **not** open public GitHub issues or pull requests for security reports.

## Threat Model

What Kafka treats as in/out of scope, the security properties it provides and disclaims (authentication via
SASL/mTLS, ACL authorization, transport security, RPC robustness/quotas), the adversary model (the untrusted
network client vs. the trusted operator and cluster peers), and how findings are triaged are documented in
[THREAT_MODEL.md](./THREAT_MODEL.md). Because Kafka is a configurable platform, many properties are
conditional on the operator's listener/authorizer/TLS configuration.
