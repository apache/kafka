---
title: Security Model
description: Apache Kafka Security Model
weight: 8
tags: ['kafka', 'docs', 'security']
aliases:
keywords:
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


## Things You Need To Know

- **Security is off by default.** A freshly-installed Apache Kafka cluster accepts unauthenticated `PLAINTEXT` connections on every listener and applies no authorization. This is appropriate only for closed test environments. Production deployments **must** explicitly configure authentication, authorization, and transport encryption before being exposed to any untrusted network.
- **Apache Kafka assumes a trusted operator.** Anyone with shell access to a broker, controller, or the underlying disks can read every topic, forge any principal, and rewrite ACLs. The security model protects messages in transit and arbitrates client access — it does not defend brokers from their own administrators.
- **Apache Kafka assumes a trusted broker fleet.** Brokers and KRaft controllers exchange records, replication state, and metadata over the inter-broker and controller listeners. Any host that can authenticate on those listeners is effectively part of the cluster's trust boundary.
- **Apache Kafka trusts its JVM and classpath.** JARs on a process's classpath run with that process's full privileges. Placing code on the classpath — or configuring a component to load it — is equivalent to trusting that code; the model assumes no hostile JARs are present.
- **Pluggable interfaces grant code execution.** Every Apache Kafka component exposes pluggable interfaces (authorizers, (de)serializers, connectors, transforms, config providers, and similar) that are loaded by class name and run with the host process's privileges. Configuring one is equivalent to running its code, so only install and use plugins that you fully trust.
- **The data plane and the control plane have different exposure.** Producer/consumer traffic, the Admin API, and JMX each have distinct authentication and authorization stories. Operators must configure them independently — securing one does not secure the others.
- **Apache Kafka does not encrypt data at rest.** Log segments, index files, and snapshots are written as plain bytes. At-rest confidentiality is the responsibility of the underlying filesystem, block device, or message-level encryption performed by producers.
- **Reporting vulnerabilities.** Suspected security issues should be reported privately to `security@kafka.apache.org` per the [ASF security process](https://www.apache.org/security/). Do not file public JIRA tickets, GitHub issues, or mailing-list posts for unpatched vulnerabilities.

## Listeners and the Network Boundary

Apache Kafka brokers expose one or more **listeners**, each with an independent security configuration selected by `listener.security.protocol.map`. The four protocols are:

| Protocol         | Authentication         | Encryption |
|------------------|------------------------|------------|
| `PLAINTEXT`      | None                   | None       |
| `SSL`            | Optional mTLS          | TLS        |
| `SASL_PLAINTEXT` | SASL                   | None       |
| `SASL_SSL`       | SASL (+ optional mTLS) | TLS        |

`inter.broker.listener.name` and `controller.listener.names` select which listeners carry replication and KRaft traffic respectively. A common pattern is to keep these on a dedicated internal listener (`SASL_SSL` or `SSL`) that is firewalled off from clients, so that a compromise of a client-facing listener cannot impersonate a broker.

Operators should:

1. Bind external listeners only to interfaces reachable by intended clients.
2. Treat `advertised.listeners` as part of the security configuration — clients connect to whatever the broker advertises after the initial metadata fetch.
3. Never expose the controller listener to client networks.

## Authentication

Apache Kafka supports two complementary authentication mechanisms; either may be used, and both can be combined on a `SASL_SSL` listener.

### TLS Client Authentication (mTLS)

When `ssl.client.auth` is `required` on a TLS listener, the client's X.509 certificate is verified against the broker's truststore. The authenticated principal is derived from the certificate's distinguished name via `ssl.principal.mapping.rules` (or a custom `KafkaPrincipalBuilder`).

mTLS is the recommended mechanism for broker-to-broker and controller-to-broker traffic, because it requires no shared password material and rotates with the rest of the PKI.

### SASL

Apache Kafka ships with five SASL mechanisms, enabled per-listener via `sasl.enabled.mechanisms`:

- **`GSSAPI`** — Kerberos. Recommended for environments that already operate a KDC; principals and credentials are managed externally.
- **`SCRAM-SHA-256` / `SCRAM-SHA-512`** — Salted challenge/response with credentials stored in the cluster metadata. Credentials are managed with `kafka-configs.sh --alter --add-config 'SCRAM-SHA-512=...'`.
- **`OAUTHBEARER`** — OAuth 2.0 bearer tokens, suitable for integration with an identity provider. The default unsecured implementation is for testing only; production deployments must configure a JWKS endpoint and validator.
- **`PLAIN`** — Username/password sent in cleartext over the SASL channel. Acceptable only inside a `SASL_SSL` listener; never use it with `SASL_PLAINTEXT`.

#### Delegation Tokens

Once a client has authenticated via SASL or mTLS, it can request a short-lived **delegation token** that is then used as a `SCRAM-SHA-256` credential for subsequent connections. Delegation tokens are intended for distributed frameworks (Spark, Flink, Connect workers) that need to fan out to many tasks without distributing the original credential. Tokens inherit the requester's principal and ACLs, expire on a fixed schedule (`delegation.token.expiry.time.ms`), and can be invalidated by the owner.

## Authorization

Authentication establishes a `KafkaPrincipal`; authorization decides what that principal may do. Authorization is performed by the configured `authorizer.class.name`. Apache Kafka ships `org.apache.kafka.metadata.authorizer.StandardAuthorizer` for KRaft clusters.

ACLs are tuples of `(principal, host, operation, resource pattern, permission)`. Resources are typed (`Topic`, `Group`, `Cluster`, `TransactionalId`, `DelegationToken`, `User`) and patterns may be `LITERAL` or `PREFIXED`.

Defaults worth understanding:

- If no authorizer is configured, **all authenticated principals have full access**. Configuring authentication without an authorizer provides identity but no authorization.
- If an authorizer is configured but no ACLs match, access is **denied**. The exception is the principals listed in `super.users`, which bypass ACL checks entirely; treat that list as you would a root password.
- `allow.everyone.if.no.acl.found=true` reverses the default-deny behaviour for resources that have no ACLs at all. It is a transitional aid for adding authorization to existing clusters and should not remain set in steady state.

ACLs are managed with `kafka-acls.sh` or the AdminClient `createAcls`/`deleteAcls` APIs, which are themselves gated by ACLs on the `Cluster` resource.

## Encryption in Transit

TLS is configured per-listener via the standard `ssl.*` properties (`ssl.keystore.*`, `ssl.truststore.*`, `ssl.protocol`, `ssl.cipher.suites`, `ssl.enabled.protocols`). Recommendations:

- Disable TLS versions below 1.2; prefer 1.3 where the JDK supports it.
- Use distinct keystores for the inter-broker listener and any client-facing listener so that a leaked client-facing key cannot impersonate a broker.
- Set `ssl.endpoint.identification.algorithm=https` on clients (the default since 2.0) so that the broker's certificate must match its hostname.
- Rotate keystores using the dynamic broker configuration mechanism (`kafka-configs.sh --entity-type brokers --alter --add-config ...`) to avoid restarts.

Kafka Connect, MirrorMaker 2, and Kafka Streams all consume the same `ssl.*` and `sasl.*` client configs — securing the broker is necessary but not sufficient.

## Encryption at Rest

Apache Kafka does not encrypt log segments, indexes, snapshots, or controller metadata on disk. Operators who require at-rest confidentiality have three options, in increasing order of cost:

1. **Filesystem or block-device encryption** Transparent to Kafka; protects against disk theft and misdirected backups but not against anyone with broker login.
2. **Message-level encryption.** Producers encrypt payloads (and optionally headers) before `send()`; consumers decrypt. Keys are managed by an external KMS. This is the only option that protects records from broker operators, but it precludes broker-side features that read payloads (e.g. Streams aggregations on the encrypted field).
3. **Tiered storage** with a remote store that performs its own encryption.

## Audit Logging

Apache Kafka emits authorizer decisions to the `kafka.authorizer.logger` log4j logger. Setting this logger to `INFO` records every denied request; `DEBUG` records every allowed request as well. In regulated environments this log should be shipped to durable, append-only storage off-broker. There is no built-in tamper-evident audit trail.

The request log (`kafka.request.logger`) provides finer detail on individual API calls and is useful for forensic investigation, but it is verbose and not enabled by default.

## Secrets in Configuration

Broker, client, and Connect properties files contain keystore passwords, SASL credentials, and similar secrets. Apache Kafka supports indirect references through `ConfigProvider` implementations (`FileConfigProvider`, `DirectoryConfigProvider`, or custom providers). Use them rather than embedding cleartext secrets in version-controlled configuration. For the file-based providers, set `allowed.paths` to the specific directories that hold those secrets so that a malicious or mistaken configuration cannot coerce the provider into reading arbitrary files elsewhere on the host. Sensitive dynamic broker configurations are encrypted at rest in the metadata log using `password.encoder.secret`; rotating that secret requires `password.encoder.old.secret` and a rolling restart.

## Component-Specific Notes

- **JMX.** Kafka processes, such as brokers, clients, Connect and Streams, expose operational metrics over JMX. JMX is an administrators/operators-only interface and must never be exposed to actual users. It is unauthenticated by default and should either be disabled, bound to localhost with an exporter alongside, or configured with `com.sun.management.jmxremote.authenticate=true` and TLS.

The components built on top of the Kafka clients have their own security models, covered on separate pages:

- [Kafka Connect](../security-model-connect)
- [Kafka Streams](../security-model-streams)

## Development and Test Tooling

Not everything shipped in the Apache Kafka source tree is part of the production attack surface. Some components exist only to develop, test, and release Kafka itself, and are explicitly out of scope for the security model — they are expected to run only in trusted development and CI environments, and issues in them are generally not treated as security vulnerabilities.

- **Trogdor.** Trogdor is a test framework that injects faults and runs workloads by design, including arbitrary user-supplied commands. It is intended to run only in development environments; the project does not consider command execution through Trogdor a security issue.
- **System tests and release tooling.** The `tests/` system-test harness and the scripts under `release/` are operator/developer tooling for building, testing, and publishing Kafka. They are not components of a running cluster.

When assessing the attack surface of a deployed cluster, scope it to the brokers, KRaft controllers, the client and inter-broker/controller listeners, the Admin API, and JMX — not the development tooling above.

## Supported Versions and Artifacts

Only the currently supported releases and `trunk` are in scope for security reports; issues that reproduce only on an end-of-life release should be reproduced against a supported version first.

The published Docker images are a special case. They are free of known CVEs on their release day, but the project does not rebuild already-released images, so over time their base layers and bundled dependencies accumulate CVEs that are not specific to Apache Kafka. Those accumulated CVEs are therefore out of scope; operators are expected to rebuild or patch images for long-running deployments.

## Classifying Reports

To keep triage consistent, a reported issue is treated as exactly one of:

- **A vulnerability** — it breaks one of the security properties above for an adversary that is in scope, such as an unauthenticated or unauthorized network client.
- **A hardening suggestion** — no stated property is broken, but a safer default or an added guard would reduce the blast radius of misuse.
- **Out of scope** — it requires capabilities the model already treats as trusted (operator-supplied configuration, keystores, ACL administration), an adversary the model does not cover (a trusted operator, peer broker, or controller), or an unsupported component (see Development and Test Tooling above).
- **By design** — it concerns a property the model explicitly disclaims, such as at-rest confidentiality or isolation from a trusted operator.

## Known Non-Findings

The following follow directly from the model above and are not, on their own, security vulnerabilities:

- **Unauthenticated or unencrypted access to a default cluster.** Security is off by default; an open `PLAINTEXT` listener with no authorizer is a deployment choice, not a defect.
- **A trusted principal performing an authorized operation.** Admin and inter-broker actions by a principal that holds the relevant ACLs — or by a `super.users` entry — are expected behaviour.
- **Findings in development and test tooling.** Issues in `tools`, `bin`, Trogdor, `tests`, and similar are out of scope (see Development and Test Tooling above).
- **Kafka Streams application-level issues.** Streams runs inside the user's application, so its security boundary is the application's, not the broker's (see the [Kafka Streams security model](../security-model-streams)).

## Reporting Security Issues

Suspected vulnerabilities should be sent to `security@kafka.apache.org`. Please do not disclose the issue publicly until the PMC has had time to investigate, prepare a fix, and coordinate a release. The current list of disclosed CVEs and affected version ranges is published at [kafka.apache.org/cve-list](https://kafka.apache.org/cve-list).
