<!--
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Threat Model — Apache Kafka

## §1 Header

- **Project:** Apache Kafka — a distributed event-streaming platform. **Brokers** persist and serve
  partitioned, replicated topics over the Kafka RPC protocol; **producers/consumers/admin clients** connect
  over the network; cluster metadata is managed by a **KRaft** controller quorum (or, on legacy clusters,
  ZooKeeper). The repo also ships **Kafka Connect** (a connector runtime + REST control plane), **Kafka
  Streams** (a client library), tiered **storage**, and the **clients** library *(documented — README, module
  layout: `core`, `server`, `clients`, `metadata`, `raft`, `connect`, `streams`, `*-coordinator`, `storage`)*.
- **Modelled against:** `apache/kafka` `trunk`/HEAD (2026-05-31).
- **Status:** **DRAFT — v0, not yet reviewed by the Kafka PMC.** Produced by the ASF Security team via the
  `threat-model-producer` rubric (<https://gist.github.com/potiuk/da14a826283038ddfe38cc9fe6310573>).
- **Reporting / version-binding / legend** as in the sibling models. **Draft confidence:** ~16 documented /
  0 maintainer / ~58 inferred. Each *(inferred)* routes to §14.

**Framing note:** Kafka is a *configurable platform*. It provides **mechanisms** — SASL/mTLS authentication,
an ACL **Authorizer**, TLS transport, quotas — and the **operator chooses** which listeners use them. A
broker can be run wide open (PLAINTEXT, no authorizer) or fully locked down; the model says which outcomes
are `VALID` in a secured config vs. operator responsibility. The adversary is an **untrusted network client**
of a broker (or the Connect REST API); the operator and trusted cluster peers are out of model.

## §2 Scope and intended use

Caller roles:

- **Untrusted network client** — any peer that can open a TCP connection to a broker listener (or Connect
  REST) before authenticating.
- **Authenticated principal** — a producer/consumer/admin client whose SASL/mTLS identity the broker
  validated; confined by ACLs.
- **Broker / controller peer** — another broker or a KRaft controller in the same cluster; operator-provisioned.
- **Operator** — configures listeners, security protocols, SASL, TLS, the authorizer + ACLs, quotas, Connect,
  and storage. **Trusted; out of model as adversary (§3).**

**Component-family table:**

| Family | Entry point | Touches outside process | In model? |
| --- | --- | --- | --- |
| Broker RPC / network layer | listener `:9092`, request handlers (`core`, `server`) | network | **Yes** |
| Authentication | SASL (PLAIN/SCRAM/GSSAPI/OAUTHBEARER), mTLS, delegation tokens | crypto; (KDC/IdP) | **Yes** |
| Authorization | ACL `Authorizer` (StandardAuthorizer/KRaft) | metadata | **Yes** |
| Transport security | per-listener TLS, inter-broker security | network | **Yes** |
| Metadata control plane | KRaft quorum (`raft`, `metadata`) / ZooKeeper (legacy) | network | **Yes (peer-trust)** |
| Coordinators | group / transaction / share coordinators | — | **Yes** |
| Storage + tiered storage | log segments; remote-storage plugins | filesystem; remote store | **Yes** |
| Kafka Connect | REST control plane + connector plugins | network egress; plugin code | **Yes (addendum C)** |
| Kafka Streams | client library (runs in the app) | — | Light → §3 |
| Clients library | parses broker responses | — | **Yes (client-side)** |
| tools / shell / trogdor / tests / docker | — | — | No → §3 |

## §3 Out of scope (explicit non-goals)

- **The operator as adversary**, and pure misconfiguration — running a **PLAINTEXT listener with no
  authorizer**, permissive ACLs, weak SASL, or `allow.everyone.if.no.acl.found=true` on an exposed broker.
  Kafka provides the controls; choosing not to use them is operator responsibility (§9/§10/§11) *(inferred —
  but see §14 wave-1 on whether the *default* posture is "supported")*.
- **Trusted cluster peers and the metadata quorum** — a malicious broker/controller holding valid cluster
  credentials, or a compromised ZooKeeper/KRaft quorum, is out of the default adversary model (§7/§14).
- **Kafka Streams as a library** — it runs inside the application process under the app's trust; its threat
  surface is the app's, except where it acts as a Kafka *client* (covered by the clients family).
- **Connector plugins' own code** (operator-installed) — the Connect *runtime + REST control plane* is in
  model; a third-party connector's bugs are that connector's (and the operator chose to install it).
- **Tools, shell, trogdor, tests, docker, build** *(inferred)*.

## §4 Trust boundaries and data flow

The boundary is the **broker listener (and the Connect REST endpoint)**: connection bytes are untrusted until
the listener's configured **authentication** completes, and each request is then checked against **ACLs**
*(inferred — standard Kafka security model)*.

Trust transitions:

1. **Connect → authenticate:** on a secured listener, SASL/mTLS establishes the principal; on a PLAINTEXT
   listener there is no authentication and the principal is anonymous *(documented — security protocols)*.
2. **Request → authorize:** the `Authorizer` checks the principal's ACLs for the (resource, operation) —
   topic read/write, group, cluster, transactionalId, delegation-token operations *(inferred)*.
3. **Request → parse/process:** the broker decodes the RPC; request-size and quota/throttling limits bound
   resource use *(inferred — DoS surface)*.
4. **Inter-broker / controller:** replication and metadata flow between peers over the inter-broker listener;
   peers are mutually trusted within the cluster *(inferred)*.
5. **(C) Connect:** the REST control plane creates/updates connectors; connector configs may carry secrets and
   **URLs the connector will fetch** (SSRF surface); the REST endpoint's auth is operator-configured *(inferred)*.

**Reachability precondition:** a finding is in-model on a **secured** listener if reachable by an
unauthenticated or under-privileged principal before/around the auth+ACL gate; a finding that only manifests
on an intentionally-open PLAINTEXT/no-ACL config is `OUT-OF-MODEL: non-default-build` / misconfig **unless the
PMC rules the default open posture "supported"** (§14 wave-1).

## §5 Assumptions about the environment

- JVM brokers/controllers; operator-managed `server.properties`, keystores/truststores, JAAS/SASL config,
  and ACLs.
- A KRaft controller quorum (or ZooKeeper, legacy) on a trusted network *(inferred)*.
- TLS and SASL backends (Kerberos KDC / OAuth IdP / SCRAM store) are operator-provided *(inferred)*.
- Local disk (log segments) and any remote tiered-storage backend are operator-trusted *(inferred)*.
- **What Kafka does to its host (*(inferred)* — wave-2):** binds listeners; reads/writes log directories +
  keystores; connects to peers, the metadata quorum, and (Connect) configured external systems; not assumed
  to execute host commands outside connector plugins the operator installed.

## §5a Build-time and configuration variants

| Knob | Default *(documented/inferred)* | Effect | Ruling needed |
| --- | --- | --- | --- |
| listener `security.protocol` | **PLAINTEXT** out of the box | No auth / no TLS unless changed | **Open (wave-1):** is the open default a supported posture or operator-must-secure? |
| `authorizer.class.name` | **unset** (no ACL enforcement) by default | No authorization unless an authorizer is set | **Open (wave-1)** |
| `allow.everyone.if.no.acl.found` | typically `false` with StandardAuthorizer | Whether absent ACLs deny or allow | **Open (wave-1)** |
| SASL mechanism (PLAIN/SCRAM/GSSAPI/OAUTHBEARER) | per-config | Credential strength + transport requirement (PLAIN needs TLS) | Confirm guidance |
| inter-broker security protocol | per-config | Confidentiality/integrity between peers | Operator (§10) |
| Connect REST auth + TLS | per-config | Whether the connector control plane is authenticated | **Open (wave-1, C)** |
| delegation tokens | opt-in | Token-based auth surface | Confirm |
| quotas / `socket.request.max.bytes` / throttling | defaults | DoS envelope | Confirm (wave-3) |

## §6 Assumptions about inputs

| Entry point | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| broker listener | Kafka RPC requests (produce/fetch/metadata/admin), records | **yes** | auth listener; ACLs; request-size/quota limits |
| SASL handshake | mechanism + credentials/tokens | **yes** | strong mechanism; TLS for PLAIN; throttle |
| Connect REST | HTTP requests, connector config (URLs, secrets, class) | **yes** (if exposed) | REST auth; validate connector source/SSRF; secret handling |
| client library | broker responses | from **broker** (trusted) / a hostile broker for a client | robust client-side decode |
| inter-broker / KRaft | replication + metadata records | from **trusted** peers | peer auth (TLS/SASL) |
| `server.properties` / JAAS / ACLs / keystores | all | **no — operator-trusted** | never sourced from a request |

## §7 Adversary model

- **Primary adversary:** an untrusted network client of a broker listener (or the Connect REST API on an
  exposed deployment). Capabilities: open connections, attempt auth, send arbitrary/oversized/crafted RPCs,
  probe ACLs, push expensive workloads; on Connect, create connectors that fetch attacker-chosen URLs.
- **Secondary:** a malicious **broker response** vs. a client; an under-privileged authenticated principal
  attempting to exceed its ACLs.
- **Goals:** unauthenticated access / auth bypass; read or write topics/groups beyond ACLs; escalate via
  delegation tokens or transactional/idempotent producer state; SSRF/secret-exfil via Connect; DoS the broker.
- **Out of model:** the operator; anyone holding broker keystores/JAAS/ACL-admin; trusted cluster peers and
  the metadata quorum (pending §14).

## §8 Security properties the project provides

*(Conditional on a secured configuration; *(inferred)* pending §14.)*

1. **Authentication (when configured).** A secured listener authenticates the principal via SASL or mTLS
   before serving requests *(documented — security protocols)*. *Symptom:* unauthenticated access where auth
   was required; SASL bypass. *Severity:* critical.
2. **ACL authorization.** With an authorizer configured, each operation is allowed only if the principal's
   ACLs grant it on the resource *(documented — Authorizer)*. *Symptom:* read/write/admin beyond ACLs.
   *Severity:* critical.
3. **Transport security.** TLS provides confidentiality/integrity + (mTLS) peer authentication on listeners
   and inter-broker links when enabled *(documented)*. *Symptom:* MITM/downgrade where TLS expected.
   *Severity:* high.
4. **Robust RPC processing.** Malformed/oversized requests are rejected (request-size cap, quotas) rather than
   crashing or unboundedly consuming the broker *(inferred)*. *Symptom:* crash/OOM/hang from crafted RPC.
   *Severity:* high.
5. **Replication/metadata integrity within the cluster.** Replicated data and KRaft metadata remain consistent
   given honest peers *(inferred)*. *Symptom:* divergent replicas / corrupted metadata. *Severity:* high.

## §9 Security properties the project does NOT provide

- **No security on a PLAINTEXT / no-authorizer listener** — an exposed broker with default open settings is
  unauthenticated and unauthorized *(documented — PLAINTEXT default; §14 wave-1 decides VALID-vs-misconfig)*.
- **No transport security by default** — TLS is opt-in.
- **No defence against the operator or a trusted cluster peer** (§3).
- **(Connect) no intrinsic SSRF/secret protection** for connector configs the REST API accepts — validating
  connector source URLs and protecting secrets is the operator's job *(inferred)*.
- **(Streams)** runs in the application's trust domain; not a broker boundary.

**False friends:**

- *A PLAINTEXT listener "works" but is unauthenticated* — the most common Kafka exposure is an open broker on
  a routable network.
- *SASL/PLAIN looks like authentication but sends the password* — it requires TLS or it is sniffable.
- *Idempotent/transactional producer IDs look like identity but are not authorization* — ACLs still gate
  access.
- *An ACL on a topic is not confidentiality at rest* — disk/operator access bypasses it.

**Well-known attack classes to keep in view:** unauthenticated-broker exposure; SASL/PLAIN over plaintext;
ACL gaps (`allow.everyone.if.no.acl.found`); RPC/parser DoS and quota evasion; **Connect REST** unauthenticated
exposure and **SSRF / secret-exfil** via connector configs; ZooKeeper exposure on legacy clusters; deserialization
in connector/config plugins.

## §10 Downstream (operator) responsibilities

- **Do not expose a PLAINTEXT/no-authorizer broker** to an untrusted network — configure SASL or mTLS + TLS,
  set an `authorizer`, and define least-privilege ACLs; review `allow.everyone.if.no.acl.found`.
- Require **TLS** for client and inter-broker links; use SASL/PLAIN only over TLS.
- **Secure the Connect REST API** (auth + TLS) and validate connector configs (source URLs, secrets) — treat
  connector plugins as code you run.
- Protect the KRaft quorum / ZooKeeper and the metadata/inter-broker network.
- Set request-size limits and quotas; protect log directories and tiered-storage credentials at rest.
- Track ASF advisories and stay on a supported line.

## §11 Known misuse patterns

- Running a broker on `PLAINTEXT://0.0.0.0:9092` with no authorizer on a shared/routable network.
- Using SASL/PLAIN without TLS.
- Leaving the Connect REST API unauthenticated and internet-reachable.
- Accepting connector configs (URLs, class names) from untrusted users.
- Exposing ZooKeeper (legacy) without auth.

## §11a Known non-findings (recurring false positives)

*(v0 seed — the PMC will own the authoritative list — §14.)*

- **"Unauthenticated access / no TLS"** against a default/sample config — PLAINTEXT default is documented;
  `OUT-OF-MODEL: non-default-build` unless the PMC rules the open default unsupported (then `VALID` — §14).
- **"Admin/cluster operation succeeds for an authorized principal"** — by design; the admin is trusted (§7).
- **Connect SSRF via a connector the operator configured** with a trusted URL — trusted input (§6); SSRF from
  an untrusted REST caller (if the REST API is unauthenticated) is the real finding.
- **Findings in `tools`, `shell`, `trogdor`, `tests`, `docker`, samples** — out of scope (§3).
- **Streams application-level issues** — out of the broker model (§3).
- **Idempotent-producer / replication internals** not reachable from an unauthorized client — out of surface.

## §12 Conditions that would change this model

- A change to the default listener security / authorizer posture.
- A new client-reachable protocol, coordinator, or Connect default.
- A change to Connect REST auth defaults or connector-config validation.
- Treating cluster peers / the metadata quorum as untrusted (pulls them into §7).
- Any report not cleanly routable to a §13 disposition.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a claimed property via an in-scope adversary/input in a secured config. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse warrants a safer default/guard. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of config / keystores / ACL-admin / a connector config. | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator / peer / quorum capability. | §7, §3 |
| `OUT-OF-MODEL: unsupported-component` | Lands in tools/shell/trogdor/tests/streams-as-app. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests on an intentionally-open PLAINTEXT/no-ACL config. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (no security without config; ACL ≠ at-rest confidentiality). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Routes to none of the above → revise the model. | §12 |

## §14 Open questions for the maintainers

**Wave 1 — the default-posture rulings (decide VALID-vs-misconfig; §5a/§8/§9):**
1. Is running a broker with the **default PLAINTEXT listener and no authorizer** a *supported* posture (relying
   on network controls), so an "unauthenticated broker" report against defaults is `BY-DESIGN` — or should it
   be `VALID`? *Proposed:* operator must secure before exposing; open default is dev-only.
2. With the StandardAuthorizer, what is the default of **`allow.everyone.if.no.acl.found`**, and is "no ACL ⇒
   deny" the intended secured behavior? *Proposed:* deny by default under StandardAuthorizer.
3. Does the **Connect REST API** require authentication by default, and is connector-config URL handling
   considered an SSRF surface the runtime should guard? *Proposed:* REST auth is operator-configured; SSRF via
   operator-trusted configs is out of model, but an unauthenticated REST API is the real exposure.

**Wave 2 — auth/authz mechanics (§8):**
4. Which **SASL mechanisms** are recommended/discouraged by default, and does the broker enforce TLS for
   credential-exposing mechanisms (PLAIN)? *Proposed:* SCRAM/GSSAPI/OAUTHBEARER recommended; PLAIN requires TLS.
5. Are **delegation tokens** and idempotent/transactional state gated by ACLs the same as normal operations?
   *Proposed:* yes.

**Wave 3 — DoS, peers, §11a (§7/§8/§11a):**
6. What **request-size / quota / throttling** guarantees bound RPC DoS, and where is the resource line?
   *Proposed:* `socket.request.max.bytes` + quotas bound it; beyond that, operator config.
7. Confirm **cluster peers / the KRaft quorum / ZooKeeper** are trusted (out of §7). *Proposed:* yes.
8. What do scanners most often (re)report that the PMC considers a **non-finding**? (Seeds §11a.)

**Meta:**
9. Confirm this model lives as root `THREAT_MODEL.md` referenced from a new `SECURITY.md`, covering the broker
   + Connect with Streams treated as a client library. *Proposed:* yes.

## §15 Machine-readable companion

Deferred for v0; a `threat-model.yaml` can later encode the §6 trust table, §2/§3 scoping, §8 rows, §9 false
friends, §11a non-findings, and §13 dispositions.
