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
  over the network; cluster metadata is managed by a **KRaft** controller quorum. The repo also ships **Kafka
  Connect** (a connector runtime + REST control plane), **Kafka Streams** (a client library), tiered
  **storage**, and the **clients** library *(documented — README, module layout: `core`, `server`, `clients`,
  `metadata`, `raft`, `connect`, `streams`, `*-coordinator`, `storage`)*.
- **Modelled against:** `apache/kafka` `trunk`/HEAD (2026-05-31). ZooKeeper is no longer present on `trunk`
  (legacy 3.9.x clusters still ship it, but it is out of scope for this model) *(maintainer — clolov)*.
- **Status:** **DRAFT — v0, partially reviewed by the Kafka PMC.** Produced by the ASF Security team via the
  `threat-model-producer` rubric (<https://gist.github.com/potiuk/da14a826283038ddfe38cc9fe6310573>). First
  review pass by @clolov has confirmed several positions (tagged *(maintainer)*); items pinged to @mimaison,
  @showuon and @mjsax remain pending and are carried as §14 questions.
- **Reporting / version-binding / legend** as in the sibling models. **Draft confidence:** ~16 documented /
  ~9 maintainer-confirmed / ~46 inferred. Each *(inferred)* routes to §14.

**Framing note:** Kafka is a *configurable platform*. It provides **mechanisms** — SASL/mTLS authentication,
an ACL **Authorizer**, TLS transport, quotas — and the **operator chooses** which listeners use them. A
broker can be run wide open (PLAINTEXT, no authorizer) or fully locked down; the model says which outcomes
are `VALID` in a secured config vs. operator responsibility. **A PLAINTEXT listener with no authorizer is a
development-only posture** *(maintainer — clolov)*. The adversary is an **untrusted network client** of a
broker (or the Connect REST API); the operator and trusted cluster peers are out of model.

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
| Authorization | ACL `Authorizer` (StandardAuthorizer, KRaft) | metadata | **Yes** |
| Transport security | per-listener TLS, inter-broker security | network | **Yes** |
| Metadata control plane | KRaft quorum (`raft`, `metadata`) | network | **Yes (peer-trust)** |
| Coordinators | group / transaction / share coordinators | — | **Yes** |
| Storage + tiered storage | log segments; remote-storage plugins | filesystem; remote store | **Yes** |
| Kafka Connect | REST control plane + connector plugins | network egress; plugin code | **Yes (addendum C — §4)** |
| Kafka Streams | client library (runs in the app) | — | Client library → §3 *(maintainer — clolov, pending @mjsax)* |
| Clients library | parses broker responses | — | **Yes (client-side)** |
| tools / committer-tools / bin / shell / trogdor / tests / docker | — | — | No → §3 *(maintainer — clolov)* |

> **Addendum C** is the Kafka Connect treatment: throughout this model, paragraphs and table rows marked
> "(C)" or "addendum C" call out the additional surface that Connect adds beyond the broker — chiefly its
> **REST control plane** and the connector configs it accepts (URLs/secrets/class names). It is flagged
> separately because Connect exposes an HTTP API and may warrant deeper treatment than the broker RPC surface
> *(answering @clolov's "what is addendum C?"; depth pending @mimaison — §14)*.

## §3 Out of scope (explicit non-goals)

- **The operator as adversary**, and pure misconfiguration — running a **PLAINTEXT listener with no
  authorizer**, permissive ACLs, weak SASL, or `allow.everyone.if.no.acl.found=true` on an exposed broker.
  Kafka provides the controls; choosing not to use them is operator responsibility (§9/§10/§11). The
  **default open posture (PLAINTEXT + no authorizer) is development-only**, so an "unauthenticated broker"
  report against that default is `OUT-OF-MODEL: non-default-build` *(maintainer — clolov)*.
- **Trusted cluster peers and the metadata quorum** — a malicious broker/controller holding valid cluster
  credentials, or a compromised KRaft quorum, is out of the default adversary model *(maintainer — clolov:
  "trust cluster peers and KRaft quorum for now")*.
- **Kafka Streams as a library** — it runs inside the application process under the app's trust; its threat
  surface is the app's, except where it acts as a Kafka *client* (covered by the clients family). Treated as
  a **client library for now** *(maintainer — clolov; final call pending @mjsax — §14)*.
- **Connector plugins' own code** (operator-installed) — the Connect *runtime + REST control plane* is in
  model; a third-party connector's bugs are that connector's (and the operator chose to install it).
- **`tools`, `committer-tools`, `bin` (operator-run broker/CLI start scripts), `shell`, `trogdor`, `tests`,
  `docker`, build** — `bin`/`committer-tools` are operator/committer tooling, not a network surface
  *(maintainer — clolov)*. *(Whether this exclusion list should be exhaustive or a set of examples is open —
  §14.)*

## §4 Trust boundaries and data flow

The boundary is the **broker listener (and the Connect REST endpoint)**: connection bytes are untrusted until
the listener's configured **authentication** completes, and each request is then checked against **ACLs**
*(inferred — standard Kafka security model)*.

Trust transitions:

1. **Connect → authenticate:** on a secured listener, SASL/mTLS establishes the principal; on a PLAINTEXT
   listener there is no authentication and the principal is anonymous *(documented — security protocols)*.
2. **Request → authorize:** the `Authorizer` checks the principal's ACLs for the (resource, operation) —
   topic read/write, group, cluster, transactionalId, delegation-token operations. With the
   **StandardAuthorizer the default is DENY** (no matching ACL ⇒ denied)
   *(maintainer — clolov, source: `StandardAuthorizer.java`)*.
3. **Request → parse/process:** the broker decodes the RPC; request-size and quota/throttling limits bound
   resource use (see §5a / §8 for defaults) *(maintainer — clolov, for the specific defaults)*.
4. **Inter-broker / controller:** replication and metadata flow between peers over the inter-broker listener;
   peers are mutually trusted within the cluster *(maintainer — clolov: trust peers + KRaft quorum)*.
5. **(C) Connect — addendum C:** the REST control plane creates/updates connectors; connector configs may
   carry secrets and **URLs the connector will fetch** (SSRF surface); the REST endpoint's auth is
   operator-configured *(inferred; depth pending @mimaison — §14)*.

**Reachability precondition:** a finding is in-model on a **secured** listener if reachable by an
unauthenticated or under-privileged principal before/around the auth+ACL gate; a finding that only manifests
on an intentionally-open PLAINTEXT/no-ACL config is `OUT-OF-MODEL: non-default-build` / misconfig, because the
open default is a **development-only** posture *(maintainer — clolov)*.

## §5 Assumptions about the environment

- JVM brokers/controllers; operator-managed `server.properties`, keystores/truststores, JAAS/SASL config,
  and ACLs.
- A KRaft controller quorum on a trusted network *(maintainer — clolov)*.
- TLS and SASL backends (Kerberos KDC / OAuth IdP / SCRAM store) are operator-provided *(inferred)*.
- Local disk (log segments) and any remote tiered-storage backend are operator-trusted *(inferred)*.
- **What Kafka does to its host (*(inferred)* — wave-2):** binds listeners; reads/writes log directories +
  keystores; connects to peers, the metadata quorum, and (Connect) configured external systems; not assumed
  to execute host commands outside connector plugins the operator installed.

## §5a Build-time and configuration variants

| Knob | Default | Effect | Status |
| --- | --- | --- | --- |
| listener `security.protocol` | **PLAINTEXT** out of the box | No auth / no TLS unless changed | **Development-only posture** *(maintainer — clolov)* |
| `authorizer.class.name` | **unset** (no ACL enforcement) by default | No authorization unless an authorizer is set; once StandardAuthorizer is set, default is **DENY** | **Development-only when unset; DENY-by-default once set** *(maintainer — clolov)* |
| `allow.everyone.if.no.acl.found` | `false` | Whether absent ACLs deny or allow; with StandardAuthorizer, no ACL ⇒ **deny** | *(maintainer — clolov: default is DENY)* |
| SASL mechanism | per-config | SCRAM/GSSAPI/OAUTHBEARER recommended; **PLAIN exposes the password and nothing enforces TLS for it** | *(maintainer — clolov)* |
| OAUTHBEARER grant | per-config | Supports `client_credentials` and `client_assertion` (KIP-1258) | *(maintainer — clolov)* |
| inter-broker security protocol | per-config | Confidentiality/integrity between peers | Operator (§10) |
| Connect REST auth + TLS | per-config | Whether the connector control plane is authenticated | **Open (C):** depth pending @mimaison (§14) |
| delegation tokens | opt-in | Token-based auth surface; ACL-gated, and a token cannot mint another token | *(maintainer — clolov)* |
| `socket.request.max.bytes` | **100 MiB** | Caps single request size (DoS) | *(maintainer — clolov)* |
| `queued.max.requests` | **500** | Caps queued requests (DoS) | *(maintainer — clolov)* |
| `connection.failed.authentication.delay.ms` | **100 ms** | Delays response to failed auth (DoS / brute-force) | *(maintainer — clolov)* |
| `queued.max.request.bytes` | **unset** | Optional byte-bound on the request queue | *(maintainer — clolov)* |
| `max.connections` / `max.connections.per.ip` | **unset** | Optional connection caps | *(maintainer — clolov)* |
| `max.connection.creation.rate` | **unset** | Optional connection-rate cap | *(maintainer — clolov)* |
| quotas (produce/consume, request, controller mutations) | **unset** | Throughput / request / controller-mutation throttling when set | *(maintainer — clolov)* |

## §6 Assumptions about inputs

| Entry point | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| broker listener | Kafka RPC requests (produce/fetch/metadata/admin), records | **yes** | auth listener; ACLs; request-size/quota limits |
| SASL handshake | mechanism + credentials/tokens | **yes** | strong mechanism (SCRAM/GSSAPI/OAUTHBEARER); TLS for PLAIN; throttle |
| Connect REST (addendum C) | HTTP requests, connector config (URLs, secrets, class) | **yes** (if exposed) | REST auth; validate connector source/SSRF; secret handling |
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
- **Out of model:** the operator; anyone holding broker keystores/JAAS/ACL-admin; **trusted cluster peers and
  the KRaft quorum** *(maintainer — clolov)*. *(Concrete examples of in-cluster-peer threats that might one
  day warrant inclusion are solicited from @mimaison / @showuon — §14.)*

## §8 Security properties the project provides

*(Conditional on a secured configuration. Tagged *(maintainer)* where @clolov confirmed; otherwise *(inferred)*
pending §14.)*

1. **Authentication (when configured).** A secured listener authenticates the principal via SASL or mTLS
   before serving requests *(documented — security protocols)*. **Recommended SASL mechanisms:
   SCRAM/GSSAPI/OAUTHBEARER**; **SASL/PLAIN sends the password and nothing enforces TLS for it**, so it must
   only be used over TLS and everything over PLAINTEXT is development-only. **OAUTHBEARER supports the
   `client_credentials` and `client_assertion` grant types (KIP-1258)** *(maintainer — clolov)*. *Symptom:*
   unauthenticated access where auth was required; SASL bypass. *Severity:* critical.
2. **ACL authorization.** With an authorizer configured, each operation is allowed only if the principal's
   ACLs grant it on the resource; **the StandardAuthorizer defaults to DENY** when no ACL matches
   *(maintainer — clolov)*. **Idempotent producers, transactions, and delegation tokens are ACL-gated** like
   ordinary operations; delegation tokens additionally carry token-specific checks — in particular a principal
   authenticated *with* a delegation token cannot mint another token *(maintainer — clolov)*. *Symptom:*
   read/write/admin beyond ACLs. *Severity:* critical.
3. **Transport security.** TLS provides confidentiality/integrity + (mTLS) peer authentication on listeners
   and inter-broker links when enabled *(documented)*. *Symptom:* MITM/downgrade where TLS expected.
   *Severity:* high.
4. **Robust RPC processing.** Malformed/oversized requests are rejected and resource use is bounded rather
   than crashing or unboundedly consuming the broker. The DoS envelope is set by **`socket.request.max.bytes`
   (default 100 MiB)**, **`queued.max.requests` (default 500)**, and **`connection.failed.authentication.delay.ms`
   (default 100 ms)**; further bounds (**`queued.max.request.bytes`**, **`max.connections`/`max.connections.per.ip`**,
   **`max.connection.creation.rate`**, and produce/consume, request, and controller-mutation **quotas**) are
   **unset by default** and are the operator's lever *(maintainer — clolov)*. *Symptom:* crash/OOM/hang from
   crafted RPC. *Severity:* high.
5. **Replication/metadata integrity within the cluster.** Replicated data and KRaft metadata remain consistent
   given honest peers *(maintainer — clolov: peers + KRaft quorum trusted)*. *Symptom:* divergent replicas /
   corrupted metadata. *Severity:* high.

## §9 Security properties the project does NOT provide

- **No security on a PLAINTEXT / no-authorizer listener** — an exposed broker with default open settings is
  unauthenticated and unauthorized; **this is a development-only posture**, so such a report against defaults
  is `OUT-OF-MODEL: non-default-build` *(maintainer — clolov)*.
- **No transport security by default** — TLS is opt-in.
- **No defence against the operator or a trusted cluster peer / the KRaft quorum** (§3, §7).
- **No enforced TLS for SASL/PLAIN** — nothing in the broker forces PLAIN onto a TLS listener; protecting the
  password is the operator's responsibility *(maintainer — clolov)*.
- **(Connect — addendum C) no intrinsic SSRF/secret protection** for connector configs the REST API accepts —
  validating connector source URLs and protecting secrets is the operator's job *(inferred; depth pending
  @mimaison — §14)*.
- **(Streams)** runs in the application's trust domain; not a broker boundary.

**False friends:**

- *A PLAINTEXT listener "works" but is unauthenticated* — the most common Kafka exposure is an open broker on
  a routable network; this is a development-only configuration.
- *SASL/PLAIN looks like authentication but sends the password* — nothing enforces TLS for it, so it is
  sniffable unless the operator puts it on a TLS listener.
- *Idempotent/transactional producer IDs look like identity but are not authorization* — ACLs still gate
  access.
- *An ACL on a topic is not confidentiality at rest* — disk/operator access bypasses it.

**Well-known attack classes to keep in view:** unauthenticated-broker exposure; SASL/PLAIN over plaintext;
ACL gaps; RPC/parser DoS and quota evasion; **Connect REST (addendum C)** unauthenticated exposure and
**SSRF / secret-exfil** via connector configs; deserialization in connector/config plugins.

## §10 Downstream (operator) responsibilities

- **Do not expose a PLAINTEXT/no-authorizer broker** to an untrusted network — that posture is for
  development only. Configure SASL or mTLS + TLS, set an `authorizer` (StandardAuthorizer defaults to DENY),
  and define least-privilege ACLs *(maintainer — clolov)*.
- Require **TLS** for client and inter-broker links; use SASL/PLAIN **only** over TLS (prefer
  SCRAM/GSSAPI/OAUTHBEARER) — nothing enforces this for you *(maintainer — clolov)*.
- **Secure the Connect REST API** (auth + TLS) and validate connector configs (source URLs, secrets) — treat
  connector plugins as code you run (addendum C).
- Protect the KRaft quorum and the metadata/inter-broker network.
- Tune the DoS levers for your exposure: the active defaults are `socket.request.max.bytes` (100 MiB),
  `queued.max.requests` (500), and `connection.failed.authentication.delay.ms` (100 ms); explicitly set the
  unset-by-default `queued.max.request.bytes`, `max.connections`/`max.connections.per.ip`,
  `max.connection.creation.rate`, and quotas where appropriate *(maintainer — clolov)*.
- Protect log directories and tiered-storage credentials at rest.
- Track ASF advisories and stay on a supported line.

## §11 Known misuse patterns

- Running a broker on `PLAINTEXT://0.0.0.0:9092` with no authorizer on a shared/routable network (a
  development-only posture exposed in production).
- Using SASL/PLAIN without TLS.
- Leaving the Connect REST API unauthenticated and internet-reachable.
- Accepting connector configs (URLs, class names) from untrusted users.

## §11a Known non-findings (recurring false positives)

*(v0 seed — the PMC will own the authoritative list — §14.)*

- **"Unauthenticated access / no TLS"** against a default/sample config — the PLAINTEXT + no-authorizer
  default is a **development-only** posture; `OUT-OF-MODEL: non-default-build` *(maintainer — clolov)*.
- **"Admin/cluster operation succeeds for an authorized principal"** — by design; the admin is trusted (§7).
- **Connect SSRF via a connector the operator configured** with a trusted URL — trusted input (§6); SSRF from
  an untrusted REST caller (if the REST API is unauthenticated) is the real finding.
- **Findings in `tools`, `committer-tools`, `bin`, `shell`, `trogdor`, `tests`, `docker`, samples** — out of
  scope (§3) *(maintainer — clolov)*.
- **Streams application-level issues** — out of the broker model (§3).
- **Idempotent-producer / replication internals** not reachable from an unauthorized client — these are
  broker-internal state (e.g. the producer-ID / sequence state for idempotence, and replication state) that
  is not directly exposed to an unauthorized client and is ACL-gated where it is reachable; they are grouped
  together as broker-internal-state surface rather than client-facing surface *(answering @clolov's grouping
  question; idempotent/transactional access itself is ACL-gated — §8.2)*.

## §12 Conditions that would change this model

- A change to the default listener security / authorizer posture.
- A new client-reachable protocol, coordinator, or Connect default.
- A change to Connect REST auth defaults or connector-config validation.
- Treating cluster peers / the KRaft quorum as untrusted (pulls them into §7).
- A decision to model Kafka Streams or the Connect REST surface in their own right (currently Streams is a
  client library and Connect is addendum C).
- Any report not cleanly routable to a §13 disposition.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a claimed property via an in-scope adversary/input in a secured config. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse warrants a safer default/guard. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of config / keystores / ACL-admin / a connector config. | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator / peer / quorum capability. | §7, §3 |
| `OUT-OF-MODEL: unsupported-component` | Lands in tools/committer-tools/bin/shell/trogdor/tests/streams-as-app. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests on an intentionally-open PLAINTEXT/no-ACL (development-only) config. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (no security without config; ACL ≠ at-rest confidentiality; no enforced TLS for PLAIN). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry. | §11a |
| `MODEL-GAP` | Routes to none of the above → revise the model. | §12 |

## §14 Open questions for the maintainers

*(Several wave-1/wave-2/wave-3 items from the v0 draft are now answered by @clolov and folded in above as
*(maintainer)*; the items below are the ones still genuinely open or pending another reviewer.)*

**Pending other reviewers:**
1. **(@mimaison / @showuon)** Are there concrete **in-cluster-peer threats** (a malicious broker/controller
   holding valid cluster credentials) worth eventually modelling, even though peers + the KRaft quorum are
   trusted *for now*? *(Solicited by @clolov; no examples yet.)*
2. **(@mjsax)** Confirm **Kafka Streams** should be treated as a **client library** (current position) rather
   than modelled in its own right. *(Folded as the working position; final call pending.)*
3. **(@mimaison)** **Kafka Connect (addendum C)** — because Connect exposes a **REST API**, does it warrant a
   deeper, Connect-specific treatment (REST auth defaults, connector-config validation, SSRF/secret handling)
   than the broker-RPC surface? *(clolov flagged this as likely needing more depth.)*

**Open modelling decisions:**
4. Should the §3 / §11a **out-of-scope component list be exhaustive or a set of examples**? The current list
   (`tools`, `committer-tools`, `bin`, `shell`, `trogdor`, `tests`, `docker`, build) is treated as examples of
   operator/non-network surface; the PMC may prefer an exhaustive enumeration. *(Raised by @clolov.)*

**Meta:**
5. Confirm where this model should live. @clolov's preference is a new page under **`docs/security`** (with a
   fallback of keeping it as root `THREAT_MODEL.md` if doc-build formatting breaks), referenced from a
   `SECURITY.md`, covering the broker + Connect with Streams treated as a client library. *(Confirm
   destination + that the SECURITY.md pointer is acceptable.)*

## §15 Machine-readable companion

Deferred for v0; a `threat-model.yaml` can later encode the §6 trust table, §2/§3 scoping, §8 rows, §9 false
friends, §11a non-findings, and §13 dispositions.
