---
title: Kafka Connect Security Model
description: Apache Kafka Connect Security Model
weight: 9
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


This page extends the [Apache Kafka security model](../security-model) to Kafka Connect. A worker authenticates to the Kafka cluster over a configured `SASL_SSL`/`SSL` listener exactly like any other client, so everything the core model says about authentication, authorization, and transport encryption to the brokers applies unchanged. What follows covers only what Connect adds on top — chiefly its own control plane, the REST API, and the fact that it runs user-supplied code.

## Things You Need To Know

- **Connect inherits the broker's client security model.** Authentication to the brokers, broker-side authorization, and transport encryption are exactly as described in the [core security model](../security-model). This page only describes what Connect layers on top.
- **The REST API is unauthenticated by default.** Out of the box, anyone who can reach the REST port can create, reconfigure, stop, or delete any connector. Because connectors and plugins run arbitrary code, REST access lets a caller run anything the worker's installed plugins allow.
- **Connect plugins run arbitrary code.** Connectors, converters, transformations, predicates, and REST extensions loaded from `plugin.path` execute in the worker JVM with its privileges. Install only plugins you trust.
- **The REST API is a shared control plane with no per-connector isolation.** There is no notion of connector ownership: any caller allowed onto the API can act on every connector and read its configuration.
- **A worker authenticates to Kafka as a single principal.** By default all connectors on a worker share that principal's identity and ACLs; Connect does not give each connector a distinct Kafka identity. A connector can override the internal clients' configuration — including credentials — unless `connector.client.config.override.policy` restricts it (see Authorization below).
- **In distributed mode, Connect stores its state in Kafka topics.** Connector configurations (including any inlined secrets), source offsets, and status live in the `config.storage.topic`, `offset.storage.topic`, and `status.storage.topic`; protect them with ACLs as you would any sensitive topic. In standalone mode, offsets are kept in a local file (`offset.storage.file.filename`) instead, so its protection is the host filesystem's responsibility.

## The REST API and the Network Boundary

The REST API is configured with the `listeners` property (for example `http://host:port` or `https://host:port`); if unset it defaults to `http` on port 8083. In distributed mode the REST API is *also* the inter-worker transport — a request received by a follower is forwarded to the leader — so it is simultaneously a user-facing management interface and an internal control channel. `rest.advertised.host.name`, `rest.advertised.port`, and `rest.advertised.listener` control the URL other workers use to reach a node.

Operators should:

1. Never expose the REST API to untrusted networks or users; bind it to a management network or front it with a reverse proxy.
2. Use `admin.listeners` to separate the admin endpoints from the regular listeners, or set it to empty to disable them where they are not needed. If `admin.listeners` is not set, the `/admin` endpoints are attached to the default listeners.
3. Prefer an `https` listener so that both user traffic and inter-worker forwarding are encrypted.

## Authentication

Connect enables no authentication on the REST API by default. There are two common ways to add it:

- **Reverse proxy.** Terminate authentication (mTLS, OIDC, basic auth, etc.) in a proxy in front of the workers and allow only the proxy to reach the REST port.
- **REST extension.** Register an authentication extension via `rest.extension.classes`. The built-in `BasicAuthSecurityRestExtension` performs JAAS-based HTTP basic authentication against a configured `LoginModule`. The reference `PropertyFileLoginModule` is **not** intended for production, as it stores credentials in cleartext; production deployments should configure a `LoginModule` that authenticates against a real credential store.

REST authentication only establishes *who is calling*; it does not, on its own, authorize that caller on a per-connector basis (see Authorization below). Separately, the worker's authentication *to the Kafka brokers* uses the standard `ssl.*`/`sasl.*` client configs described in the [core model](../security-model).

## Authorization

This is the weakest part of Connect's security posture, and operators must plan around it:

- **No per-connector authorization on the REST API.** Once a caller is allowed onto the REST API it can act on *any* connector — reading another connector's configuration and stopping or deleting it. Authentication extensions gate access to the API as a whole, not to individual connectors.
- **Client config overrides are enabled by default.** `connector.client.config.override.policy` defaults to `All`, so a connector configuration can override the worker's internal Kafka client settings. Since 4.2.0 it is recommended to set this to `Allowlist` (which becomes the default in 5.0) and permit only the keys you actually need. Overrides that load classes — such as `sasl.jaas.config` and `sasl.login.class` — are by design a code-execution vector, so only allow them when only trusted users can reach the REST API.
- **One principal for all connectors.** Because the worker authenticates to Kafka as a single principal, ACLs cannot distinguish one connector from another. Partition that principal's ACLs by topic prefix per connector and grant it only the access its connectors require.

The practical consequences are that you cannot grant Connect REST API access to anyone you would not trust as a cluster administrator, and if multiple distrusting users or teams need Connect you should give each its own Connect cluster rather than sharing one.

## Encryption in Transit

Two independent channels need TLS:

- **Worker-to-Kafka.** Configured with the standard `ssl.*` client properties, exactly as in the [core model](../security-model).
- **REST API (and inter-worker).** Enable an `https` listener. By default the REST server reuses the worker's `ssl.*` settings; to configure the REST endpoint independently of the Kafka client, use the `listeners.https.*` prefixed properties (when the prefix is used, the unprefixed `ssl.*` settings are ignored for the REST server). The same settings secure inter-worker forwarding in distributed mode.

## Secrets in Configuration

Connector configurations frequently contain credentials for external systems. As in the [core model](../security-model), reference them indirectly through a `ConfigProvider` rather than inlining them, and set `allowed.paths` on the file-based providers to constrain which directories they can read. Two Connect-specific caveats:

- **Config providers are resolved through the shared REST API.** A caller who can guess or enumerate a provider alias can resolve its full value, so a provider is only as isolated as the REST API in front of it.
- **What goes in the config topic depends on how secrets are supplied.** A secret placed directly in a connector configuration is written to `config.storage.topic` as an ordinary Kafka record, and is therefore only as protected as that topic's ACLs and the brokers' at-rest story. A `ConfigProvider` reference keeps the secret itself out of the topic — only the template string (`${alias:fields}`) is stored, not the resolved value. Note, however, that this does not hide the secret from REST API callers: anyone who knows a valid template string can usually retrieve the resolved value through the REST API.

## Plugins

Connect loads connectors, converters, single-message transforms, predicates, `ConfigProvider`s, and REST extensions from `plugin.path`. All of them run in the worker JVM with its full privileges, so installing a plugin is equivalent to granting it arbitrary code execution on the worker. Install only plugins from sources you trust, and treat the ability to influence which plugins load — or to set class-loading connector configs — as administrator-level access.

## Known Non-Findings

In line with the [core model's classification](../security-model), the following follow from Connect's design and are not, on their own, security vulnerabilities:

- **File-based connectors granting disk access.** Adding the file connectors to a worker effectively grants read/write access to the worker's local disk. This is the connector's intended function, not a flaw.
- **Local-disk-only weaknesses in the file-based config providers.** Issues that require an attacker to already have local disk access on a Connect worker — for example the ability to create arbitrary files or symlinks — are not security issues, because such access is outside Connect's trust boundary. Path-validation robustness in these providers may still be hardened independently.
- **`PropertyFileLoginModule` storing cleartext credentials.** The reference `PropertyFileLoginModule` shipped with the basic auth extension is documented as test-only; production deployments are expected to supply a real `LoginModule`. Its cleartext storage is therefore expected behaviour rather than a defect.
