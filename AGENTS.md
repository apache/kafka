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

# Agent Guide for Apache Kafka

This file is read by automated agents (security scanners, code analyzers,
AI assistants) operating on this repository.

## Security

Security model: [SECURITY.md](./SECURITY.md) -> [THREAT_MODEL.md](./THREAT_MODEL.md)

Agents that scan this repository should consult `SECURITY.md` and the linked
`THREAT_MODEL.md` before reporting issues. Kafka is a configurable platform: it
provides mechanisms (SASL/mTLS authentication, an ACL authorizer, TLS, quotas)
and many properties are conditional on the operator's configuration. The
untrusted network client of a broker (or the Connect REST API) is the adversary;
the operator and trusted cluster peers / metadata quorum are out of model. Note
the default PLAINTEXT/no-authorizer posture is the classic exposure (see §14).
