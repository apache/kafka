---
title: MirrorMaker 2 Security Model
description: Apache Kafka MirrorMaker 2 Security Model
weight: 11
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


This page extends the [Apache Kafka security model](security-model) to MirrorMaker. MirrorMaker is built on Kafka Connect, so the [Connect security model](security-model-connect) applies in full; what follows is specific to replicating across clusters.

## Things You Need To Know

- **It spans two trust boundaries at once.** A MirrorMaker instance authenticates to both a source and a target cluster; configure `source.cluster.*` and `target.cluster.*` independently, each with its own credentials and TLS settings.
- **Never replicate in cleartext across an untrusted network.** Cross-cluster traffic frequently leaves a single security domain, so secure both client connections with TLS rather than tunnelling plaintext.
- **It is a Connect deployment.** Its REST control plane, plugin model, and single-principal-per-worker caveats are exactly those of the [Connect security model](security-model-connect).
