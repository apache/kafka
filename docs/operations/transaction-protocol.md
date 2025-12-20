---
title: Transaction Protocol
description: Transaction Protocol
weight: 11
tags: ['kafka', 'docs']
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


## Overview

Starting from Apache Kafka 4.0, Transactions Server Side Defense ([KIP-890](https://cwiki.apache.org/confluence/display/KAFKA/KIP-890%3A+Transactions+Server-Side+Defense)) brings a strengthened transactional protocol. When enabled and using 4.0 producer clients, the producer epoch is bumped on every transaction to ensure every transaction includes the intended messages and duplicates are not written as part of the next transaction.

The protocol is automatically enabled on the server since Apache Kafka 4.0. Enabling and disabling the protocol is controlled by the `transaction.version` feature flag. This flag can be set using the storage tool on new cluster creation, or dynamically to an existing cluster via the features tool. Producer clients starting 4.0 and above will use the new transactional protocol as long as it is enabled on the server.

## Upgrade & Downgrade

To enable the new protocol on the server, set `transaction.version=2`. The producer clients do not need to be restarted, and will dynamically upgrade the next time they connect or re-connect to a broker. (Alternatively, the client can be restarted to force this connection). A producer will not upgrade mid-transaction, but on the start of the next transaction after it becomes aware of the server-side upgrade.

Downgrades are safe to perform and work similarly. The older protocol will be used by the clients on the first transaction after the producer becomes aware of the downgraded protocol.

## Performance

The new transactional protocol improves performance over verification by only sending a single call to add partitions on the server side, rather than one from the client to add and one from the server to verify.

One consequence of this change is that we can no longer use the hardcoded retry backoff introduced by [KAFKA-5477](https://issues.apache.org/jira/browse/KAFKA-5477). Due to the asynchronous nature of the `endTransaction` api, the client can start adding partitions to the next transaction before the markers are written. When this happens, the server will return `CONCURRENT_TRANSACTIONS` until the previous transaction completes. Rather than the default client backoff for these retries, there was a shorter retry backoff of 20ms.

Now with the server-side request, the server will attempt to retry adding the partition a few times when it sees the `CONCURRENT_TRANSACTIONS` error before it returns the error to the client. This can result in higher produce latencies reported on these requests. The transaction end to end latency (measured from the time the client begins the transaction to the time to commit) does not increase overall with this change. The time just shifts from client-side backoff to being calculated as part of the produce latency.

The server-side backoff and total retry time can be configured with the following new configs:

  * `add.partitions.to.txn.retry.backoff.ms`
  * `add.partitions.to.txn.retry.backoff.max.ms`


