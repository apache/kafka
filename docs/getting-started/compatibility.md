---
title: Compatibility
description: 
weight: 7
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


With the release of Kafka 4.0, significant changes have been introduced that impact compatibility across various components. To assist users in planning upgrades and ensuring seamless interoperability, a comprehensive compatibility matrix has been prepared. 

# JDK Compatibility Across Kafka Versions  
  
<table>  
<tr>  
<th>

Module
</th>  
<th>

Kafka Version
</th>  
<th>

Java 11
</th>  
<th>

Java 17
</th>  
<th>

Java 23
</th> </tr>  
<tr>  
<td>

Clients
</td>  
<td>

4.0.0
</td>  
<td>

✅
</td>  
<td>

✅
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

Streams
</td>  
<td>

4.0.0
</td>  
<td>

✅
</td>  
<td>

✅
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

Connect
</td>  
<td>

4.0.0
</td>  
<td>

❌
</td>  
<td>

✅
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

Server
</td>  
<td>

4.0.0
</td>  
<td>

❌
</td>  
<td>

✅
</td>  
<td>

✅
</td> </tr> </table>

**Note: Java 8 is removed in Kafka 4.0 and is no longer supported.**

# Server Compatibility  
  
<table>  
<tr>  
<th>

KRaft Cluster Version
</th>  
<th>

Compatibility 4.0 Server (dynamic voter)
</th>  
<th>

Compatibility 4.0 Server (static voter)
</th> </tr>  
<tr>  
<td>

before 3.2.x
</td>  
<td>

❌
</td>  
<td>

❌
</td> </tr>  
<tr>  
<td>

3.3.x
</td>  
<td>

❌
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

3.4.x
</td>  
<td>

❌
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

3.5.x
</td>  
<td>

❌
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

3.6.x
</td>  
<td>

❌
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

3.7.x
</td>  
<td>

❌
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

3.8.x
</td>  
<td>

❌
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

3.9.x
</td>  
<td>

✅
</td>  
<td>

✅
</td> </tr>  
<tr>  
<td>

4.0.x
</td>  
<td>

✅
</td>  
<td>

✅
</td> </tr> </table>

**Note: Can’t upgrade server from static voter to dynamic voter, see [KAFKA-16538](https://issues.apache.org/jira/browse/KAFKA-16538).**

## Client/Broker Forward Compatibility  

<table>
  <tr>
    <th>Kafka Version</th>
    <th>Module</th>
    <th>Compatibility with Kafka 4.0</th>
    <th>Key Differences/Limitations</th>
  </tr>
  <tr>
    <td rowspan="3">0.x, 1.x, 2.0</td>
    <td>Client</td>
    <td>❌ Not Compatible</td>
    <td>

Pre-0.10.x protocols are fully removed in Kafka 4.0 ([KIP-896](https://cwiki.apache.org/confluence/x/K5sODg)).
</td>
  </tr>
  <tr>
    <td>Streams</td>
    <td>❌ Not Compatible</td>
    <td>

Pre-0.10.x protocols are fully removed in Kafka 4.0 ([KIP-896](https://cwiki.apache.org/confluence/x/K5sODg)).</td>
  </tr>
  <tr>
    <td>Connect</td>
    <td>❌ Not Compatible</td>
    <td>

Pre-0.10.x protocols are fully removed in Kafka 4.0 ([KIP-896](https://cwiki.apache.org/confluence/x/K5sODg)).</td>
  </tr>
  <tr>
    <td rowspan="3">2.1 ~ 2.8</td>
    <td>Client</td>
    <td>⚠️ Partially Compatible</td>
    <td>

More details in the [Consumer](/40/documentation.html#upgrade_400_notable_consumer), [Producer](/40/documentation.html#upgrade_400_notable_producer), and [Admin Client](/40/documentation.html#upgrade_400_notable_admin_client) section.</td>
  </tr>
  <tr>
    <td>Streams</td>
    <td>⚠️ Limited Compatibility</td>
    <td>

More details in the [Kafka Streams](/40/documentation.html#upgrade_400_notable_kafka_streams) section.</td>
  </tr>
  <tr>
    <td>Connect</td>
    <td>⚠️ Limited Compatibility</td>
    <td>

More details in the [Connect](/40/documentation.html#upgrade_400_notable_connect) section.</td>
  </tr>
  <tr>
    <td rowspan="3">3.x</td>
    <td>Client</td>
    <td>✅ Fully Compatible</td>
    <td>—</td>
  </tr>
  <tr>
    <td>Streams</td>
    <td>✅ Fully Compatible</td>
    <td>—</td>
  </tr>
  <tr>
    <td>Connect</td>
    <td>✅ Fully Compatible</td>
    <td>—</td>
  </tr>
</table>

Note: Starting with Kafka 4.0, the `--zookeeper` option in AdminClient commands has been removed. Users must use the `--bootstrap-server` option to interact with the Kafka cluster. This change aligns with the transition to KRaft mode. 
