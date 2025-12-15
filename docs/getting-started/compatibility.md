---
title: Compatibility
description: 
weight: 7
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

With the release of Kafka 4.0, significant changes have been introduced that impact compatibility across various components. To assist users in planning upgrades and ensuring seamless interoperability, a comprehensive compatibility matrix has been prepared. 

# JDK Compatibility Across Kafka Versions

Module | Kafka Version | Java 11 | Java 17 | Java 23  
---|---|---|---|---  
Clients | 4.0.0 | ✅ | ✅ | ✅  
Streams | 4.0.0 | ✅ | ✅ | ✅  
Connect | 4.0.0 | ❌ | ✅ | ✅  
Server | 4.0.0 | ❌ | ✅ | ✅  
  
**Note: Java 8 is removed in Kafka 4.0 and is no longer supported.**

# Server Compatibility

KRaft Cluster Version | Compatibility 4.0 Server (dynamic voter) | Compatibility 4.0 Server (static voter)  
---|---|---  
before 3.2.x | ❌ | ❌  
3.3.x | ❌ | ✅  
3.4.x | ❌ | ✅  
3.5.x | ❌ | ✅  
3.6.x | ❌ | ✅  
3.7.x | ❌ | ✅  
3.8.x | ❌ | ✅  
3.9.x | ✅ | ✅  
4.0.x | ✅ | ✅  
  
**Note: Can’t upgrade server from static voter to dynamic voter, see[KAFKA-16538](https://issues.apache.org/jira/browse/KAFKA-16538).**

### Client/Broker Forward Compatibility

Kafka Version | Module | Compatibility with Kafka 4.0 | Key Differences/Limitations  
---|---|---|---  
0.x, 1.x, 2.0 | Client | ❌ Not Compatible | Pre-0.10.x protocols are fully removed in Kafka 4.0 ([KIP-896](https://cwiki.apache.org/confluence/x/K5sODg)).   
Streams | ❌ Not Compatible | Pre-0.10.x protocols are fully removed in Kafka 4.0 ([KIP-896](https://cwiki.apache.org/confluence/x/K5sODg)).   
Connect | ❌ Not Compatible | Pre-0.10.x protocols are fully removed in Kafka 4.0 ([KIP-896](https://cwiki.apache.org/confluence/x/K5sODg)).   
2.1 ~ 2.8 | Client | ⚠️ Partially Compatible |  More details in the [Consumer](/40/documentation.html#upgrade_400_notable_consumer), [Producer](/40/documentation.html#upgrade_400_notable_producer), and [Admin Client](/40/documentation.html#upgrade_400_notable_admin_client) section.   
Streams | ⚠️ Limited Compatibility |  More details in the [Kafka Streams](/40/documentation.html#upgrade_400_notable_kafka_streams) section.   
Connect | ⚠️ Limited Compatibility |  More details in the [Connect](/40/documentation.html#upgrade_400_notable_connect) section.   
3.x | Client | ✅ Fully Compatible |   
Streams | ✅ Fully Compatible |   
Connect | ✅ Fully Compatible |   
  
Note: Starting with Kafka 4.0, the `--zookeeper` option in AdminClient commands has been removed. Users must use the `--bootstrap-server` option to interact with the Kafka cluster. This change aligns with the transition to KRaft mode. 
