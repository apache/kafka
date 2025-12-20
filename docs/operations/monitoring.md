---
title: Monitoring
description: Monitoring
weight: 8
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


Kafka uses Yammer Metrics for metrics reporting in the server. The Java clients use Kafka Metrics, a built-in metrics registry that minimizes transitive dependencies pulled into client applications. Both expose metrics via JMX and can be configured to report stats using pluggable stats reporters to hook up to your monitoring system. 

All Kafka rate metrics have a corresponding cumulative count metric with suffix `-total`. For example, `records-consumed-rate` has a corresponding metric named `records-consumed-total`. 

The easiest way to see the available metrics is to fire up jconsole and point it at a running kafka client or server; this will allow browsing all metrics with JMX. 

## Security Considerations for Remote Monitoring using JMX

Apache Kafka disables remote JMX by default. You can enable remote monitoring using JMX by setting the environment variable `JMX_PORT` for processes started using the CLI or standard Java system properties to enable remote JMX programmatically. You must enable security when enabling remote JMX in production scenarios to ensure that unauthorized users cannot monitor or control your broker or application as well as the platform on which these are running. Note that authentication is disabled for JMX by default in Kafka and security configs must be overridden for production deployments by setting the environment variable `KAFKA_JMX_OPTS` for processes started using the CLI or by setting appropriate Java system properties. See [Monitoring and Management Using JMX Technology](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html) for details on securing JMX. 

We do graphing and alerting on the following metrics:   
<table>  
<tr>  
<th>

Description
</th>  
<th>

Mbean name
</th>  
<th>

Normal value
</th> </tr>  
<tr>  
<td>

Message in rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=([-.\w]+)
</td>  
<td>

Incoming message rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Byte in rate from clients
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=([-.\w]+)
</td>  
<td>

Byte in (from the clients) rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Byte in rate from other brokers
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesInPerSec
</td>  
<td>

Byte in (from the other brokers) rate across all topics.
</td> </tr>  
<tr>  
<td>

Controller Request rate from Broker
</td>  
<td>

kafka.controller:type=ControllerChannelManager,name=RequestRateAndQueueTimeMs,brokerId=([0-9]+)
</td>  
<td>

The rate (requests per second) at which the ControllerChannelManager takes requests from the queue of the given broker. And the time it takes for a request to stay in this queue before it is taken from the queue.
</td> </tr>  
<tr>  
<td>

Controller Event queue size
</td>  
<td>

kafka.controller:type=ControllerEventManager,name=EventQueueSize
</td>  
<td>

Size of the ControllerEventManager's queue.
</td> </tr>  
<tr>  
<td>

Controller Event queue time
</td>  
<td>

kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs
</td>  
<td>

Time that takes for any event (except the Idle event) to wait in the ControllerEventManager's queue before being processed
</td> </tr>  
<tr>  
<td>

Request rate
</td>  
<td>

kafka.network:type=RequestMetrics,name=RequestsPerSec,request={Produce|FetchConsumer|FetchFollower},version=([0-9]+)
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Error rate
</td>  
<td>

kafka.network:type=RequestMetrics,name=ErrorsPerSec,request=([-.\w]+),error=([-.\w]+)
</td>  
<td>

Number of errors in responses counted per-request-type, per-error-code. If a response contains multiple errors, all are counted. error=NONE indicates successful responses.
</td> </tr>  
<tr>  
<td>

Produce request rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec,topic=([-.\w]+)
</td>  
<td>

Produce request rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Fetch request rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=TotalFetchRequestsPerSec,topic=([-.\w]+)
</td>  
<td>

Fetch request (from clients or followers) rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Failed produce request rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=FailedProduceRequestsPerSec,topic=([-.\w]+)
</td>  
<td>

Failed Produce request rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Failed fetch request rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=FailedFetchRequestsPerSec,topic=([-.\w]+)
</td>  
<td>

Failed Fetch request (from clients or followers) rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Request size in bytes
</td>  
<td>

kafka.network:type=RequestMetrics,name=RequestBytes,request=([-.\w]+)
</td>  
<td>

Size of requests for each request type.
</td> </tr>  
<tr>  
<td>

Temporary memory size in bytes
</td>  
<td>

kafka.network:type=RequestMetrics,name=TemporaryMemoryBytes,request={Produce|Fetch}
</td>  
<td>

Temporary memory used for message format conversions and decompression.
</td> </tr>  
<tr>  
<td>

Message conversion time
</td>  
<td>

kafka.network:type=RequestMetrics,name=MessageConversionsTimeMs,request={Produce|Fetch}
</td>  
<td>

Time in milliseconds spent on message format conversions.
</td> </tr>  
<tr>  
<td>

Message conversion rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name={Produce|Fetch}MessageConversionsPerSec,topic=([-.\w]+)
</td>  
<td>

Message format conversion rate, for Produce or Fetch requests, per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Request Queue Size
</td>  
<td>

kafka.network:type=RequestChannel,name=RequestQueueSize
</td>  
<td>

Size of the request queue.
</td> </tr>  
<tr>  
<td>

Byte out rate to clients
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=([-.\w]+)
</td>  
<td>

Byte out (to the clients) rate per topic. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Byte out rate to other brokers
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=ReplicationBytesOutPerSec
</td>  
<td>

Byte out (to the other brokers) rate across all topics
</td> </tr>  
<tr>  
<td>

Rejected byte rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=BytesRejectedPerSec,topic=([-.\w]+)
</td>  
<td>

Rejected byte rate per topic, due to the record batch size being greater than max.message.bytes configuration. Omitting 'topic=(...)' will yield the all-topic rate.
</td> </tr>  
<tr>  
<td>

Message validation failure rate due to no key specified for compacted topic
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=NoKeyCompactedTopicRecordsPerSec
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Message validation failure rate due to invalid magic number
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=InvalidMagicNumberRecordsPerSec
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Message validation failure rate due to incorrect crc checksum
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=InvalidMessageCrcRecordsPerSec
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Message validation failure rate due to non-continuous offset or sequence number in batch
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=InvalidOffsetOrSequenceRecordsPerSec
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Log flush rate and time
</td>  
<td>

kafka.log:type=LogFlushStats,name=LogFlushRateAndTimeMs
</td>  
<td>


</td> </tr>  
<tr>  
<td>

\# of offline log directories
</td>  
<td>

kafka.log:type=LogManager,name=OfflineLogDirectoryCount
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Leader election rate
</td>  
<td>

kafka.controller:type=ControllerStats,name=LeaderElectionRateAndTimeMs
</td>  
<td>

non-zero when there are broker failures
</td> </tr>  
<tr>  
<td>

Unclean leader election rate
</td>  
<td>

kafka.controller:type=ControllerStats,name=UncleanLeaderElectionsPerSec
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Is controller active on broker
</td>  
<td>

kafka.controller:type=KafkaController,name=ActiveControllerCount
</td>  
<td>

only one broker in the cluster should have 1
</td> </tr>  
<tr>  
<td>

Pending topic deletes
</td>  
<td>

kafka.controller:type=KafkaController,name=TopicsToDeleteCount
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Pending replica deletes
</td>  
<td>

kafka.controller:type=KafkaController,name=ReplicasToDeleteCount
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Ineligible pending topic deletes
</td>  
<td>

kafka.controller:type=KafkaController,name=TopicsIneligibleToDeleteCount
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Ineligible pending replica deletes
</td>  
<td>

kafka.controller:type=KafkaController,name=ReplicasIneligibleToDeleteCount
</td>  
<td>


</td> </tr>  
<tr>  
<td>

\# of under replicated partitions (|ISR| < |all replicas|)
</td>  
<td>

kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

\# of under minIsr partitions (|ISR| < min.insync.replicas)
</td>  
<td>

kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

\# of at minIsr partitions (|ISR| = min.insync.replicas)
</td>  
<td>

kafka.server:type=ReplicaManager,name=AtMinIsrPartitionCount
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Producer Id counts
</td>  
<td>

kafka.server:type=ReplicaManager,name=ProducerIdCount
</td>  
<td>

Count of all producer ids created by transactional and idempotent producers in each replica on the broker
</td> </tr>  
<tr>  
<td>

Partition counts
</td>  
<td>

kafka.server:type=ReplicaManager,name=PartitionCount
</td>  
<td>

mostly even across brokers
</td> </tr>  
<tr>  
<td>

Offline Replica counts
</td>  
<td>

kafka.server:type=ReplicaManager,name=OfflineReplicaCount
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Leader replica counts
</td>  
<td>

kafka.server:type=ReplicaManager,name=LeaderCount
</td>  
<td>

mostly even across brokers
</td> </tr>  
<tr>  
<td>

ISR shrink rate
</td>  
<td>

kafka.server:type=ReplicaManager,name=IsrShrinksPerSec
</td>  
<td>

If a broker goes down, ISR for some of the partitions will shrink. When that broker is up again, ISR will be expanded once the replicas are fully caught up. Other than that, the expected value for both ISR shrink rate and expansion rate is 0.
</td> </tr>  
<tr>  
<td>

ISR expansion rate
</td>  
<td>

kafka.server:type=ReplicaManager,name=IsrExpandsPerSec
</td>  
<td>

See above
</td> </tr>  
<tr>  
<td>

Failed ISR update rate
</td>  
<td>

kafka.server:type=ReplicaManager,name=FailedIsrUpdatesPerSec
</td>  
<td>

0
</td> </tr>  
<tr>  
<td>

Max lag in messages btw follower and leader replicas
</td>  
<td>

kafka.server:type=ReplicaFetcherManager,name=MaxLag,clientId=Replica
</td>  
<td>

lag should be proportional to the maximum batch size of a produce request.
</td> </tr>  
<tr>  
<td>

Lag in messages per follower replica
</td>  
<td>

kafka.server:type=FetcherLagMetrics,name=ConsumerLag,clientId=([-.\w]+),topic=([-.\w]+),partition=([0-9]+)
</td>  
<td>

lag should be proportional to the maximum batch size of a produce request.
</td> </tr>  
<tr>  
<td>

Requests waiting in the producer purgatory
</td>  
<td>

kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Produce
</td>  
<td>

non-zero if ack=-1 is used
</td> </tr>  
<tr>  
<td>

Requests waiting in the fetch purgatory
</td>  
<td>

kafka.server:type=DelayedOperationPurgatory,name=PurgatorySize,delayedOperation=Fetch
</td>  
<td>

size depends on fetch.wait.max.ms in the consumer
</td> </tr>  
<tr>  
<td>

Request total time
</td>  
<td>

kafka.network:type=RequestMetrics,name=TotalTimeMs,request={Produce|FetchConsumer|FetchFollower}
</td>  
<td>

broken into queue, local, remote and response send time
</td> </tr>  
<tr>  
<td>

Time the request waits in the request queue
</td>  
<td>

kafka.network:type=RequestMetrics,name=RequestQueueTimeMs,request={Produce|FetchConsumer|FetchFollower}
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Time the request is processed at the leader
</td>  
<td>

kafka.network:type=RequestMetrics,name=LocalTimeMs,request={Produce|FetchConsumer|FetchFollower}
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Time the request waits for the follower
</td>  
<td>

kafka.network:type=RequestMetrics,name=RemoteTimeMs,request={Produce|FetchConsumer|FetchFollower}
</td>  
<td>

non-zero for produce requests when ack=-1
</td> </tr>  
<tr>  
<td>

Time the request waits in the response queue
</td>  
<td>

kafka.network:type=RequestMetrics,name=ResponseQueueTimeMs,request={Produce|FetchConsumer|FetchFollower}
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Time to send the response
</td>  
<td>

kafka.network:type=RequestMetrics,name=ResponseSendTimeMs,request={Produce|FetchConsumer|FetchFollower}
</td>  
<td>


</td> </tr>  
<tr>  
<td>

Number of messages the consumer lags behind the producer by. Published by the consumer, not broker.
</td>  
<td>

kafka.consumer:type=consumer-fetch-manager-metrics,client-id={client-id} Attribute: records-lag-max
</td>  
<td>


</td> </tr>  
<tr>  
<td>

The average fraction of time the network processors are idle
</td>  
<td>

kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent
</td>  
<td>

between 0 and 1, ideally > 0.3
</td> </tr>  
<tr>  
<td>

The number of connections disconnected on a processor due to a client not re-authenticating and then using the connection beyond its expiration time for anything other than re-authentication
</td>  
<td>

kafka.server:type=socket-server-metrics,listener=[SASL_PLAINTEXT|SASL_SSL],networkProcessor=<#>,name=expired-connections-killed-count
</td>  
<td>

ideally 0 when re-authentication is enabled, implying there are no longer any older, pre-2.2.0 clients connecting to this (listener, processor) combination
</td> </tr>  
<tr>  
<td>

The total number of connections disconnected, across all processors, due to a client not re-authenticating and then using the connection beyond its expiration time for anything other than re-authentication
</td>  
<td>

kafka.network:type=SocketServer,name=ExpiredConnectionsKilledCount
</td>  
<td>

ideally 0 when re-authentication is enabled, implying there are no longer any older, pre-2.2.0 clients connecting to this broker
</td> </tr>  
<tr>  
<td>

The average fraction of time the request handler threads are idle
</td>  
<td>

kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent
</td>  
<td>

between 0 and 1, ideally > 0.3
</td> </tr>  
<tr>  
<td>

Bandwidth quota metrics per (user, client-id), user or client-id
</td>  
<td>

kafka.server:type={Produce|Fetch},user=([-.\w]+),client-id=([-.\w]+)
</td>  
<td>

Two attributes. throttle-time indicates the amount of time in ms the client was throttled. Ideally = 0. byte-rate indicates the data produce/consume rate of the client in bytes/sec. For (user, client-id) quotas, both user and client-id are specified. If per-client-id quota is applied to the client, user is not specified. If per-user quota is applied, client-id is not specified.
</td> </tr>  
<tr>  
<td>

Request quota metrics per (user, client-id), user or client-id
</td>  
<td>

kafka.server:type=Request,user=([-.\w]+),client-id=([-.\w]+)
</td>  
<td>

Two attributes. throttle-time indicates the amount of time in ms the client was throttled. Ideally = 0. request-time indicates the percentage of time spent in broker network and I/O threads to process requests from client group. For (user, client-id) quotas, both user and client-id are specified. If per-client-id quota is applied to the client, user is not specified. If per-user quota is applied, client-id is not specified.
</td> </tr>  
<tr>  
<td>

Requests exempt from throttling
</td>  
<td>

kafka.server:type=Request
</td>  
<td>

exempt-throttle-time indicates the percentage of time spent in broker network and I/O threads to process requests that are exempt from throttling.
</td> </tr>  
<tr>  
<td>

ZooKeeper client request latency
</td>  
<td>

kafka.server:type=ZooKeeperClientMetrics,name=ZooKeeperRequestLatencyMs
</td>  
<td>

Latency in milliseconds for ZooKeeper requests from broker.
</td> </tr>  
<tr>  
<td>

ZooKeeper connection status
</td>  
<td>

kafka.server:type=SessionExpireListener,name=SessionState
</td>  
<td>

Connection status of broker's ZooKeeper session which may be one of Disconnected|SyncConnected|AuthFailed|ConnectedReadOnly|SaslAuthenticated|Expired.
</td> </tr>  
<tr>  
<td>

Max time to load group metadata
</td>  
<td>

kafka.server:type=group-coordinator-metrics,name=partition-load-time-max
</td>  
<td>

maximum time, in milliseconds, it took to load offsets and group metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)
</td> </tr>  
<tr>  
<td>

Avg time to load group metadata
</td>  
<td>

kafka.server:type=group-coordinator-metrics,name=partition-load-time-avg
</td>  
<td>

average time, in milliseconds, it took to load offsets and group metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)
</td> </tr>  
<tr>  
<td>

Max time to load transaction metadata
</td>  
<td>

kafka.server:type=transaction-coordinator-metrics,name=partition-load-time-max
</td>  
<td>

maximum time, in milliseconds, it took to load transaction metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)
</td> </tr>  
<tr>  
<td>

Avg time to load transaction metadata
</td>  
<td>

kafka.server:type=transaction-coordinator-metrics,name=partition-load-time-avg
</td>  
<td>

average time, in milliseconds, it took to load transaction metadata from the consumer offset partitions loaded in the last 30 seconds (including time spent waiting for the loading task to be scheduled)
</td> </tr>  
<tr>  
<td>

Rate of transactional verification errors
</td>  
<td>

kafka.server:type=AddPartitionsToTxnManager,name=VerificationFailureRate
</td>  
<td>

Rate of verifications that returned in failure either from the AddPartitionsToTxn API response or through errors in the AddPartitionsToTxnManager. In steady state 0, but transient errors are expected during rolls and reassignments of the transactional state partition.
</td> </tr>  
<tr>  
<td>

Time to verify a transactional request
</td>  
<td>

kafka.server:type=AddPartitionsToTxnManager,name=VerificationTimeMs
</td>  
<td>

The amount of time queueing while a possible previous request is in-flight plus the round trip to the transaction coordinator to verify (or not verify)
</td> </tr>  
<tr>  
<td>

Consumer Group Offset Count
</td>  
<td>

kafka.server:type=GroupMetadataManager,name=NumOffsets
</td>  
<td>

Total number of committed offsets for Consumer Groups
</td> </tr>  
<tr>  
<td>

Consumer Group Count
</td>  
<td>

kafka.server:type=GroupMetadataManager,name=NumGroups
</td>  
<td>

Total number of Consumer Groups
</td> </tr>  
<tr>  
<td>

Consumer Group Count, per State
</td>  
<td>

kafka.server:type=GroupMetadataManager,name=NumGroups[PreparingRebalance,CompletingRebalance,Empty,Stable,Dead]
</td>  
<td>

The number of Consumer Groups in each state: PreparingRebalance, CompletingRebalance, Empty, Stable, Dead
</td> </tr>  
<tr>  
<td>

Number of reassigning partitions
</td>  
<td>

kafka.server:type=ReplicaManager,name=ReassigningPartitions
</td>  
<td>

The number of reassigning leader partitions on a broker.
</td> </tr>  
<tr>  
<td>

Outgoing byte rate of reassignment traffic
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=ReassignmentBytesOutPerSec
</td>  
<td>

0; non-zero when a partition reassignment is in progress.
</td> </tr>  
<tr>  
<td>

Incoming byte rate of reassignment traffic
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=ReassignmentBytesInPerSec
</td>  
<td>

0; non-zero when a partition reassignment is in progress.
</td> </tr>  
<tr>  
<td>

Size of a partition on disk (in bytes)
</td>  
<td>

kafka.log:type=Log,name=Size,topic=([-.\w]+),partition=([0-9]+)
</td>  
<td>

The size of a partition on disk, measured in bytes.
</td> </tr>  
<tr>  
<td>

Number of log segments in a partition
</td>  
<td>

kafka.log:type=Log,name=NumLogSegments,topic=([-.\w]+),partition=([0-9]+)
</td>  
<td>

The number of log segments in a partition.
</td> </tr>  
<tr>  
<td>

First offset in a partition
</td>  
<td>

kafka.log:type=Log,name=LogStartOffset,topic=([-.\w]+),partition=([0-9]+)
</td>  
<td>

The first offset in a partition.
</td> </tr>  
<tr>  
<td>

Last offset in a partition
</td>  
<td>

kafka.log:type=Log,name=LogEndOffset,topic=([-.\w]+),partition=([0-9]+)
</td>  
<td>

The last offset in a partition.
</td> </tr> </table>

## Tiered Storage Monitoring

The following set of metrics are available for monitoring of the tiered storage feature:  
  
  
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

Remote Fetch Bytes Per Sec
</td>  
<td>

Rate of bytes read from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteFetchBytesPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Fetch Requests Per Sec
</td>  
<td>

Rate of read requests from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteFetchRequestsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Fetch Errors Per Sec
</td>  
<td>

Rate of read errors from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteFetchErrorsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Copy Bytes Per Sec
</td>  
<td>

Rate of bytes copied to remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteCopyBytesPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Copy Requests Per Sec
</td>  
<td>

Rate of write requests to remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteCopyRequestsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Copy Errors Per Sec
</td>  
<td>

Rate of write errors from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteCopyErrorsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Copy Lag Bytes
</td>  
<td>

Bytes which are eligible for tiering, but are not in remote storage yet. Omitting 'topic=(...)' will yield the all-topic sum
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteCopyLagBytes,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Copy Lag Segments
</td>  
<td>

Segments which are eligible for tiering, but are not in remote storage yet. Omitting 'topic=(...)' will yield the all-topic count
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteCopyLagSegments,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Delete Requests Per Sec
</td>  
<td>

Rate of delete requests to remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteDeleteRequestsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Delete Errors Per Sec
</td>  
<td>

Rate of delete errors from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteDeleteErrorsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Delete Lag Bytes
</td>  
<td>

Tiered bytes which are eligible for deletion, but have not been deleted yet. Omitting 'topic=(...)' will yield the all-topic sum
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteDeleteLagBytes,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Delete Lag Segments
</td>  
<td>

Tiered segments which are eligible for deletion, but have not been deleted yet. Omitting 'topic=(...)' will yield the all-topic count
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteDeleteLagSegments,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Build Remote Log Aux State Requests Per Sec
</td>  
<td>

Rate of requests for rebuilding the auxiliary state from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=BuildRemoteLogAuxStateRequestsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Build Remote Log Aux State Errors Per Sec
</td>  
<td>

Rate of errors for rebuilding the auxiliary state from remote storage per topic. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=BuildRemoteLogAuxStateErrorsPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Log Size Computation Time
</td>  
<td>

The amount of time needed to compute the size of the remote log. Omitting 'topic=(...)' will yield the all-topic time
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteLogSizeComputationTime,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Log Size Bytes
</td>  
<td>

The total size of a remote log in bytes. Omitting 'topic=(...)' will yield the all-topic sum
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteLogSizeBytes,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Remote Log Metadata Count
</td>  
<td>

The total number of metadata entries for remote storage. Omitting 'topic=(...)' will yield the all-topic count
</td>  
<td>

kafka.server:type=BrokerTopicMetrics,name=RemoteLogMetadataCount,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

Delayed Remote Fetch Expires Per Sec
</td>  
<td>

The number of expired remote fetches per second. Omitting 'topic=(...)' will yield the all-topic rate
</td>  
<td>

kafka.server:type=DelayedRemoteFetchMetrics,name=ExpiresPerSec,topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

RemoteLogReader Task Queue Size
</td>  
<td>

Size of the queue holding remote storage read tasks
</td>  
<td>

org.apache.kafka.storage.internals.log:type=RemoteStorageThreadPool,name=RemoteLogReaderTaskQueueSize
</td> </tr>  
<tr>  
<td>

RemoteLogReader Avg Idle Percent
</td>  
<td>

Average idle percent of thread pool for processing remote storage read tasks
</td>  
<td>

org.apache.kafka.storage.internals.log:type=RemoteStorageThreadPool,name=RemoteLogReaderAvgIdlePercent
</td> </tr>  
<tr>  
<td>

RemoteLogManager Tasks Avg Idle Percent
</td>  
<td>

Average idle percent of thread pool for copying data to remote storage
</td>  
<td>

kafka.log.remote:type=RemoteLogManager,name=RemoteLogManagerTasksAvgIdlePercent
</td> </tr> </table>

## KRaft Monitoring Metrics

The set of metrics that allow monitoring of the KRaft quorum and the metadata log.  
Note that some exposed metrics depend on the role of the node as defined by `process.roles`

### KRaft Quorum Monitoring Metrics

These metrics are reported on both Controllers and Brokers in a KRaft Cluster   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

Current State
</td>  
<td>

The current state of this member; possible values are leader, candidate, voted, follower, unattached, observer.
</td>  
<td>

kafka.server:type=raft-metrics,name=current-state
</td> </tr>  
<tr>  
<td>

Current Leader
</td>  
<td>

The current quorum leader's id; -1 indicates unknown.
</td>  
<td>

kafka.server:type=raft-metrics,name=current-leader
</td> </tr>  
<tr>  
<td>

Current Voted
</td>  
<td>

The current voted leader's id; -1 indicates not voted for anyone.
</td>  
<td>

kafka.server:type=raft-metrics,name=current-vote
</td> </tr>  
<tr>  
<td>

Current Epoch
</td>  
<td>

The current quorum epoch.
</td>  
<td>

kafka.server:type=raft-metrics,name=current-epoch
</td> </tr>  
<tr>  
<td>

High Watermark
</td>  
<td>

The high watermark maintained on this member; -1 if it is unknown.
</td>  
<td>

kafka.server:type=raft-metrics,name=high-watermark
</td> </tr>  
<tr>  
<td>

Log End Offset
</td>  
<td>

The current raft log end offset.
</td>  
<td>

kafka.server:type=raft-metrics,name=log-end-offset
</td> </tr>  
<tr>  
<td>

Number of Unknown Voter Connections
</td>  
<td>

Number of unknown voters whose connection information is not cached. This value of this metric is always 0.
</td>  
<td>

kafka.server:type=raft-metrics,name=number-unknown-voter-connections
</td> </tr>  
<tr>  
<td>

Average Commit Latency
</td>  
<td>

The average time in milliseconds to commit an entry in the raft log.
</td>  
<td>

kafka.server:type=raft-metrics,name=commit-latency-avg
</td> </tr>  
<tr>  
<td>

Maximum Commit Latency
</td>  
<td>

The maximum time in milliseconds to commit an entry in the raft log.
</td>  
<td>

kafka.server:type=raft-metrics,name=commit-latency-max
</td> </tr>  
<tr>  
<td>

Average Election Latency
</td>  
<td>

The average time in milliseconds spent on electing a new leader.
</td>  
<td>

kafka.server:type=raft-metrics,name=election-latency-avg
</td> </tr>  
<tr>  
<td>

Maximum Election Latency
</td>  
<td>

The maximum time in milliseconds spent on electing a new leader.
</td>  
<td>

kafka.server:type=raft-metrics,name=election-latency-max
</td> </tr>  
<tr>  
<td>

Fetch Records Rate
</td>  
<td>

The average number of records fetched from the leader of the raft quorum.
</td>  
<td>

kafka.server:type=raft-metrics,name=fetch-records-rate
</td> </tr>  
<tr>  
<td>

Append Records Rate
</td>  
<td>

The average number of records appended per sec by the leader of the raft quorum.
</td>  
<td>

kafka.server:type=raft-metrics,name=append-records-rate
</td> </tr>  
<tr>  
<td>

Average Poll Idle Ratio
</td>  
<td>

The average fraction of time the client's poll() is idle as opposed to waiting for the user code to process records.
</td>  
<td>

kafka.server:type=raft-metrics,name=poll-idle-ratio-avg
</td> </tr>  
<tr>  
<td>

Current Metadata Version
</td>  
<td>

Outputs the feature level of the current effective metadata version.
</td>  
<td>

kafka.server:type=MetadataLoader,name=CurrentMetadataVersion
</td> </tr>  
<tr>  
<td>

Metadata Snapshot Load Count
</td>  
<td>

The total number of times we have loaded a KRaft snapshot since the process was started.
</td>  
<td>

kafka.server:type=MetadataLoader,name=HandleLoadSnapshotCount
</td> </tr>  
<tr>  
<td>

Latest Metadata Snapshot Size
</td>  
<td>

The total size in bytes of the latest snapshot that the node has generated. If none have been generated yet, this is the size of the latest snapshot that was loaded. If no snapshots have been generated or loaded, this is 0.
</td>  
<td>

kafka.server:type=SnapshotEmitter,name=LatestSnapshotGeneratedBytes
</td> </tr>  
<tr>  
<td>

Latest Metadata Snapshot Age
</td>  
<td>

The interval in milliseconds since the latest snapshot that the node has generated. If none have been generated yet, this is approximately the time delta since the process was started.
</td>  
<td>

kafka.server:type=SnapshotEmitter,name=LatestSnapshotGeneratedAgeMs
</td> </tr> </table>

### KRaft Controller Monitoring Metrics  
  
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

Active Controller Count
</td>  
<td>

The number of Active Controllers on this node. Valid values are '0' or '1'.
</td>  
<td>

kafka.controller:type=KafkaController,name=ActiveControllerCount
</td> </tr>  
<tr>  
<td>

Event Queue Time Ms
</td>  
<td>

A Histogram of the time in milliseconds that requests spent waiting in the Controller Event Queue.
</td>  
<td>

kafka.controller:type=ControllerEventManager,name=EventQueueTimeMs
</td> </tr>  
<tr>  
<td>

Event Queue Processing Time Ms
</td>  
<td>

A Histogram of the time in milliseconds that requests spent being processed in the Controller Event Queue.
</td>  
<td>

kafka.controller:type=ControllerEventManager,name=EventQueueProcessingTimeMs
</td> </tr>  
<tr>  
<td>

Fenced Broker Count
</td>  
<td>

The number of fenced brokers as observed by this Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=FencedBrokerCount
</td> </tr>  
<tr>  
<td>

Active Broker Count
</td>  
<td>

The number of active brokers as observed by this Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=ActiveBrokerCount
</td> </tr>  
<tr>  
<td>

Migrating ZK Broker Count
</td>  
<td>

The number of brokers registered with the Controller that haven't yet migrated to KRaft mode.
</td>  
<td>

kafka.controller:type=KafkaController,name=MigratingZkBrokerCount
</td> </tr>  
<tr>  
<td>

ZK Migrating State
</td>  
<td>



  * 0 - NONE, cluster created in KRaft mode;
  * 4 - ZK, Migration has not started, controller is a ZK controller;
  * 2 - PRE_MIGRATION, the KRaft Controller is waiting for all ZK brokers to register in migration mode;
  * 1 - MIGRATION, ZK metadata has been migrated, but some broker is still running in ZK mode;
  * 3 - POST_MIGRATION, the cluster migration is complete;


</td>  
<td>

kafka.controller:type=KafkaController,name=ZkMigrationState
</td> </tr>  
<tr>  
<td>

Global Topic Count
</td>  
<td>

The number of global topics as observed by this Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=GlobalTopicCount
</td> </tr>  
<tr>  
<td>

Global Partition Count
</td>  
<td>

The number of global partitions as observed by this Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=GlobalPartitionCount
</td> </tr>  
<tr>  
<td>

Offline Partition Count
</td>  
<td>

The number of offline topic partitions (non-internal) as observed by this Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=OfflinePartitionsCount
</td> </tr>  
<tr>  
<td>

Preferred Replica Imbalance Count
</td>  
<td>

The count of topic partitions for which the leader is not the preferred leader.
</td>  
<td>

kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount
</td> </tr>  
<tr>  
<td>

Metadata Error Count
</td>  
<td>

The number of times this controller node has encountered an error during metadata log processing.
</td>  
<td>

kafka.controller:type=KafkaController,name=MetadataErrorCount
</td> </tr>  
<tr>  
<td>

Last Applied Record Offset
</td>  
<td>

The offset of the last record from the cluster metadata partition that was applied by the Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=LastAppliedRecordOffset
</td> </tr>  
<tr>  
<td>

Last Committed Record Offset
</td>  
<td>

The offset of the last record committed to this Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=LastCommittedRecordOffset
</td> </tr>  
<tr>  
<td>

Last Applied Record Timestamp
</td>  
<td>

The timestamp of the last record from the cluster metadata partition that was applied by the Controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=LastAppliedRecordTimestamp
</td> </tr>  
<tr>  
<td>

Last Applied Record Lag Ms
</td>  
<td>

The difference between now and the timestamp of the last record from the cluster metadata partition that was applied by the controller. For active Controllers the value of this lag is always zero.
</td>  
<td>

kafka.controller:type=KafkaController,name=LastAppliedRecordLagMs
</td> </tr>  
<tr>  
<td>

ZooKeeper Write Behind Lag
</td>  
<td>

The amount of lag in records that ZooKeeper is behind relative to the highest committed record in the metadata log. This metric will only be reported by the active KRaft controller.
</td>  
<td>

kafka.controller:type=KafkaController,name=ZkWriteBehindLag
</td> </tr>  
<tr>  
<td>

ZooKeeper Metadata Snapshot Write Time
</td>  
<td>

The number of milliseconds the KRaft controller took reconciling a snapshot into ZooKeeper.
</td>  
<td>

kafka.controller:type=KafkaController,name=ZkWriteSnapshotTimeMs
</td> </tr>  
<tr>  
<td>

ZooKeeper Metadata Delta Write Time
</td>  
<td>

The number of milliseconds the KRaft controller took writing a delta into ZK.
</td>  
<td>

kafka.controller:type=KafkaController,name=ZkWriteDeltaTimeMs
</td> </tr>  
<tr>  
<td>

Timed-out Broker Heartbeat Count
</td>  
<td>

The number of broker heartbeats that timed out on this controller since the process was started. Note that only active controllers handle heartbeats, so only they will see increases in this metric.
</td>  
<td>

kafka.controller:type=KafkaController,name=TimedOutBrokerHeartbeatCount
</td> </tr>  
<tr>  
<td>

Number Of Operations Started In Event Queue
</td>  
<td>

The total number of controller event queue operations that were started. This includes deferred operations.
</td>  
<td>

kafka.controller:type=KafkaController,name=EventQueueOperationsStartedCount
</td> </tr>  
<tr>  
<td>

Number of Operations Timed Out In Event Queue
</td>  
<td>

The total number of controller event queue operations that timed out before they could be performed.
</td>  
<td>

kafka.controller:type=KafkaController,name=EventQueueOperationsTimedOutCount
</td> </tr>  
<tr>  
<td>

Number Of New Controller Elections
</td>  
<td>

Counts the number of times this node has seen a new controller elected. A transition to the "no leader" state is not counted here. If the same controller as before becomes active, that still counts.
</td>  
<td>

kafka.controller:type=KafkaController,name=NewActiveControllersCount
</td> </tr> </table>

### KRaft Broker Monitoring Metrics  
  
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

Last Applied Record Offset
</td>  
<td>

The offset of the last record from the cluster metadata partition that was applied by the broker
</td>  
<td>

kafka.server:type=broker-metadata-metrics,name=last-applied-record-offset
</td> </tr>  
<tr>  
<td>

Last Applied Record Timestamp
</td>  
<td>

The timestamp of the last record from the cluster metadata partition that was applied by the broker.
</td>  
<td>

kafka.server:type=broker-metadata-metrics,name=last-applied-record-timestamp
</td> </tr>  
<tr>  
<td>

Last Applied Record Lag Ms
</td>  
<td>

The difference between now and the timestamp of the last record from the cluster metadata partition that was applied by the broker
</td>  
<td>

kafka.server:type=broker-metadata-metrics,name=last-applied-record-lag-ms
</td> </tr>  
<tr>  
<td>

Metadata Load Error Count
</td>  
<td>

The number of errors encountered by the BrokerMetadataListener while loading the metadata log and generating a new MetadataDelta based on it.
</td>  
<td>

kafka.server:type=broker-metadata-metrics,name=metadata-load-error-count
</td> </tr>  
<tr>  
<td>

Metadata Apply Error Count
</td>  
<td>

The number of errors encountered by the BrokerMetadataPublisher while applying a new MetadataImage based on the latest MetadataDelta.
</td>  
<td>

kafka.server:type=broker-metadata-metrics,name=metadata-apply-error-count
</td> </tr> </table>

## Common monitoring metrics for producer/consumer/connect/streams

The following metrics are available on producer/consumer/connector/streams instances. For specific metrics, please see following sections.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

connection-close-rate
</td>  
<td>

Connections closed per second in the window.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

connection-close-total
</td>  
<td>

Total connections closed in the window.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

connection-creation-rate
</td>  
<td>

New connections established per second in the window.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

connection-creation-total
</td>  
<td>

Total new connections established in the window.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

network-io-rate
</td>  
<td>

The average number of network operations (reads or writes) on all connections per second.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

network-io-total
</td>  
<td>

The total number of network operations (reads or writes) on all connections.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

outgoing-byte-rate
</td>  
<td>

The average number of outgoing bytes sent per second to all servers.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

outgoing-byte-total
</td>  
<td>

The total number of outgoing bytes sent to all servers.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

request-rate
</td>  
<td>

The average number of requests sent per second.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

request-total
</td>  
<td>

The total number of requests sent.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

request-size-avg
</td>  
<td>

The average size of all requests in the window.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

request-size-max
</td>  
<td>

The maximum size of any request sent in the window.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

incoming-byte-rate
</td>  
<td>

Bytes/second read off all sockets.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

incoming-byte-total
</td>  
<td>

Total bytes read off all sockets.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

response-rate
</td>  
<td>

Responses received per second.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

response-total
</td>  
<td>

Total responses received.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

select-rate
</td>  
<td>

Number of times the I/O layer checked for new I/O to perform per second.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

select-total
</td>  
<td>

Total number of times the I/O layer checked for new I/O to perform.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-wait-time-ns-avg
</td>  
<td>

The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-wait-time-ns-total
</td>  
<td>

The total time the I/O thread spent waiting in nanoseconds.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-waittime-total
</td>  
<td>

***Deprecated*** The total time the I/O thread spent waiting in nanoseconds. Replacement is `io-wait-time-ns-total`.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-wait-ratio
</td>  
<td>

The fraction of time the I/O thread spent waiting.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-time-ns-avg
</td>  
<td>

The average length of time for I/O per select call in nanoseconds.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-time-ns-total
</td>  
<td>

The total time the I/O thread spent doing I/O in nanoseconds.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

iotime-total
</td>  
<td>

***Deprecated*** The total time the I/O thread spent doing I/O in nanoseconds. Replacement is `io-time-ns-total`.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

io-ratio
</td>  
<td>

The fraction of time the I/O thread spent doing I/O.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

connection-count
</td>  
<td>

The current number of active connections.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

successful-authentication-rate
</td>  
<td>

Connections per second that were successfully authenticated using SASL or SSL.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

successful-authentication-total
</td>  
<td>

Total connections that were successfully authenticated using SASL or SSL.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-authentication-rate
</td>  
<td>

Connections per second that failed authentication.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-authentication-total
</td>  
<td>

Total connections that failed authentication.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

successful-reauthentication-rate
</td>  
<td>

Connections per second that were successfully re-authenticated using SASL.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

successful-reauthentication-total
</td>  
<td>

Total connections that were successfully re-authenticated using SASL.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

reauthentication-latency-max
</td>  
<td>

The maximum latency in ms observed due to re-authentication.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

reauthentication-latency-avg
</td>  
<td>

The average latency in ms observed due to re-authentication.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-reauthentication-rate
</td>  
<td>

Connections per second that failed re-authentication.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-reauthentication-total
</td>  
<td>

Total connections that failed re-authentication.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

successful-authentication-no-reauth-total
</td>  
<td>

Total connections that were successfully authenticated by older, pre-2.2.0 SASL clients that do not support re-authentication. May only be non-zero.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[producer|consumer|connect]-metrics,client-id=([-.\w]+)
</td> </tr> </table>

## Common Per-broker metrics for producer/consumer/connect/streams

The following metrics are available on producer/consumer/connector/streams instances. For specific metrics, please see following sections.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

outgoing-byte-rate
</td>  
<td>

The average number of outgoing bytes sent per second for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

outgoing-byte-total
</td>  
<td>

The total number of outgoing bytes sent for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

request-rate
</td>  
<td>

The average number of requests sent per second for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

request-total
</td>  
<td>

The total number of requests sent for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

request-size-avg
</td>  
<td>

The average size of all requests in the window for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

request-size-max
</td>  
<td>

The maximum size of any request sent in the window for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

incoming-byte-rate
</td>  
<td>

The average number of bytes received per second for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

incoming-byte-total
</td>  
<td>

The total number of bytes received for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

request-latency-avg
</td>  
<td>

The average request latency in ms for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

request-latency-max
</td>  
<td>

The maximum request latency in ms for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

response-rate
</td>  
<td>

Responses received per second for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr>  
<tr>  
<td>

response-total
</td>  
<td>

Total responses received for a node.
</td>  
<td>

kafka.[producer|consumer|connect]:type=[consumer|producer|connect]-node-metrics,client-id=([-.\w]+),node-id=([0-9]+)
</td> </tr> </table>

## Producer monitoring

The following metrics are available on producer instances.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

waiting-threads
</td>  
<td>

The number of user threads blocked waiting for buffer memory to enqueue their records.
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

buffer-total-bytes
</td>  
<td>

The maximum amount of buffer memory the client can use (whether or not it is currently used).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

buffer-available-bytes
</td>  
<td>

The total amount of buffer memory that is not being used (either unallocated or in the free list).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bufferpool-wait-time
</td>  
<td>

The fraction of time an appender waits for space allocation.
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bufferpool-wait-time-total
</td>  
<td>

***Deprecated*** The total time an appender waits for space allocation in nanoseconds. Replacement is `bufferpool-wait-time-ns-total`
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bufferpool-wait-time-ns-total
</td>  
<td>

The total time an appender waits for space allocation in nanoseconds.
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

flush-time-ns-total
</td>  
<td>

The total time the Producer spent in Producer.flush in nanoseconds.
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

txn-init-time-ns-total
</td>  
<td>

The total time the Producer spent initializing transactions in nanoseconds (for EOS).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

txn-begin-time-ns-total
</td>  
<td>

The total time the Producer spent in beginTransaction in nanoseconds (for EOS).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

txn-send-offsets-time-ns-total
</td>  
<td>

The total time the Producer spent sending offsets to transactions in nanoseconds (for EOS).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

txn-commit-time-ns-total
</td>  
<td>

The total time the Producer spent committing transactions in nanoseconds (for EOS).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

txn-abort-time-ns-total
</td>  
<td>

The total time the Producer spent aborting transactions in nanoseconds (for EOS).
</td>  
<td>

kafka.producer:type=producer-metrics,client-id=([-.\w]+)
</td> </tr> </table>

### Producer Sender Metrics

{{< include-html file="/static/37/generated/producer_metrics.html" >}} 

## Consumer monitoring

The following metrics are available on consumer instances.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

time-between-poll-avg
</td>  
<td>

The average delay between invocations of poll().
</td>  
<td>

kafka.consumer:type=consumer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

time-between-poll-max
</td>  
<td>

The max delay between invocations of poll().
</td>  
<td>

kafka.consumer:type=consumer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

last-poll-seconds-ago
</td>  
<td>

The number of seconds since the last poll() invocation.
</td>  
<td>

kafka.consumer:type=consumer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

poll-idle-ratio-avg
</td>  
<td>

The average fraction of time the consumer's poll() is idle as opposed to waiting for the user code to process records.
</td>  
<td>

kafka.consumer:type=consumer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

committed-time-ns-total
</td>  
<td>

The total time the Consumer spent in committed in nanoseconds.
</td>  
<td>

kafka.consumer:type=consumer-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-sync-time-ns-total
</td>  
<td>

The total time the Consumer spent committing offsets in nanoseconds (for AOS).
</td>  
<td>

kafka.consumer:type=consumer-metrics,client-id=([-.\w]+)
</td> </tr> </table>

### Consumer Group Metrics  
  
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

commit-latency-avg
</td>  
<td>

The average time taken for a commit request
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-latency-max
</td>  
<td>

The max time taken for a commit request
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-rate
</td>  
<td>

The number of commit calls per second
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-total
</td>  
<td>

The total number of commit calls
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

assigned-partitions
</td>  
<td>

The number of partitions currently assigned to this consumer
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

heartbeat-response-time-max
</td>  
<td>

The max time taken to receive a response to a heartbeat request
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

heartbeat-rate
</td>  
<td>

The average number of heartbeats per second
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

heartbeat-total
</td>  
<td>

The total number of heartbeats
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

join-time-avg
</td>  
<td>

The average time taken for a group rejoin
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

join-time-max
</td>  
<td>

The max time taken for a group rejoin
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

join-rate
</td>  
<td>

The number of group joins per second
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

join-total
</td>  
<td>

The total number of group joins
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

sync-time-avg
</td>  
<td>

The average time taken for a group sync
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

sync-time-max
</td>  
<td>

The max time taken for a group sync
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

sync-rate
</td>  
<td>

The number of group syncs per second
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

sync-total
</td>  
<td>

The total number of group syncs
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

rebalance-latency-avg
</td>  
<td>

The average time taken for a group rebalance
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

rebalance-latency-max
</td>  
<td>

The max time taken for a group rebalance
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

rebalance-latency-total
</td>  
<td>

The total time taken for group rebalances so far
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

rebalance-total
</td>  
<td>

The total number of group rebalances participated
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

rebalance-rate-per-hour
</td>  
<td>

The number of group rebalance participated per hour
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-rebalance-total
</td>  
<td>

The total number of failed group rebalances
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-rebalance-rate-per-hour
</td>  
<td>

The number of failed group rebalance event per hour
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

last-rebalance-seconds-ago
</td>  
<td>

The number of seconds since the last rebalance event
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

last-heartbeat-seconds-ago
</td>  
<td>

The number of seconds since the last controller heartbeat
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

partitions-revoked-latency-avg
</td>  
<td>

The average time taken by the on-partitions-revoked rebalance listener callback
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

partitions-revoked-latency-max
</td>  
<td>

The max time taken by the on-partitions-revoked rebalance listener callback
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

partitions-assigned-latency-avg
</td>  
<td>

The average time taken by the on-partitions-assigned rebalance listener callback
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

partitions-assigned-latency-max
</td>  
<td>

The max time taken by the on-partitions-assigned rebalance listener callback
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

partitions-lost-latency-avg
</td>  
<td>

The average time taken by the on-partitions-lost rebalance listener callback
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

partitions-lost-latency-max
</td>  
<td>

The max time taken by the on-partitions-lost rebalance listener callback
</td>  
<td>

kafka.consumer:type=consumer-coordinator-metrics,client-id=([-.\w]+)
</td> </tr> </table>

### Consumer Fetch Metrics

{{< include-html file="/static/37/generated/consumer_metrics.html" >}} 

## Connect Monitoring

A Connect worker process contains all the producer and consumer metrics as well as metrics specific to Connect. The worker process itself has a number of metrics, while each connector and task have additional metrics. {{< include-html file="/static/37/generated/connect_metrics.html" >}} 

## Streams Monitoring

A Kafka Streams instance contains all the producer and consumer metrics as well as additional metrics specific to Streams. The metrics have three recording levels: `info`, `debug`, and `trace`. 

Note that the metrics have a 4-layer hierarchy. At the top level there are client-level metrics for each started Kafka Streams client. Each client has stream threads, with their own metrics. Each stream thread has tasks, with their own metrics. Each task has a number of processor nodes, with their own metrics. Each task also has a number of state stores and record caches, all with their own metrics. 

Use the following configuration option to specify which metrics you want collected: 
    
    
    metrics.recording.level="info"

### Client Metrics

All of the following metrics have a recording level of `info`:   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

version
</td>  
<td>

The version of the Kafka Streams client.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-id
</td>  
<td>

The version control commit ID of the Kafka Streams client.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

application-id
</td>  
<td>

The application ID of the Kafka Streams client.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

topology-description
</td>  
<td>

The description of the topology executed in the Kafka Streams client.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

state
</td>  
<td>

The state of the Kafka Streams client.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

alive-stream-threads
</td>  
<td>

The current number of alive stream threads that are running or participating in rebalance.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

failed-stream-threads
</td>  
<td>

The number of failed stream threads since the start of the Kafka Streams client.
</td>  
<td>

kafka.streams:type=stream-metrics,client-id=([-.\w]+)
</td> </tr> </table>

### Thread Metrics

All of the following metrics have a recording level of `info`:   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

commit-latency-avg
</td>  
<td>

The average execution time in ms, for committing, across all running tasks of this thread.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-latency-max
</td>  
<td>

The maximum execution time in ms, for committing, across all running tasks of this thread.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

poll-latency-avg
</td>  
<td>

The average execution time in ms, for consumer polling.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

poll-latency-max
</td>  
<td>

The maximum execution time in ms, for consumer polling.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-latency-avg
</td>  
<td>

The average execution time in ms, for processing.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-latency-max
</td>  
<td>

The maximum execution time in ms, for processing.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

punctuate-latency-avg
</td>  
<td>

The average execution time in ms, for punctuating.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

punctuate-latency-max
</td>  
<td>

The maximum execution time in ms, for punctuating.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-rate
</td>  
<td>

The average number of commits per second.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

commit-total
</td>  
<td>

The total number of commit calls.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

poll-rate
</td>  
<td>

The average number of consumer poll calls per second.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

poll-total
</td>  
<td>

The total number of consumer poll calls.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-rate
</td>  
<td>

The average number of processed records per second.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-total
</td>  
<td>

The total number of processed records.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

punctuate-rate
</td>  
<td>

The average number of punctuate calls per second.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

punctuate-total
</td>  
<td>

The total number of punctuate calls.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

task-created-rate
</td>  
<td>

The average number of tasks created per second.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

task-created-total
</td>  
<td>

The total number of tasks created.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

task-closed-rate
</td>  
<td>

The average number of tasks closed per second.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

task-closed-total
</td>  
<td>

The total number of tasks closed.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

blocked-time-ns-total
</td>  
<td>

The total time the thread spent blocked on kafka.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

thread-start-time
</td>  
<td>

The time that the thread was started.
</td>  
<td>

kafka.streams:type=stream-thread-metrics,thread-id=([-.\w]+)
</td> </tr> </table>

### Task Metrics

All of the following metrics have a recording level of `debug`, except for the dropped-records-* and active-process-ratio metrics which have a recording level of `info`:   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

process-latency-avg
</td>  
<td>

The average execution time in ns, for processing.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-latency-max
</td>  
<td>

The maximum execution time in ns, for processing.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-rate
</td>  
<td>

The average number of processed records per second across all source processor nodes of this task.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-total
</td>  
<td>

The total number of processed records across all source processor nodes of this task.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-lateness-avg
</td>  
<td>

The average observed lateness of records (stream time - record timestamp).
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-lateness-max
</td>  
<td>

The max observed lateness of records (stream time - record timestamp).
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

enforced-processing-rate
</td>  
<td>

The average number of enforced processings per second.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

enforced-processing-total
</td>  
<td>

The total number enforced processings.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

dropped-records-rate
</td>  
<td>

The average number of records dropped within this task.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

dropped-records-total
</td>  
<td>

The total number of records dropped within this task.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

active-process-ratio
</td>  
<td>

The fraction of time the stream thread spent on processing this task among all assigned active tasks.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

input-buffer-bytes-total
</td>  
<td>

The total number of bytes accumulated by this task,
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

cache-size-bytes-total
</td>  
<td>

The cache size in bytes accumulated by this task.
</td>  
<td>

kafka.streams:type=stream-task-metrics,thread-id=([-.\w]+),task-id=([-.\w]+)
</td> </tr> </table>

### Processor Node Metrics

The following metrics are only available on certain types of nodes, i.e., the process-* metrics are only available for source processor nodes, the `suppression-emit-*` metrics are only available for suppression operation nodes, `emit-final-*` metrics are only available for windowed aggregations nodes, and the `record-e2e-latency-*` metrics are only available for source processor nodes and terminal nodes (nodes without successor nodes). All of the metrics have a recording level of `debug`, except for the `record-e2e-latency-*` metrics which have a recording level of `info`:   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

bytes-consumed-total
</td>  
<td>

The total number of bytes consumed by a source processor node.
</td>  
<td>

kafka.streams:type=stream-topic-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+),topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bytes-produced-total
</td>  
<td>

The total number of bytes produced by a sink processor node.
</td>  
<td>

kafka.streams:type=stream-topic-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+),topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-rate
</td>  
<td>

The average number of records processed by a source processor node per second.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

process-total
</td>  
<td>

The total number of records processed by a source processor node per second.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

suppression-emit-rate
</td>  
<td>

The rate at which records that have been emitted downstream from suppression operation nodes.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

suppression-emit-total
</td>  
<td>

The total number of records that have been emitted downstream from suppression operation nodes.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

emit-final-latency-max
</td>  
<td>

The max latency to emit final records when a record could be emitted.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

emit-final-latency-avg
</td>  
<td>

The avg latency to emit final records when a record could be emitted.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

emit-final-records-rate
</td>  
<td>

The rate of records emitted when records could be emitted.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

emit-final-records-total
</td>  
<td>

The total number of records emitted.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-e2e-latency-avg
</td>  
<td>

The average end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-e2e-latency-max
</td>  
<td>

The maximum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-e2e-latency-min
</td>  
<td>

The minimum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node.
</td>  
<td>

kafka.streams:type=stream-processor-node-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

records-consumed-total
</td>  
<td>

The total number of records consumed by a source processor node.
</td>  
<td>

kafka.streams:type=stream-topic-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+),topic=([-.\w]+)
</td> </tr>  
<tr>  
<td>

records-produced-total
</td>  
<td>

The total number of records produced by a sink processor node.
</td>  
<td>

kafka.streams:type=stream-topic-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),processor-node-id=([-.\w]+),topic=([-.\w]+)
</td> </tr> </table>

### State Store Metrics

All of the following metrics have a recording level of `debug`, except for the record-e2e-latency-* metrics which have a recording level `trace`. Note that the `store-scope` value is specified in `StoreSupplier#metricsScope()` for user's customized state stores; for built-in state stores, currently we have: 

  * `in-memory-state`
  * `in-memory-lru-state`
  * `in-memory-window-state`
  * `in-memory-suppression` (for suppression buffers)
  * `rocksdb-state` (for RocksDB backed key-value store)
  * `rocksdb-window-state` (for RocksDB backed window store)
  * `rocksdb-session-state` (for RocksDB backed session store)

Metrics suppression-buffer-size-avg, suppression-buffer-size-max, suppression-buffer-count-avg, and suppression-buffer-count-max are only available for suppression buffers. All other metrics are not available for suppression buffers.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

put-latency-avg
</td>  
<td>

The average put execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-latency-max
</td>  
<td>

The maximum put execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-if-absent-latency-avg
</td>  
<td>

The average put-if-absent execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-if-absent-latency-max
</td>  
<td>

The maximum put-if-absent execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

get-latency-avg
</td>  
<td>

The average get execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

get-latency-max
</td>  
<td>

The maximum get execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

delete-latency-avg
</td>  
<td>

The average delete execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

delete-latency-max
</td>  
<td>

The maximum delete execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-all-latency-avg
</td>  
<td>

The average put-all execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-all-latency-max
</td>  
<td>

The maximum put-all execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

all-latency-avg
</td>  
<td>

The average all operation execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

all-latency-max
</td>  
<td>

The maximum all operation execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

range-latency-avg
</td>  
<td>

The average range execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

range-latency-max
</td>  
<td>

The maximum range execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

flush-latency-avg
</td>  
<td>

The average flush execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

flush-latency-max
</td>  
<td>

The maximum flush execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

restore-latency-avg
</td>  
<td>

The average restore execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

restore-latency-max
</td>  
<td>

The maximum restore execution time in ns.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-rate
</td>  
<td>

The average put rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-if-absent-rate
</td>  
<td>

The average put-if-absent rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

get-rate
</td>  
<td>

The average get rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

delete-rate
</td>  
<td>

The average delete rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

put-all-rate
</td>  
<td>

The average put-all rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

all-rate
</td>  
<td>

The average all operation rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

range-rate
</td>  
<td>

The average range rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

flush-rate
</td>  
<td>

The average flush rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

restore-rate
</td>  
<td>

The average restore rate for this store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

suppression-buffer-size-avg
</td>  
<td>

The average total size, in bytes, of the buffered data over the sampling window.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),in-memory-suppression-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

suppression-buffer-size-max
</td>  
<td>

The maximum total size, in bytes, of the buffered data over the sampling window.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),in-memory-suppression-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

suppression-buffer-count-avg
</td>  
<td>

The average number of records buffered over the sampling window.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),in-memory-suppression-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

suppression-buffer-count-max
</td>  
<td>

The maximum number of records buffered over the sampling window.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),in-memory-suppression-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-e2e-latency-avg
</td>  
<td>

The average end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-e2e-latency-max
</td>  
<td>

The maximum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

record-e2e-latency-min
</td>  
<td>

The minimum end-to-end latency of a record, measured by comparing the record timestamp with the system time when it has been fully processed by the node.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr> </table>

### RocksDB Metrics

RocksDB metrics are grouped into statistics-based metrics and properties-based metrics. The former are recorded from statistics that a RocksDB state store collects whereas the latter are recorded from properties that RocksDB exposes. Statistics collected by RocksDB provide cumulative measurements over time, e.g. bytes written to the state store. Properties exposed by RocksDB provide current measurements, e.g., the amount of memory currently used. Note that the `store-scope` for built-in RocksDB state stores are currently the following: 

  * `rocksdb-state` (for RocksDB backed key-value store)
  * `rocksdb-window-state` (for RocksDB backed window store)
  * `rocksdb-session-state` (for RocksDB backed session store)

**RocksDB Statistics-based Metrics:** All of the following statistics-based metrics have a recording level of `debug` because collecting statistics in [RocksDB may have an impact on performance](https://github.com/facebook/rocksdb/wiki/Statistics#stats-level-and-performance-costs). Statistics-based metrics are collected every minute from the RocksDB state stores. If a state store consists of multiple RocksDB instances, as is the case for WindowStores and SessionStores, each metric reports an aggregation over the RocksDB instances of the state store.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

bytes-written-rate
</td>  
<td>

The average number of bytes written per second to the RocksDB state store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bytes-written-total
</td>  
<td>

The total number of bytes written to the RocksDB state store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bytes-read-rate
</td>  
<td>

The average number of bytes read per second from the RocksDB state store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bytes-read-total
</td>  
<td>

The total number of bytes read from the RocksDB state store.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

memtable-bytes-flushed-rate
</td>  
<td>

The average number of bytes flushed per second from the memtable to disk.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

memtable-bytes-flushed-total
</td>  
<td>

The total number of bytes flushed from the memtable to disk.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

memtable-hit-ratio
</td>  
<td>

The ratio of memtable hits relative to all lookups to the memtable.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

memtable-flush-time-avg
</td>  
<td>

The average duration of memtable flushes to disc in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

memtable-flush-time-min
</td>  
<td>

The minimum duration of memtable flushes to disc in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

memtable-flush-time-max
</td>  
<td>

The maximum duration of memtable flushes to disc in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

block-cache-data-hit-ratio
</td>  
<td>

The ratio of block cache hits for data blocks relative to all lookups for data blocks to the block cache.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

block-cache-index-hit-ratio
</td>  
<td>

The ratio of block cache hits for index blocks relative to all lookups for index blocks to the block cache.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

block-cache-filter-hit-ratio
</td>  
<td>

The ratio of block cache hits for filter blocks relative to all lookups for filter blocks to the block cache.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

write-stall-duration-avg
</td>  
<td>

The average duration of write stalls in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

write-stall-duration-total
</td>  
<td>

The total duration of write stalls in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bytes-read-compaction-rate
</td>  
<td>

The average number of bytes read per second during compaction.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

bytes-written-compaction-rate
</td>  
<td>

The average number of bytes written per second during compaction.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

compaction-time-avg
</td>  
<td>

The average duration of disc compactions in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

compaction-time-min
</td>  
<td>

The minimum duration of disc compactions in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

compaction-time-max
</td>  
<td>

The maximum duration of disc compactions in ms.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

number-open-files
</td>  
<td>

The number of current open files.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

number-file-errors-total
</td>  
<td>

The total number of file errors occurred.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr> </table> **RocksDB Properties-based Metrics:** All of the following properties-based metrics have a recording level of `info` and are recorded when the metrics are accessed. If a state store consists of multiple RocksDB instances, as is the case for WindowStores and SessionStores, each metric reports the sum over all the RocksDB instances of the state store, except for the block cache metrics `block-cache-*`. The block cache metrics report the sum over all RocksDB instances if each instance uses its own block cache, and they report the recorded value from only one instance if a single block cache is shared among all instances.   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

num-immutable-mem-table
</td>  
<td>

The number of immutable memtables that have not yet been flushed.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

cur-size-active-mem-table
</td>  
<td>

The approximate size of the active memtable in bytes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

cur-size-all-mem-tables
</td>  
<td>

The approximate size of active and unflushed immutable memtables in bytes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

size-all-mem-tables
</td>  
<td>

The approximate size of active, unflushed immutable, and pinned immutable memtables in bytes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-entries-active-mem-table
</td>  
<td>

The number of entries in the active memtable.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-entries-imm-mem-tables
</td>  
<td>

The number of entries in the unflushed immutable memtables.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-deletes-active-mem-table
</td>  
<td>

The number of delete entries in the active memtable.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-deletes-imm-mem-tables
</td>  
<td>

The number of delete entries in the unflushed immutable memtables.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

mem-table-flush-pending
</td>  
<td>

This metric reports 1 if a memtable flush is pending, otherwise it reports 0.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-running-flushes
</td>  
<td>

The number of currently running flushes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

compaction-pending
</td>  
<td>

This metric reports 1 if at least one compaction is pending, otherwise it reports 0.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-running-compactions
</td>  
<td>

The number of currently running compactions.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

estimate-pending-compaction-bytes
</td>  
<td>

The estimated total number of bytes a compaction needs to rewrite on disk to get all levels down to under target size (only valid for level compaction).
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

total-sst-files-size
</td>  
<td>

The total size in bytes of all SST files.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

live-sst-files-size
</td>  
<td>

The total size in bytes of all SST files that belong to the latest LSM tree.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

num-live-versions
</td>  
<td>

Number of live versions of the LSM tree.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

block-cache-capacity
</td>  
<td>

The capacity of the block cache in bytes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

block-cache-usage
</td>  
<td>

The memory size of the entries residing in block cache in bytes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

block-cache-pinned-usage
</td>  
<td>

The memory size for the entries being pinned in the block cache in bytes.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

estimate-num-keys
</td>  
<td>

The estimated number of keys in the active and unflushed immutable memtables and storage.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

estimate-table-readers-mem
</td>  
<td>

The estimated memory in bytes used for reading SST tables, excluding memory used in block cache.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

background-errors
</td>  
<td>

The total number of background errors.
</td>  
<td>

kafka.streams:type=stream-state-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),[store-scope]-id=([-.\w]+)
</td> </tr> </table>

### Record Cache Metrics

All of the following metrics have a recording level of `debug`:   
<table>  
<tr>  
<th>

Metric/Attribute name
</th>  
<th>

Description
</th>  
<th>

Mbean name
</th> </tr>  
<tr>  
<td>

hit-ratio-avg
</td>  
<td>

The average cache hit ratio defined as the ratio of cache read hits over the total cache read requests.
</td>  
<td>

kafka.streams:type=stream-record-cache-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),record-cache-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

hit-ratio-min
</td>  
<td>

The minimum cache hit ratio.
</td>  
<td>

kafka.streams:type=stream-record-cache-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),record-cache-id=([-.\w]+)
</td> </tr>  
<tr>  
<td>

hit-ratio-max
</td>  
<td>

The maximum cache hit ratio.
</td>  
<td>

kafka.streams:type=stream-record-cache-metrics,thread-id=([-.\w]+),task-id=([-.\w]+),record-cache-id=([-.\w]+)
</td> </tr> </table>

## Others

We recommend monitoring GC time and other stats and various server stats such as CPU utilization, I/O service time, etc. On the client side, we recommend monitoring the message/byte rate (global and per topic), request rate/size/time, and on the consumer side, max lag in messages among all partitions and min fetch request rate. For a consumer to keep up, max lag needs to be less than a threshold and min fetch rate needs to be larger than 0. 
