---
title: Topology Description Plugin
type: docs
description: Broker-side plugin for recording and exposing the processing topology of streams groups.
weight: 16
tags: ['kafka', 'docs']
aliases: 
keywords: 
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

Starting with Apache Kafka 4.4, brokers can record a human-readable description of the processing topology of each **streams group**, as defined by [KIP-1331](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1331%3A+Streams+Group+Topology+Description+Plugin). Kafka Streams clients push the same information that `Topology#describe()` returns to the group coordinator, which hands it to a pluggable, broker-side storage backend. Operators can then inspect the topology of any streams group via the [`Admin`](/{version}/javadoc/org/apache/kafka/clients/admin/Admin.html) API or the `bin/kafka-streams-groups.sh` CLI — without access to the application's source code or a running instance.

# Overview

The feature applies only to streams groups using the [Streams Rebalance Protocol](/{version}/streams/developer-guide/streams-rebalance-protocol/) (`group.protocol=streams`, KIP-1071). It is **disabled by default**: the broker only solicits, stores, and serves topology descriptions when the broker configuration `group.streams.topology.description.plugin.class` is set to a `StreamsGroupTopologyDescriptionPlugin` implementation.

When the feature is enabled:

  * Kafka Streams clients automatically push a description of their topology when the broker requests one — no application code changes are required. The push can be turned off per client via the Streams configuration `topology.description.push.enabled`.
  * The description is versioned by the group's **topology epoch**, so the broker always knows whether its stored description matches the topology the group is currently running.
  * The stored description can be retrieved with `Admin#describeStreamsGroups` (using `DescribeStreamsGroupsOptions#includeTopologyDescription(true)`) or with `kafka-streams-groups.sh --describe --topology`.

# How it works: the push/describe cycle

The feature adds one new RPC, `StreamsGroupTopologyDescriptionUpdate`, and extends the existing `StreamsGroupHeartbeat` and `StreamsGroupDescribe` RPCs. The cycle works as follows:

  1. **Solicitation.** When the group coordinator has not yet recorded a successful push for the group's current topology epoch (for example, a new group or a topology change that bumped the epoch), it sets the `TopologyDescriptionRequired` flag in the `StreamsGroupHeartbeat` response.
  2. **Push.** A client that sees this flag — and has `topology.description.push.enabled=true` — sends a `StreamsGroupTopologyDescriptionUpdate` request to the group coordinator, containing its group ID, member ID, the topology epoch, and the topology description (subtopologies, sources, processors, sinks, state stores, and global stores).
  3. **Store.** The broker validates that the sender is a known member of the group and invokes the plugin's `setTopology(groupId, topologyEpoch, description)` method. On success, the broker records the stored topology epoch and stops soliciting. Multiple members may push concurrently for the same epoch; the pushed data is identical, and the plugin must handle this idempotently.
  4. **Failure handling.** If the plugin fails to store the description, the broker distinguishes two cases:
     * A **permanent failure** (`StreamsTopologyDescriptionPermanentFailureException`) means the description will never be accepted at this topology epoch (for example, it is too large or semantically rejected). The broker records the failed epoch and stops soliciting until the topology epoch advances.
     * A **transient failure** (`StreamsTopologyDescriptionTransientFailureException`, or any other exception) causes the broker to arm a per-group exponential back-off (30 seconds up to 1 hour) and re-solicit the description on a later heartbeat.

     In both cases, the pushing client receives the error code `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED`. The client does not retry on its own; the broker drives retries through heartbeat solicitation.
  5. **Describe.** When a caller requests the topology description via `StreamsGroupDescribe` (version 1 or higher, with `IncludeTopologyDescription=true`), the broker invokes the plugin's `getTopology(groupId, topologyEpoch)` method and attaches the description together with a status field to the response. See [Interpreting the topology description status](#interpreting-the-topology-description-status) below.
  6. **Deletion.** When a streams group is deleted (via `DeleteGroups`) or expires, the broker invokes the plugin's `deleteTopology(groupId)` method so that the plugin can clean up its stored data. See [Group deletion and GROUP_DELETION_FAILED](#group-deletion-and-group_deletion_failed) below for the failure semantics.

# Broker configuration

  * [`group.streams.topology.description.plugin.class`](/{version}/configuration/broker-configs#brokerconfigs_group.streams.topology.description.plugin.class): The fully qualified class name of a `StreamsGroupTopologyDescriptionPlugin` implementation. When not set (the default), the feature is disabled: the broker never solicits topology descriptions, and describe requests report status `NOT_STORED`.

Apache Kafka ships a reference implementation, `org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin`, which stores one description per group in an in-memory map. It is intended for testing and as a starting point for real implementations. It is **not suitable for production** because its state is lost on broker restart and is not shared across brokers.

# Client configuration

  * [`topology.description.push.enabled`](/{version}/configuration/kafka-streams-configs#streamsconfigs_topology.description.push.enabled): Controls whether the Kafka Streams client sends topology descriptions to the broker when requested. When set to `false`, the client will not prepare or push topology descriptions. Enabled by default.

Note that this configuration only controls whether the client *responds* to broker solicitations. If the broker has no plugin configured, the client is never asked to push, regardless of this setting.

# Implementing a plugin

A plugin implements the [`StreamsGroupTopologyDescriptionPlugin`](/{version}/javadoc/org/apache/kafka/coordinator/group/api/streams/StreamsGroupTopologyDescriptionPlugin.html) interface from the `group-coordinator-api` module:

```java
public interface StreamsGroupTopologyDescriptionPlugin extends Configurable, AutoCloseable {

    CompletableFuture<Void> setTopology(String groupId, int topologyEpoch, StreamsGroupTopologyDescription description);

    CompletableFuture<Void> deleteTopology(String groupId);

    CompletableFuture<StreamsGroupTopologyDescription> getTopology(String groupId, int topologyEpoch);
}
```

Guidelines for implementations:

  * **Be thread-safe.** `setTopology` may be called concurrently by multiple members of the same group.
  * **Be idempotent.** Calls to `setTopology` with the same `(groupId, topologyEpoch)` carry identical data and must be idempotent. `deleteTopology` may be called more than once for the same group, including when nothing is stored.
  * **Complete futures asynchronously; never throw synchronously.** Failures must be signalled by completing the returned future exceptionally. A synchronous throw from `setTopology` is treated as a permanent failure with a generic client-visible error message.
  * **Classify failures.** Complete the `setTopology` future with `StreamsTopologyDescriptionPermanentFailureException` when the description will never be accepted at this topology epoch, and with `StreamsTopologyDescriptionTransientFailureException` (or any other exception) for retriable backend failures. The permanent-vs-transient distinction is broker-internal; the pushing client always sees `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED` with the exception's message.
  * **Key data by group and epoch.** `getTopology(groupId, topologyEpoch)` should return the description only if it matches the requested topology epoch, and complete with `null` when the plugin no longer has the data (for example, after a backend wipe) — the broker then reports status `NOT_STORED`. If the future completes exceptionally, the broker reports a read error (status `ERROR`) for the group. Note that the broker only solicits a new push when it has not recorded a successful push for the current topology epoch; a plugin that loses already-stored data is not automatically re-populated until the topology epoch advances, so implementations should use durable storage.
  * **Lifecycle.** The plugin is instantiated once per broker, configured via `Configurable#configure(Map)` with the broker configuration, and closed via `AutoCloseable#close()` on broker shutdown.

# Reading the topology description

## Admin API

Pass `DescribeStreamsGroupsOptions#includeTopologyDescription(true)` to `Admin#describeStreamsGroups`:

```java
try (Admin admin = Admin.create(props)) {
    DescribeStreamsGroupsResult result = admin.describeStreamsGroups(
        List.of("my-streams-app"),
        new DescribeStreamsGroupsOptions().includeTopologyDescription(true));
    StreamsGroupDescription description = result.describedGroups().get("my-streams-app").get();
    StreamsGroupTopologyDescriptionStatus status = description.topologyDescriptionStatus();
    Optional<StreamsGroupTopologyDescription> topology = description.topologyDescription();
}
```

The returned `StreamsGroupTopologyDescription` mirrors `org.apache.kafka.streams.TopologyDescription` (subtopologies with source, processor, and sink nodes, plus global stores) without requiring a dependency on the `kafka-streams` library. Requesting a topology description against a broker that does not support it (older than 4.4) fails with `UnsupportedVersionException`.

## CLI

Use the `--topology` option of `bin/kafka-streams-groups.sh` together with `--describe`:

```
kafka-streams-groups.sh --bootstrap-server localhost:9092 \
  --describe --group my-streams-app --topology
```

When a description is available, the output mirrors the format of `Topology#describe()`:

```
Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [streams-plaintext-input])
      --> KSTREAM-FLATMAPVALUES-0000000001
    Processor: KSTREAM-FLATMAPVALUES-0000000001 (stores: [])
      --> KSTREAM-AGGREGATE-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-AGGREGATE-0000000002 (stores: [counts-store])
      --> KSTREAM-SINK-0000000003
      <-- KSTREAM-FLATMAPVALUES-0000000001
    Sink: KSTREAM-SINK-0000000003 (topic: streams-wordcount-output)
      <-- KSTREAM-AGGREGATE-0000000002
```

If no description is available, the tool prints an explanatory message and exits with a non-zero exit code. See the [kafka-streams-groups.sh documentation](/{version}/streams/developer-guide/kafka-streams-group-sh/) for the full CLI reference.

## Interpreting the topology description status

Every describe response that requested a topology description carries a `StreamsGroupTopologyDescriptionStatus`. The description itself is present if and only if the status is `AVAILABLE`.

  * `NOT_REQUESTED`: The topology description was not requested (the caller did not set `includeTopologyDescription(true)`).
  * `NOT_STORED`: No topology description is recorded for this group — for example, because no topology description plugin is configured on the broker, or the clients have not pushed a description yet.
  * `ERROR`: The broker failed to fetch the topology description from the plugin. See the broker logs for details.
  * `AVAILABLE`: The topology description is available and carried in the response.

# Group deletion and GROUP_DELETION_FAILED

When a streams group is deleted while a topology description plugin is configured, the broker calls the plugin's `deleteTopology` method before removing the group. If the plugin fails to delete its data, the `DeleteGroups` request returns the error code `GROUP_DELETION_FAILED` for that group, with the plugin's exception message in the per-group `ErrorMessage` field (available in `DeleteGroups` version 3 and higher), and the broker does **not** delete the group. Retrying the deletion re-invokes `deleteTopology` idempotently. Groups that expire through periodic cleanup are treated identically — their removal is deferred to a future cleanup cycle until the plugin deletion succeeds.

# Observability

The broker exposes metrics for every plugin interaction under the MBean group `kafka.server:type=group-coordinator-metrics`; the full list is in the [group coordinator monitoring reference](/{version}/operations/monitoring#group-coordinator-monitoring). Each sensor is published as both a `-rate` (per-second) and a `-count` (cumulative) metric, so `streams-group-topology-description-set-success` becomes `streams-group-topology-description-set-success-rate` and `streams-group-topology-description-set-success-count`.

  * `streams-group-topology-description-set-success` / `streams-group-topology-description-set-error`: outcomes of `setTopology` calls, driven by client pushes.
  * `streams-group-topology-description-get-success` / `streams-group-topology-description-get-error`: outcomes of `getTopology` calls, driven by describe requests.
  * `streams-group-topology-description-delete-success` / `streams-group-topology-description-delete-error`: outcomes of `deleteTopology` calls, driven by group deletion and cleanup.
  * `streams-group-topology-description-cleanup-cycle`: number of periodic cleanup cycles the coordinator has run.
  * `streams-group-topology-description-cleanup-eligible`: number of groups the cleanup scan found eligible for plugin-state deletion.

Watch the `-error` sensors first: a rising `get-error` rate explains `ERROR` describe responses, a rising `set-error` rate explains descriptions that never appear, and a rising `delete-error` rate explains `GROUP_DELETION_FAILED`.

# Troubleshooting

Before reading broker logs, check the `streams-group-topology-description-*-error` metrics described under [Observability](#observability) — they pinpoint which plugin call (`set`, `get`, or `delete`) is failing.

**`--topology` reports "No topology description is stored" (status `NOT_STORED`).**

  * Verify that `group.streams.topology.description.plugin.class` is set on all brokers hosting the group coordinator. Without it, the feature is disabled.
  * Verify that the application does not set `topology.description.push.enabled=false`.
  * If the group (or its topology epoch) is new, the clients may simply not have pushed yet — the broker solicits the push via the heartbeat, so the description typically appears within a few heartbeat intervals.
  * If the description still does not appear, check the broker logs for failed `setTopology` calls. After a permanent failure (for example, a description the plugin rejects), the broker stops soliciting until the topology epoch advances.
  * If the description used to appear and has now vanished, the plugin may have lost its stored data. When the plugin's `getTopology` returns `null`, the broker surfaces the status as `NOT_STORED` (logged at `WARN`) and keeps returning `NOT_STORED` on subsequent describes. Because the broker only re-solicits a push when the topology epoch advances, restarting the application without bumping the topology will not recover the description — advance the topology epoch or clear the plugin state to trigger a fresh push.

**Status `ERROR` when describing.**

  * The plugin's `getTopology` call failed on the broker. Check the broker logs of the group coordinator for the underlying exception.

**Push delivery issues.**

  * A failed push surfaces to the client as `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED` and is logged by the Streams client; the client does not retry on its own. The broker re-solicits the push via the heartbeat — after transient plugin failures with an exponential back-off between 30 seconds and 1 hour.
  * If the pushing member has been fenced or the group was deleted meanwhile, the push fails with `UNKNOWN_MEMBER_ID` and the client rejoins the group; this is expected and self-healing.

**`DeleteGroups` fails with `GROUP_DELETION_FAILED`.**

  * The plugin failed to delete the stored description. The per-group error message contains the plugin's failure reason; the broker logs contain the full exception. Resolve the plugin/backend problem and retry the deletion — `deleteTopology` is invoked again idempotently.

**`UnsupportedVersionException` when requesting the topology description.**

  * The broker is older than Apache Kafka 4.4 and does not support `StreamsGroupDescribe` version 1. Upgrade the broker, or describe the group without requesting the topology description.

  * [Documentation](/documentation)
  * [Kafka Streams](/documentation/streams)
  * [Developer Guide](/documentation/streams/developer-guide/)

