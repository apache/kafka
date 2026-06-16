/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Broker-level component that owns everything tied to the streams-group topology
 * description plugin: the configured plugin reference, the per-group
 * re-solicitation back-off, and the entry points the group coordinator calls into —
 * heartbeat post-processing and the push RPC. The {@code DeleteGroups} hook lands in
 * a follow-up sub-task.
 *
 * <p>This class is broker-level (one instance per {@code GroupCoordinatorService}); the
 * back-off map is keyed by {@code groupId} and shared across all partitions hosted on the
 * broker. State here is intentionally non-timeline and non-replayed: it is rebuilt from
 * scratch on broker restart, and the persisted {@code StoredDescriptionTopologyEpoch} /
 * {@code FailedDescriptionTopologyEpoch} fields on each streams group drive
 * convergence after a restart.
 *
 * <p>Methods that schedule runtime operations require a partition resolver supplied by
 * the caller (typically {@code GroupCoordinatorService::topicPartitionFor}) so this
 * class can stay decoupled from the offsets-topic partition layout.
 */
public class StreamsGroupTopologyDescriptionManager implements AutoCloseable {
    private final Optional<StreamsGroupTopologyDescriptionPlugin> plugin;
    private final StreamsGroupTopologyDescriptionBackoff backoff;
    private final CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;
    private final Function<String, TopicPartition> topicPartitionFor;

    public StreamsGroupTopologyDescriptionManager(
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        Function<String, TopicPartition> topicPartitionFor,
        Time time
    ) {
        this.plugin = plugin;
        this.runtime = runtime;
        this.topicPartitionFor = topicPartitionFor;
        this.backoff = new StreamsGroupTopologyDescriptionBackoff(time);
    }

    /**
     * Release plugin-side resources. The plugin is instantiated by the service via
     * {@code config.getConfiguredInstance(...)}, so the service owns it and must close
     * it on shutdown to avoid leaking threads, network clients, etc. across broker
     * restart cycles.
     */
    @Override
    public void close() throws Exception {
        if (plugin.isPresent()) {
            plugin.get().close();
        }
    }

    /**
     * @return true if a topology description plugin is configured on this broker.
     */
    public boolean isPluginConfigured() {
        return plugin.isPresent();
    }

    /**
     * Post-processes a successful streams group heartbeat result by deciding whether the
     * broker should set {@code TopologyDescriptionRequired=true} on the response, and
     * arming the per-group back-off when it does.
     *
     * <p>The flag is set when the request is at a version that carries the field
     * ({@code TopologyDescriptionRequired} arrives at v1), the topology description plugin
     * is configured, the group has resolved to a topology epoch, that epoch is neither
     * stored nor permanently failed at the plugin, no back-off is in effect for this
     * epoch, and the response does not carry a {@code STALE_TOPOLOGY} status (the member
     * would just be told to catch up first). When the response already carries an error
     * code we leave it alone.
     *
     * <p>The version gate is intentional: a v0 client cannot deserialize the flag, so
     * arming the back-off for it would accumulate entries that grow exponentially while
     * the flag itself gets dropped at serialization — wasting heap on a per-group basis
     * for clients that will never push.
     */
    public StreamsGroupHeartbeatResult maybeSetTopologyDescriptionRequired(
        StreamsGroupHeartbeatResult result,
        String groupId,
        int apiVersion
    ) {
        if (apiVersion < 1 || plugin.isEmpty()) {
            return result;
        }
        StreamsGroupHeartbeatResponseData response = result.data();
        if (response.errorCode() != Errors.NONE.code()) {
            return result;
        }
        int currentEpoch = result.currentTopologyEpoch();
        if (currentEpoch < 0
            || result.storedDescriptionTopologyEpoch() == currentEpoch
            || result.failedDescriptionTopologyEpoch() == currentEpoch
            || responseHasStaleTopology(response)) {
            return result;
        }
        // Atomic check-and-arm: only set the flag if the back-off window is not already
        // in effect for this epoch, so two concurrent heartbeats for the same group cannot
        // both arm the back-off and double the window beyond its intended length.
        if (backoff.armIfNotActive(groupId, currentEpoch)) {
            response.setTopologyDescriptionRequired(true);
        }
        return result;
    }

    /**
     * Drive the push chain: validate the (group, member), convert the wire payload, call
     * the plugin, persist the outcome, and centralize back-off state mutations in a
     * single {@code whenComplete}.
     *
     * <p>The chain carries the terminal disposition through an {@link AtomicReference}
     * holder so {@code whenComplete} can act on it without having to reason about the
     * response shape. The holder is committed to {@link BackoffAction#ARM} as the chain
     * crosses the plugin boundary, so any post-plugin failure — including a metadata
     * record write that fails after a successful plugin call — re-arms the back-off and
     * the next heartbeat re-solicits an idempotent re-push. Pre-plugin failures
     * ({@code validateStreamsGroupMember} fencing the caller, {@code fromRequest}
     * rejecting a malformed payload, or the read operation surfacing a runtime error
     * such as NOT_COORDINATOR) leave the holder at {@link BackoffAction#NOOP}, so a
     * fenced/unauthorized caller cannot grief the back-off and suppress legitimate
     * solicitation for the rest of the group.
     */
    public CompletableFuture<StreamsGroupTopologyDescriptionUpdateResponseData> handleSetTopology(
        StreamsGroupTopologyDescriptionUpdateRequestData request
    ) {
        final String groupId = request.groupId();
        final String memberId = request.memberId();
        final int pushedEpoch = request.topologyEpoch();
        final TopicPartition tp = topicPartitionFor.apply(groupId);
        final StreamsGroupTopologyDescriptionPlugin p = plugin.get();

        final AtomicReference<BackoffAction> backoffAction = new AtomicReference<>(BackoffAction.NOOP);
        return runtime.scheduleReadOperation(
                "streams-group-topology-description-validate",
                tp,
                (coordinator, lastCommittedOffset) -> {
                    coordinator.validateStreamsGroupTopologyDescriptionUpdate(
                        groupId, memberId, pushedEpoch, lastCommittedOffset);
                    return null;
                })
            .thenApply(__ -> StreamsGroupTopologyDescriptionConverter.fromRequest(request.topologyDescription()))
            .thenCompose(description -> {
                // Plugin boundary: from here on, a failure means we attempted the push and
                // therefore arms the back-off (unless a downstream stage explicitly upgrades
                // the action to CLEAR after a successful epoch write).
                backoffAction.set(BackoffAction.ARM);
                return invokePluginSetTopology(p, groupId, pushedEpoch, description);
            })
            .thenCompose(pluginOutcome -> switch (pluginOutcome.kind()) {
                case SUCCESS -> runtime.scheduleWriteOperation(
                    "streams-group-set-stored-topology-epoch",
                    tp,
                    coordinator -> coordinator.streamsGroupSetTopologyDescriptionEpoch(groupId, pushedEpoch, false)
                ).thenApply(unused -> {
                    backoffAction.set(BackoffAction.CLEAR);
                    return new StreamsGroupTopologyDescriptionUpdateResponseData();
                });
                case PERMANENT -> runtime.scheduleWriteOperation(
                    "streams-group-set-failed-topology-epoch",
                    tp,
                    coordinator -> coordinator.streamsGroupSetTopologyDescriptionEpoch(groupId, pushedEpoch, true)
                ).thenApply(unused -> {
                    backoffAction.set(BackoffAction.CLEAR);
                    return errorResponse(
                        Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED, pluginOutcome.message());
                });
                case TRANSIENT -> CompletableFuture.completedFuture(errorResponse(
                    Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED, pluginOutcome.message()));
            })
            .whenComplete((response, throwable) -> {
                switch (backoffAction.get()) {
                    case CLEAR -> backoff.clear(groupId, pushedEpoch);
                    case ARM -> backoff.armOrExtend(groupId, pushedEpoch);
                    case NOOP -> { /* pre-plugin failure: don't touch the back-off */ }
                }
            });
    }

    // Visible for testing.
    StreamsGroupTopologyDescriptionBackoff backoff() {
        return backoff;
    }

    private static boolean responseHasStaleTopology(StreamsGroupHeartbeatResponseData response) {
        if (response.status() == null) {
            return false;
        }
        byte staleCode = Status.STALE_TOPOLOGY.code();
        return response.status().stream().anyMatch(s -> s.statusCode() == staleCode);
    }

    private static StreamsGroupTopologyDescriptionUpdateResponseData errorResponse(
        Errors error,
        String message
    ) {
        return new StreamsGroupTopologyDescriptionUpdateResponseData()
            .setErrorCode(error.code())
            .setErrorMessage(message);
    }

    /**
     * Calls the plugin's {@code setTopology} and folds the result into a {@link PluginOutcome}.
     * The future never completes exceptionally — the outcome carries the failure category.
     */
    private static CompletableFuture<PluginOutcome> invokePluginSetTopology(
        StreamsGroupTopologyDescriptionPlugin plugin,
        String groupId,
        int pushedEpoch,
        StreamsGroupTopologyDescription description
    ) {
        final CompletableFuture<Void> pluginFuture;
        try {
            pluginFuture = Objects.requireNonNull(plugin.setTopology(groupId, pushedEpoch, description),
                "Plugin returned null future from setTopology.");
        } catch (Exception e) {
            // A synchronous throw or a null future both violate the SPI contract —
            // implementations must signal failures by completing the future exceptionally.
            // Treat either as a permanent failure with a stable, generic client-visible
            // message so we don't forward an unbounded or null exception message that
            // could leak plugin internals, and so a misbehaving plugin doesn't NPE the
            // chain and degenerate into back-off-with-doubling instead of the
            // permanent-failure handling it should get.
            return CompletableFuture.completedFuture(
                PluginOutcome.permanent("Topology description plugin failed."));
        }
        return pluginFuture.handle((unused, throwable) -> {
            if (throwable == null) {
                return PluginOutcome.success();
            }
            Throwable cause = Errors.maybeUnwrapException(throwable);
            if (cause instanceof StreamsTopologyDescriptionPermanentFailureException) {
                return PluginOutcome.permanent(cause.getMessage());
            }
            return PluginOutcome.transientFailure(cause.getMessage());
        });
    }

    private record PluginOutcome(Kind kind, String message) {
        enum Kind { SUCCESS, PERMANENT, TRANSIENT }

        static PluginOutcome success() {
            return new PluginOutcome(Kind.SUCCESS, null);
        }

        static PluginOutcome permanent(String message) {
            return new PluginOutcome(Kind.PERMANENT, message);
        }

        static PluginOutcome transientFailure(String message) {
            return new PluginOutcome(Kind.TRANSIENT, message);
        }
    }

    private enum BackoffAction { NOOP, ARM, CLEAR }
}
