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
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.internals.ExponentialBackoff;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.api.streams.PluginPermanentFailureException;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Owns broker-side orchestration of the streams-group topology description plugin: heartbeat
 * solicitation, {@code setTopology} / {@code getTopology} / {@code deleteTopology} dispatch,
 * the per-group exponential back-off, and the periodic natural-expiration cleanup cycle.
 *
 * <p>When no plugin is configured, every public method becomes a fast no-op.
 */
public class TopologyDescriptionManager implements AutoCloseable {

    private static final byte TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED = 1;
    private static final byte TOPOLOGY_DESCRIPTION_STATUS_ERROR = 2;
    private static final byte TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE = 3;

    /**
     * Per-group back-off after an unsuccessful solicitation — either a transient
     * {@code setTopology} failure (anything other than {@code PluginPermanentFailureException}) or
     * a heartbeat-side solicitation that never produced a successful push within the previous
     * back-off window (e.g. a client with {@code topology.description.push.enabled=false}, or one
     * that is unreachable). Doubles per consecutive solicitation from 30 s up to 1 h.
     */
    private static final ExponentialBackoff RETRY_BACKOFF =
        new ExponentialBackoff(30_000L, 2, 3_600_000L, 0.0);

    /**
     * Per-group back-off state for transient {@code setTopology} failures. The entry is keyed by
     * the topology epoch the failure was observed at; an epoch advance implicitly invalidates the
     * back-off. In-memory only — coordinator failover loses the state, in which case the new
     * leader re-solicits once and the back-off re-arms on the next failure.
     */
    private static final class Backoff {
        final int topologyEpoch;
        final int attempts;
        final long nextAttemptMs;

        Backoff(int topologyEpoch, int attempts, long nextAttemptMs) {
            this.topologyEpoch = topologyEpoch;
            this.attempts = attempts;
            this.nextAttemptMs = nextAttemptMs;
        }
    }

    private final Logger log;
    private final Optional<StreamsGroupTopologyDescriptionPlugin> plugin;
    private final CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime;
    private final Timer timer;
    private final Time time;
    private final long cleanupCheckIntervalMs;
    private final GroupCoordinatorMetrics metrics;
    private final Function<String, TopicPartition> topicPartitionFor;
    private final Supplier<Boolean> isActive;

    private final ConcurrentHashMap<String, Backoff> backoff = new ConcurrentHashMap<>();
    private final AtomicBoolean cleanupCycleInFlight = new AtomicBoolean(false);
    private volatile TimerTask cleanupTask;

    public TopologyDescriptionManager(
        LogContext logContext,
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        Timer timer,
        Time time,
        long cleanupCheckIntervalMs,
        GroupCoordinatorMetrics metrics,
        Function<String, TopicPartition> topicPartitionFor,
        Supplier<Boolean> isActive
    ) {
        this.log = logContext.logger(TopologyDescriptionManager.class);
        this.plugin = plugin;
        this.runtime = runtime;
        this.timer = timer;
        this.time = time;
        this.cleanupCheckIntervalMs = cleanupCheckIntervalMs;
        this.metrics = metrics;
        this.topicPartitionFor = topicPartitionFor;
        this.isActive = isActive;
    }

    /**
     * @return whether a plugin is configured (and therefore this manager has any work to do).
     */
    public boolean isPresent() {
        return plugin.isPresent();
    }

    /**
     * Starts the periodic topology-description cleanup timer. Called from the service's
     * {@code startup()}; idempotent under multiple calls.
     */
    public void start() {
        if (plugin.isEmpty()) return;
        scheduleCleanupCycle();
    }

    @Override
    public void close() {
        TimerTask snapshot = cleanupTask;
        if (snapshot != null) {
            snapshot.cancel();
        }
        plugin.ifPresent(p -> Utils.closeQuietly(p, "topology description plugin"));
    }

    // -----------------------------------------------------------------------------------------
    // Heartbeat path
    // -----------------------------------------------------------------------------------------

    /**
     * Decides whether to set {@code TopologyDescriptionRequired=true} on the heartbeat response
     * and applies the decision in place. Pure broker-side: no plugin RPC.
     *
     * <p>When the broker sets the flag, the back-off is armed (or extended) at the same time:
     * the next heartbeat for the same group + topology epoch is suppressed until the back-off
     * window expires. This handles both unresponsive plugins (transient failure re-arms the
     * same back-off) and clients that ignore the flag (the next heartbeat after the window
     * lapses simply re-solicits, advancing the back-off by one step).
     */
    public void maybeMarkTopologyDescriptionRequired(
        StreamsGroupHeartbeatResponseData responseData,
        String groupId,
        StreamsGroupHeartbeatResult result
    ) {
        if (plugin.isEmpty() || responseData.errorCode() != Errors.NONE.code()) return;
        int topologyEpoch = result.topologyEpoch();
        if (!shouldSolicitTopologyPush(groupId, topologyEpoch, result)) return;
        if (hasStaleTopologyStatus(responseData)) return;
        responseData.setTopologyDescriptionRequired(true);
        armBackoff(groupId, topologyEpoch);
    }

    private static boolean hasStaleTopologyStatus(StreamsGroupHeartbeatResponseData responseData) {
        List<StreamsGroupHeartbeatResponseData.Status> status = responseData.status();
        if (status == null) return false;
        byte staleCode = org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status.STALE_TOPOLOGY.code();
        for (StreamsGroupHeartbeatResponseData.Status s : status) {
            if (s.statusCode() == staleCode) return true;
        }
        return false;
    }

    /**
     * Pure broker-side check for whether a heartbeat should request a topology push. The four
     * suppression rules: topology not initialised yet (epoch &lt; 0), stored epoch already matches
     * current, permanent failure already recorded at this epoch, or the back-off
     * still in its window for this {@code (groupId, epoch)} pair.
     */
    private boolean shouldSolicitTopologyPush(String groupId, int topologyEpoch, StreamsGroupHeartbeatResult result) {
        if (topologyEpoch < 0) return false;
        if (result.storedTopologyEpoch() == topologyEpoch) return false;
        if (result.lastFailedTopologyEpoch() == topologyEpoch) return false;
        Backoff b = backoff.get(groupId);
        return b == null || b.topologyEpoch != topologyEpoch || time.milliseconds() >= b.nextAttemptMs;
    }

    // -----------------------------------------------------------------------------------------
    // setTopology path
    // -----------------------------------------------------------------------------------------

    /**
     * Invokes {@code plugin.setTopology} and persists the result via the appropriate broker-side
     * tagged field. On success: {@code StoredTopologyEpoch = pushedEpoch}, back-off cleared.
     * On {@code PluginPermanentFailureException}: {@code LastFailedTopologyEpoch = pushedEpoch},
     * back-off cleared (the ratchet takes over).
     * On any other exception: arm/extend the per-group back-off; no metadata write.
     * The wire response always carries {@code STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED} with the
     * exception's message; the permanent-vs-transient split is reflected only in broker-side state.
     */
    public CompletableFuture<StreamsGroupTopologyDescriptionUpdateResponseData> handleSetTopology(
        String groupId,
        int pushedEpoch,
        StreamsGroupTopologyDescription description
    ) {
        // Caller has already validated the plugin is configured.
        StreamsGroupTopologyDescriptionPlugin p = plugin.orElseThrow(IllegalStateException::new);
        return p.setTopology(groupId, pushedEpoch, description)
            .handle((__, throwable) -> {
                StreamsGroupTopologyDescriptionUpdateResponseData responseData =
                    new StreamsGroupTopologyDescriptionUpdateResponseData();
                if (throwable != null) {
                    Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null
                        ? throwable.getCause() : throwable;
                    log.warn("Plugin operation failed for group {}", groupId, cause);
                    metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME);
                    boolean permanentFailure = cause instanceof PluginPermanentFailureException;
                    responseData.setErrorCode(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code());
                    responseData.setErrorMessage(cause.getMessage());
                    if (permanentFailure) {
                        backoff.remove(groupId);
                        recordTopologyDescriptionFailedAsync(groupId, pushedEpoch);
                    } else {
                        armBackoff(groupId, pushedEpoch);
                    }
                } else {
                    log.info("Plugin operation succeeded for group {}", groupId);
                    metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME);
                    responseData.setErrorCode(Errors.NONE.code());
                    backoff.remove(groupId);
                    recordTopologyDescriptionStoredAsync(groupId, pushedEpoch);
                }
                return responseData;
            });
    }

    private void armBackoff(String groupId, int pushedEpoch) {
        long now = time.milliseconds();
        backoff.compute(groupId, (k, existing) -> {
            int attempts = (existing != null && existing.topologyEpoch == pushedEpoch)
                ? existing.attempts + 1
                : 1;
            return new Backoff(pushedEpoch, attempts, now + RETRY_BACKOFF.backoff(attempts - 1));
        });
    }

    private void recordTopologyDescriptionStoredAsync(String groupId, int pushedEpoch) {
        runtime.<Void>scheduleWriteOperation(
            "record-topology-description-stored",
            topicPartitionFor.apply(groupId),
            coordinator -> coordinator.updateStreamsGroupTopologyFields(groupId, pushedEpoch, null)
        ).whenComplete((__, throwable) -> {
            if (throwable != null) {
                log.warn("Failed to persist StoredTopologyEpoch={} for group {}; the next heartbeat will re-solicit.",
                    pushedEpoch, groupId, throwable);
            }
        });
    }

    private void recordTopologyDescriptionFailedAsync(String groupId, int pushedEpoch) {
        runtime.<Void>scheduleWriteOperation(
            "record-topology-description-failed",
            topicPartitionFor.apply(groupId),
            coordinator -> coordinator.updateStreamsGroupTopologyFields(groupId, null, pushedEpoch)
        ).whenComplete((__, throwable) -> {
            if (throwable != null) {
                log.warn("Failed to persist LastFailedTopologyEpoch={} for group {}; the next heartbeat may re-solicit and hit the same failure.",
                    pushedEpoch, groupId, throwable);
            }
        });
    }

    private void clearStoredTopologyEpochAsync(String groupId) {
        runtime.<Void>scheduleWriteOperation(
            "clear-stored-topology-epoch",
            topicPartitionFor.apply(groupId),
            coordinator -> coordinator.updateStreamsGroupTopologyFields(groupId, -1, null)
        ).whenComplete((__, throwable) -> {
            if (throwable != null) {
                log.warn("Failed to clear StoredTopologyEpoch for group {}; the next cleanup cycle will retry.",
                    groupId, throwable);
            }
        });
    }

    // -----------------------------------------------------------------------------------------
    // Describe path
    // -----------------------------------------------------------------------------------------

    /**
     * For each described group with {@code IncludeTopologyDescription=true}, decides whether to
     * call {@code plugin.getTopology} (only when {@code StoredTopologyEpoch == currentEpoch}) and
     * populates the response. Returns a future that completes when all plugin calls have settled.
     */
    public CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> attachTopologyDescriptions(
        StreamsGroupDescribeResult result
    ) {
        if (plugin.isEmpty()) {
            for (StreamsGroupDescribeResponseData.DescribedGroup g : result.describedGroups()) {
                if (g.errorCode() == Errors.NONE.code()) {
                    g.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
                }
            }
            return CompletableFuture.completedFuture(result.describedGroups());
        }
        StreamsGroupTopologyDescriptionPlugin p = plugin.get();
        List<CompletableFuture<Void>> pluginFutures = new ArrayList<>();
        for (StreamsGroupDescribeResponseData.DescribedGroup describedGroup : result.describedGroups()) {
            CompletableFuture<Void> f = maybeAttachTopologyDescription(p, describedGroup, result);
            if (f != null) pluginFutures.add(f);
        }
        if (pluginFutures.isEmpty()) return CompletableFuture.completedFuture(result.describedGroups());
        return CompletableFuture.allOf(pluginFutures.toArray(new CompletableFuture<?>[0]))
            .thenApply(__ -> result.describedGroups());
    }

    private CompletableFuture<Void> maybeAttachTopologyDescription(
        StreamsGroupTopologyDescriptionPlugin p,
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup,
        StreamsGroupDescribeResult result
    ) {
        if (describedGroup.errorCode() != Errors.NONE.code()) return null;
        if (describedGroup.topology() == null) {
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
            return null;
        }
        Integer storedTopologyEpoch = result.storedTopologyEpochs().get(describedGroup.groupId());
        if (storedTopologyEpoch == null) {
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
            return null;
        }
        int topologyEpoch = describedGroup.topology().epoch();
        if (storedTopologyEpoch != topologyEpoch) {
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
            return null;
        }
        return p.getTopology(describedGroup.groupId(), topologyEpoch)
            .handle((topology, throwable) -> {
                applyGetTopologyResult(describedGroup, topologyEpoch, topology, throwable);
                return null;
            });
    }

    private void applyGetTopologyResult(
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup,
        int topologyEpoch,
        StreamsGroupTopologyDescription topology,
        Throwable throwable
    ) {
        String groupId = describedGroup.groupId();
        if (throwable != null) {
            log.warn("Plugin getTopology failed for group {}", groupId, throwable);
            metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_ERROR);
            return;
        }
        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
        if (topology != null) {
            describedGroup.setTopologyDescription(pojoToDescribeResponse(topology));
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE);
            return;
        }
        log.warn("Plugin getTopology returned null for group {} while StoredTopologyEpoch={} matched the current topology epoch.",
            groupId, topologyEpoch);
        describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
    }

    /**
     * Converts the broker-side POJO returned by {@code plugin.getTopology} into the wire schema
     * carried on the describe response. The two share field names but live in different packages.
     */
    private static StreamsGroupDescribeResponseData.TopologyDescription pojoToDescribeResponse(
        StreamsGroupTopologyDescription topology
    ) {
        StreamsGroupDescribeResponseData.TopologyDescription out = new StreamsGroupDescribeResponseData.TopologyDescription();
        List<StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology> subs = new ArrayList<>();
        for (StreamsGroupTopologyDescription.Subtopology st : topology.subtopologies()) {
            StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology s =
                new StreamsGroupDescribeResponseData.TopologyDescriptionSubtopology()
                    .setSubtopologyId(st.id());
            List<StreamsGroupDescribeResponseData.TopologyDescriptionNode> nodes = new ArrayList<>();
            for (StreamsGroupTopologyDescription.Node n : st.nodes()) {
                nodes.add(pojoNodeToWire(n));
            }
            s.setNodes(nodes);
            subs.add(s);
        }
        out.setSubtopologies(subs);
        List<StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore> globals = new ArrayList<>();
        for (StreamsGroupTopologyDescription.GlobalStore g : topology.globalStores()) {
            StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore w =
                new StreamsGroupDescribeResponseData.TopologyDescriptionGlobalStore()
                    .setSource(pojoNodeToWire(g.source()))
                    .setProcessor(pojoNodeToWire(g.processor()));
            globals.add(w);
        }
        out.setGlobalStores(globals);
        return out;
    }

    private static StreamsGroupDescribeResponseData.TopologyDescriptionNode pojoNodeToWire(
        StreamsGroupTopologyDescription.Node node
    ) {
        StreamsGroupDescribeResponseData.TopologyDescriptionNode w =
            new StreamsGroupDescribeResponseData.TopologyDescriptionNode()
                .setName(node.name())
                .setSuccessors(new ArrayList<>(node.successors()));
        if (node instanceof StreamsGroupTopologyDescription.Source source) {
            w.setNodeType((byte) 1);
            w.setSourceTopics(new ArrayList<>(source.topics()));
        } else if (node instanceof StreamsGroupTopologyDescription.Processor processor) {
            w.setNodeType((byte) 2);
            w.setStores(new ArrayList<>(processor.stores()));
        } else if (node instanceof StreamsGroupTopologyDescription.Sink sink) {
            w.setNodeType((byte) 3);
            sink.topic().ifPresent(w::setSinkTopic);
        }
        return w;
    }

    // -----------------------------------------------------------------------------------------
    // Explicit DeleteGroups path
    // -----------------------------------------------------------------------------------------

    /**
     * Fires {@code plugin.deleteTopology} for each streams group ID with a stored topology, before
     * the actual group tombstone is written. Returns a future that completes once every per-group
     * plugin call has settled, resolving to a map from {@code groupId} to the failure cause for
     * groups whose plugin call failed. Groups not present in the map succeeded (or had no stored
     * topology). The service uses the map to skip the tombstone for failed groups and report
     * {@code DELETE_FAILED} on the per-group result, with the cause string in {@code ErrorMessage}.
     */
    public CompletableFuture<Map<String, Throwable>> deleteBeforeGroupDelete(
        Map<String, Integer> storedEpochs
    ) {
        if (storedEpochs.isEmpty() || plugin.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        StreamsGroupTopologyDescriptionPlugin p = plugin.get();
        Map<String, Throwable> failures = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> pluginFutures = new ArrayList<>(storedEpochs.size());
        for (String groupId : storedEpochs.keySet()) {
            backoff.remove(groupId);
            pluginFutures.add(
                callDeleteTopology(p, groupId).handle((__, throwable) -> {
                    if (throwable != null) {
                        Throwable cause = throwable instanceof CompletionException && throwable.getCause() != null
                            ? throwable.getCause() : throwable;
                        log.warn("Plugin deleteTopology failed for group {} during DeleteGroups; group will not be tombstoned.",
                            groupId, cause);
                        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME);
                        failures.put(groupId, cause);
                    } else {
                        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME);
                    }
                    return null;
                })
            );
        }
        return CompletableFuture.allOf(pluginFutures.toArray(new CompletableFuture<?>[0]))
            .thenApply(__ -> Map.copyOf(failures));
    }

    // The plugin SPI mandates exceptions-via-future, but a misbehaving plugin may throw synchronously;
    // wrap the call so we always get back a future even in that case.
    private static CompletableFuture<Void> callDeleteTopology(StreamsGroupTopologyDescriptionPlugin p, String groupId) {
        try {
            return p.deleteTopology(groupId);
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    // -----------------------------------------------------------------------------------------
    // Periodic cleanup cycle
    // -----------------------------------------------------------------------------------------

    private void scheduleCleanupCycle() {
        TimerTask task = new TimerTask(cleanupCheckIntervalMs) {
            @Override
            public void run() {
                if (!isActive.get()) return;
                try {
                    runCleanupCycle();
                } catch (Throwable t) {
                    log.warn("Unexpected error scheduling topology-description cleanup.", t);
                }
                if (isActive.get()) scheduleCleanupCycle();
            }
        };
        cleanupTask = task;
        timer.add(task);
    }

    private void runCleanupCycle() {
        if (plugin.isEmpty()) return;
        if (!cleanupCycleInFlight.compareAndSet(false, true)) {
            log.warn("Topology-description cleanup cycle skipped: previous cycle is still in flight.");
            return;
        }
        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME);
        StreamsGroupTopologyDescriptionPlugin p = plugin.get();
        List<CompletableFuture<Set<String>>> partitionFutures = runtime.scheduleReadAllOperation(
            "list-streams-groups-needing-topology-cleanup",
            (coordinator, lastCommittedOffset) ->
                coordinator.listStreamsGroupsNeedingTopologyCleanup(lastCommittedOffset)
        );
        Queue<CompletableFuture<?>> perGroupFutures = new ConcurrentLinkedQueue<>();
        List<CompletableFuture<Void>> partitionDoneFutures = new ArrayList<>(partitionFutures.size());
        for (CompletableFuture<Set<String>> partitionFuture : partitionFutures) {
            CompletableFuture<Void> partitionDone = partitionFuture.handle((groupIds, throwable) -> {
                if (throwable != null) {
                    log.warn("Topology-description cleanup read failed for one partition.", throwable);
                    return null;
                }
                if (groupIds == null || groupIds.isEmpty()) return null;
                metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME, groupIds.size());
                for (String groupId : groupIds) {
                    backoff.remove(groupId);
                    perGroupFutures.add(callDeleteTopology(p, groupId).handle((__, pluginEx) -> {
                        if (pluginEx != null) {
                            log.warn("Plugin deleteTopology failed for group {} during topology cleanup; will retry next cycle.",
                                groupId, pluginEx);
                            metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME);
                            return null;
                        }
                        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME);
                        clearStoredTopologyEpochAsync(groupId);
                        return null;
                    }));
                }
                return null;
            });
            partitionDoneFutures.add(partitionDone);
        }
        CompletableFuture.allOf(partitionDoneFutures.toArray(new CompletableFuture<?>[0]))
            .thenCompose(__ -> CompletableFuture.allOf(perGroupFutures.toArray(new CompletableFuture<?>[0])))
            .whenComplete((__, ___) -> cleanupCycleInFlight.set(false));
    }
}
