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
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/**
 * Broker-level component that owns the streams-group topology description plugin reference,
 * the per-group re-solicitation back-off, and the periodic plugin-row cleanup cycle for
 * naturally-expired streams groups. The push-RPC chain (validate → convert → plugin →
 * metadata write → back-off mutation) lives on {@code GroupCoordinatorService}, which calls
 * this class's building blocks ({@link #invokeSetTopology}, {@link #completeEpochWrite},
 * {@link #armBackoff}, etc.); the cleanup cycle is fully self-contained here and only
 * needs to be started by the service via {@link #startCleanupCycle}.
 *
 * <p>This class is broker-level (one instance per {@code GroupCoordinatorService}); the
 * back-off map is keyed by {@code groupId} and shared across all partitions hosted on the
 * broker. State here is intentionally non-timeline and non-replayed: it is rebuilt from
 * scratch on broker restart, and the persisted {@code StoredDescriptionTopologyEpoch} /
 * {@code FailedDescriptionTopologyEpoch} fields on each streams group drive
 * convergence after a restart.
 */
public class StreamsGroupTopologyDescriptionManager implements AutoCloseable {
    private final Logger log;
    private final Optional<StreamsGroupTopologyDescriptionPlugin> plugin;
    private final StreamsGroupTopologyDescriptionBackoff backoff;

    private final Timer timer;
    private final long cleanupCheckIntervalMs;
    private final Function<String, TopicPartition> topicPartitionFor;
    private final GroupCoordinatorMetrics groupCoordinatorMetrics;

    /**
     * True between {@link #startCleanupCycle} and {@link #close}. Read at every cycle
     * boundary that would otherwise schedule new plugin calls or runtime writes, so a
     * cycle that is in flight when {@code close} fires drains rather than racing the
     * runtime tear-down.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Single-flight guard for the periodic cleanup cycle: a tick that fires while the
     * previous cycle is still settling per-group plugin calls and conditional-clear writes
     * is dropped. Set true at the top of {@link #runCleanupCycle}, released by the
     * terminal {@code whenComplete} that joins all per-partition reads and per-group
     * futures.
     */
    private final AtomicBoolean cycleInFlight = new AtomicBoolean(false);

    /**
     * The currently-scheduled cleanup tick on the broker-level {@link Timer}.
     * Self-rescheduled inside the {@link TimerTask}'s {@code run}; {@link #close} cancels
     * this snapshot and the task's own re-arm check observes {@code running == false}
     * so the next tick does not re-schedule itself.
     */
    private volatile TimerTask scheduledTask;

    public StreamsGroupTopologyDescriptionManager(
        LogContext logContext,
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        Time time,
        Timer timer,
        long cleanupCheckIntervalMs,
        Function<String, TopicPartition> topicPartitionFor,
        GroupCoordinatorMetrics groupCoordinatorMetrics
    ) {
        this.log = logContext.logger(StreamsGroupTopologyDescriptionManager.class);
        this.plugin = plugin;
        this.backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        this.timer = timer;
        this.cleanupCheckIntervalMs = cleanupCheckIntervalMs;
        this.topicPartitionFor = topicPartitionFor;
        this.groupCoordinatorMetrics = groupCoordinatorMetrics;
    }

    /**
     * Arm the periodic cleanup cycle against the supplied runtime. Called by
     * {@code GroupCoordinatorService.startup} once the coordinator is active; no-op when
     * no plugin is configured. Must be called before {@link #close}; a second call while
     * already running logs and is otherwise a no-op.
     *
     * <p>The runtime is captured by the self-rescheduling timer task's lambda rather
     * than stored as a field, so the manager does not retain a runtime reference past
     * {@link #close}.
     */
    public void startCleanupCycle(CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime) {
        if (plugin.isEmpty()) return;
        if (!running.compareAndSet(false, true)) {
            log.warn("Topology-description cleanup cycle is already started.");
            return;
        }
        scheduleNextTick(runtime);
    }

    /**
     * Stop the cleanup cycle and release plugin-side resources. Flips {@code running}
     * false (so any in-flight cycle skips its remaining boundary checks and stops
     * scheduling new work against the runtime), cancels the currently-scheduled tick,
     * and closes the plugin. Called by {@code GroupCoordinatorService.shutdown} before
     * the runtime is closed, so writes scheduled before the flip drain through their
     * own futures rather than racing the runtime tear-down.
     */
    @Override
    public void close() throws Exception {
        if (running.compareAndSet(true, false)) {
            TimerTask snapshot = scheduledTask;
            if (snapshot != null) {
                snapshot.cancel();
            }
        }
        if (plugin.isPresent()) {
            plugin.get().close();
        }
    }

    /**
     * Schedule the next cleanup tick on the broker-level {@link Timer}. The {@link TimerTask}
     * self-reschedules from inside its own {@code run} so the cycle keeps firing every
     * {@code cleanupCheckIntervalMs} until {@link #close} flips {@code running} false.
     */
    private void scheduleNextTick(CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime) {
        if (!running.get()) return;
        TimerTask task = new TimerTask(cleanupCheckIntervalMs) {
            @Override
            public void run() {
                if (!running.get()) return;
                try {
                    runCleanupCycle(runtime);
                } catch (Throwable t) {
                    log.warn("Unexpected error running topology-description cleanup cycle.", t);
                }
                if (running.get()) scheduleNextTick(runtime);
            }
        };
        scheduledTask = task;
        timer.add(task);
    }

    /**
     * Drive one topology-description cleanup cycle: read every shard for streams groups
     * eligible for plugin-side cleanup (empty + all offsets expired + storedEpoch != -1), call
     * {@code plugin.deleteTopology} for each, then for every group whose plugin call succeeded
     * write a conditional metadata record that clears {@code StoredDescriptionTopologyEpoch}
     * only if the persisted value still matches the epoch we observed at scan time (so a
     * concurrent {@code setTopology} that has advanced the field is preserved). Failed plugin
     * calls retry on the next cycle; the next sweep then tombstones the now-empty group.
     *
     * <p>Single-flight: a cycle that fires while a previous one is still settling per-group
     * futures is dropped with a warn-level log.
     *
     * <p><b>Concurrent setTopology race vs plugin.deleteTopology.</b> {@code plugin.deleteTopology}
     * is keyed only on {@code groupId}. If a new member joins between the
     * eligibility scan and the cycle's plugin call and pushes a fresh topology, the plugin's
     * row is removed regardless of the new epoch — the conditional clear above no-ops on the
     * metadata side, but the plugin-side data the member just wrote is gone. A subsequent
     * {@code describe} → {@code getTopology} returns null and surfaces {@code NOT_STORED} with
     * a warn log; this is the graceful-degradation path accepted under the label
     * "plugin-side data loss". The {@code isEmpty} requirement on the scan keeps the window
     * narrow — concurrent setTopology requires a member to join an empty, fully-expired group
     * between scan and delete — and the next heartbeat at the same epoch will not re-solicit
     * (storedEpoch in metadata still reflects the new push), so the group converges on
     * NOT_STORED without churn rather than chasing the lost plugin row.
     */
    // Visible for testing.
    public void runCleanupCycle(CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime) {
        if (plugin.isEmpty()) return;
        if (!cycleInFlight.compareAndSet(false, true)) {
            log.warn("Topology-description cleanup cycle skipped: previous cycle is still in flight.");
            return;
        }
        // Any synchronous throw between this point and the moment the terminal whenComplete
        // is attached would leave the in-flight flag stuck at true forever (the outer timer
        // task catches and reschedules, but every subsequent tick would short-circuit at the
        // CAS above and never tombstone deferred streams groups). Wrap chain construction so
        // a synchronous failure releases the flag before propagating.
        try {
            groupCoordinatorMetrics.recordSensor(
                GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME);

            List<CompletableFuture<Map<String, Integer>>> partitionFutures = runtime.scheduleReadAllOperation(
                "list-streams-groups-needing-topology-cleanup",
                GroupCoordinatorShard::listStreamsGroupsNeedingTopologyCleanup
            );

            // ConcurrentLinkedQueue because per-partition .handle callbacks can append concurrently
            // from whichever thread completed each runtime read.
            Queue<CompletableFuture<?>> perGroupFutures = new ConcurrentLinkedQueue<>();
            List<CompletableFuture<Void>> partitionDoneFutures = new ArrayList<>(partitionFutures.size());
            for (CompletableFuture<Map<String, Integer>> partitionFuture : partitionFutures) {
                partitionDoneFutures.add(partitionFuture.handle((eligible, throwable) -> {
                    if (throwable != null) {
                        log.warn("Topology-description cleanup read failed for one partition.", throwable);
                        return null;
                    }
                    if (eligible == null || eligible.isEmpty()) return null;
                    // Shutdown started after the per-partition read was scheduled. Skip the
                    // plugin dispatch so we do not issue plugin.deleteTopology calls into a
                    // manager whose plugin is about to be closed; existing in-flight calls
                    // continue to drain via their own futures.
                    if (!running.get()) return null;
                    groupCoordinatorMetrics.recordSensor(
                        GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME,
                        eligible.size()
                    );
                    perGroupFutures.add(invokeDeleteTopologies(eligible.keySet())
                        .thenCompose(failures -> {
                            recordPluginDeleteOutcome(eligible.size(), failures.size());
                            // Shutdown can have started between the plugin call and the
                            // follow-up writes. Skip the conditional clears so we do not
                            // schedule writes against a runtime that is being closed; the
                            // next cycle on a fresh broker incarnation will pick the state
                            // up from the persisted storedDescriptionTopologyEpoch.
                            if (!running.get()) return CompletableFuture.completedFuture(null);
                            List<CompletableFuture<Void>> clearFutures = new ArrayList<>(eligible.size());
                            eligible.forEach((groupId, expectedStoredEpoch) -> {
                                if (failures.containsKey(groupId)) {
                                    // Plugin failed: leave both stored epoch and the push-path
                                    // back-off in place. Eligibility's "group is empty" snapshot
                                    // only held at scan time; a member can rejoin between scan
                                    // and now, and the existing back-off correctly throttles their
                                    // set-topology attempt against the still-broken plugin
                                    // instead of letting it re-attack at attempts=0 every join.
                                    return;
                                }
                                // Plugin succeeded; the group will be tombstoned in the next sweep
                                // once the stored epoch is cleared. Drop the broker-wide back-off
                                // entry — it is no longer load-bearing for any future state of
                                // this groupId. A member that re-creates the same id afterwards
                                // is a fresh lifecycle and will arm a fresh back-off chain.
                                clearBackoffGroup(groupId);
                                clearFutures.add(clearStoredDescriptionTopologyEpochAsync(runtime, groupId, expectedStoredEpoch));
                            });
                            return CompletableFuture.allOf(clearFutures.toArray(new CompletableFuture<?>[0]));
                        }));
                    return null;
                }));
            }

            CompletableFuture.allOf(partitionDoneFutures.toArray(new CompletableFuture<?>[0]))
                .thenCompose(__ -> CompletableFuture.allOf(perGroupFutures.toArray(new CompletableFuture<?>[0])))
                .whenComplete((__, throwable) -> {
                    if (throwable != null) {
                        log.warn("Topology-description cleanup cycle failed to complete cleanly.", throwable);
                    }
                    cycleInFlight.set(false);
                });
        } catch (Throwable t) {
            // Release the single-flight flag synchronously so the next tick can run. Rethrow
            // so the outer timer-task's catch logs the cause and reschedules — same observable
            // result as the async failure path, just on the construction side of the chain.
            cycleInFlight.set(false);
            throw t;
        }
    }

    private void recordPluginDeleteOutcome(int attempted, int errors) {
        int successes = attempted - errors;
        if (successes > 0) {
            groupCoordinatorMetrics.recordSensor(
                GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME, successes);
        }
        if (errors > 0) {
            groupCoordinatorMetrics.recordSensor(
                GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME, errors);
        }
    }

    /**
     * Conditional metadata write that clears {@code StoredDescriptionTopologyEpoch} for
     * {@code groupId} only when the persisted value still equals {@code expectedStoredEpoch}.
     * Mismatches and missing groups are silently ignored by the shard-side method. The
     * returned future is what the cleanup cycle's single-flight guard awaits before releasing
     * the in-flight flag; runtime write failures (NOT_COORDINATOR etc.) are logged here and
     * swallowed so a single failed write does not poison the cycle's allOf — the next cycle
     * will retry naturally because the persisted storedEpoch is still non-default.
     */
    private CompletableFuture<Void> clearStoredDescriptionTopologyEpochAsync(
        CoordinatorRuntime<GroupCoordinatorShard, CoordinatorRecord> runtime,
        String groupId,
        int expectedStoredEpoch
    ) {
        return runtime.scheduleWriteOperation(
            "clear-stored-topology-epoch",
            topicPartitionFor.apply(groupId),
            coordinator -> coordinator.clearStoredDescriptionTopologyEpoch(groupId, expectedStoredEpoch)
        ).handle((__, throwable) -> {
            if (throwable != null) {
                log.warn("Failed to clear StoredDescriptionTopologyEpoch for group {}; the next cleanup cycle will retry.",
                    groupId, throwable);
            }
            return null;
        });
    }

    // Visible for testing.
    TimerTask scheduledCleanupTask() {
        return scheduledTask;
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
        int apiVersion,
        int memberEpoch
    ) {
        // Do not solicit a push from a departing member (a leave heartbeat carries a negative
        // member epoch): arming the back-off on its behalf would only delay solicitation for the
        // rest of the group.
        if (apiVersion < 1 || plugin.isEmpty() || memberEpoch < 0) {
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
     * Call {@code plugin.setTopology} and fold the result into a {@link PluginOutcome}.
     * The returned future never completes exceptionally — the outcome carries the
     * failure category so the caller can dispatch on it without try/catch on the
     * future. A synchronous throw from the plugin (which violates the SPI contract) is
     * mapped to a permanent failure with a generic message rather than forwarding the
     * raw exception text, and a {@code null} returned future is treated the same way.
     */
    public CompletableFuture<PluginOutcome> invokeSetTopology(
        String groupId,
        int topologyEpoch,
        StreamsGroupTopologyDescription description
    ) {
        if (plugin.isEmpty()) {
            return CompletableFuture.completedFuture(
                PluginOutcome.permanent("Topology description plugin failed."));
        }
        final CompletableFuture<Void> pluginFuture;
        try {
            pluginFuture = Objects.requireNonNull(
                plugin.get().setTopology(groupId, topologyEpoch, description));
        } catch (Exception e) {
            // A synchronous throw violates the SPI contract — implementations must signal
            // failures by completing the future exceptionally. Treat it as a permanent
            // failure with a stable, generic client-visible message so we don't forward
            // an unbounded or null exception message that could leak plugin internals.
            return CompletableFuture.completedFuture(
                PluginOutcome.permanent("Topology description plugin failed."));
        }
        return pluginFuture.handle((unused, throwable) -> {
            if (throwable == null) {
                return PluginOutcome.success();
            }
            // CompletionException / ExecutionException can legally carry a null cause; if a
            // plugin completes its future with one of those (rare but legal),
            // maybeUnwrapException returns null. Treat that as a transient failure with a
            // generic message rather than NPE-ing inside this handle and losing the
            // transient/permanent classification downstream.
            Throwable cause = Errors.maybeUnwrapException(throwable);
            if (cause == null) {
                return PluginOutcome.transientFailure("Plugin failure (no cause).");
            }
            if (cause instanceof StreamsTopologyDescriptionPermanentFailureException) {
                return PluginOutcome.permanent(cause.getMessage());
            }
            return PluginOutcome.transientFailure(cause.getMessage());
        });
    }

    /**
     * Arm or extend the back-off window for a group at the given topology epoch.
     * Delegates to {@link StreamsGroupTopologyDescriptionBackoff#armOrExtend}.
     */
    public void armBackoff(String groupId, int topologyEpoch) {
        backoff.armOrExtend(groupId, topologyEpoch);
    }

    /**
     * Settle the per-group back-off after the bookkeeping write that records a push outcome
     * (the stored epoch on success, the failed epoch on a permanent failure) completes, and
     * return the response to send to the client — or rethrow the write failure so the service's
     * terminal handler maps it.
     *
     * <p>On a clean write the back-off is cleared for this epoch (epoch-scoped so a late callback
     * at an old epoch cannot wipe a window a concurrent heartbeat armed at the advanced epoch);
     * if the group was deleted underneath the write its whole entry is dropped; a coordinator-moved
     * error leaves the back-off untouched (the new coordinator owns convergence once the client
     * retries); any other failure arms it so the next heartbeat re-solicits.
     */
    public StreamsGroupTopologyDescriptionUpdateResponseData completeEpochWrite(
        String groupId,
        int topologyEpoch,
        Throwable writeException,
        StreamsGroupTopologyDescriptionUpdateResponseData responseOnCommit
    ) {
        if (writeException == null) {
            backoff.clear(groupId, topologyEpoch);
            return responseOnCommit;
        }
        Throwable cause = Errors.maybeUnwrapException(writeException);
        if (cause instanceof GroupIdNotFoundException) {
            backoff.clearGroup(groupId);
        } else if (cause instanceof NotCoordinatorException
            || cause instanceof CoordinatorLoadInProgressException
            || cause instanceof CoordinatorNotAvailableException) {
            // Coordinator moved between the plugin call and the write; the new coordinator owns
            // convergence after the client retries, so leave the back-off alone.
        } else {
            backoff.armOrExtend(groupId, topologyEpoch);
        }
        throw new CompletionException(writeException);
    }

    /**
     * Drop the back-off entry for a group unconditionally. Currently called when a group is
     * removed via explicit DeleteGroups and on a post-plugin write failing with
     * GroupIdNotFoundException. NOTE: groups removed by other lifecycle paths (session expiry,
     * partition unload, tombstone-via-replay) are not yet wired to this, so their back-off
     * entries can leak until the group id is reused. Delegates to
     * {@link StreamsGroupTopologyDescriptionBackoff#clearGroup}.
     */
    public void clearBackoffGroup(String groupId) {
        backoff.clearGroup(groupId);
    }

    /**
     * Call {@code plugin.deleteTopology} for every supplied group id. Returns a per-group
     * map of failures keyed by group id; groups absent from the map either had no plugin
     * configured or the plugin call succeeded. The returned future never completes
     * exceptionally — failures are folded into the map so the service-level
     * {@code DeleteGroups} flow can dispatch on the per-group outcome without try/catch
     * on the underlying future. A synchronous throw from the plugin (which violates the
     * SPI contract) is mapped to the same {@code GROUP_DELETION_FAILED} as an
     * exceptional future.
     *
     * <p>Pure plugin invocation: does not read group state and does not touch the
     * back-off map. The service layer pre-filters the input via
     * {@code streamsGroupsWithStoredTopologyDescription} and is responsible for invoking
     * {@link #clearBackoffGroup} for the groups that were attempted.
     */
    public CompletableFuture<Map<String, ApiError>> invokeDeleteTopologies(Set<String> groupIds) {
        if (plugin.isEmpty() || groupIds.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        final StreamsGroupTopologyDescriptionPlugin p = plugin.get();
        List<CompletableFuture<Map.Entry<String, ApiError>>> outcomes = new ArrayList<>(groupIds.size());
        for (String groupId : groupIds) {
            CompletableFuture<Map.Entry<String, ApiError>> outcome;
            try {
                outcome = p.deleteTopology(groupId).handle((unused, throwable) -> toFailureEntry(groupId, throwable));
            } catch (Exception e) {
                // Synchronous throw from the plugin violates the SPI contract; treat it as
                // any other per-group failure so the failures map carries it back to the
                // caller without dropping the rest of the batch.
                outcome = CompletableFuture.completedFuture(toFailureEntry(groupId, e));
            }
            outcomes.add(outcome);
        }
        CompletableFuture<?>[] all = outcomes.toArray(new CompletableFuture<?>[0]);
        return CompletableFuture.allOf(all).thenApply(unused -> {
            Map<String, ApiError> failures = new HashMap<>();
            for (CompletableFuture<Map.Entry<String, ApiError>> future : outcomes) {
                Map.Entry<String, ApiError> entry = future.join();
                if (entry != null) {
                    failures.put(entry.getKey(), entry.getValue());
                }
            }
            return failures;
        });
    }

    private static Map.Entry<String, ApiError> toFailureEntry(String groupId, Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        // Do not forward the plugin's raw exception message to the client: it can be null and
        // may leak plugin internals (the ErrorMessage is serialized at DeleteGroups v3+). Use a
        // fixed generic message, mirroring invokeSetTopology.
        return Map.entry(groupId, new ApiError(Errors.GROUP_DELETION_FAILED,
            "Topology description plugin failed to delete the topology."));
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

    /**
     * Outcome of a {@code plugin.setTopology} call, folded into a value so the caller can
     * dispatch on {@link Kind} without try/catch on the underlying future.
     */
    public record PluginOutcome(Kind kind, String message) {

        public enum Kind { SUCCESS, PERMANENT, TRANSIENT }

        public static PluginOutcome success() {
            return new PluginOutcome(Kind.SUCCESS, null);
        }

        public static PluginOutcome permanent(String message) {
            return new PluginOutcome(Kind.PERMANENT, message);
        }

        public static PluginOutcome transientFailure(String message) {
            return new PluginOutcome(Kind.TRANSIENT, message);
        }
    }
}
