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

import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateRequestData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.server.util.timer.Timer;
import org.apache.kafka.server.util.timer.TimerTask;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
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
import java.util.function.Supplier;

import static org.apache.kafka.common.requests.StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE;
import static org.apache.kafka.common.requests.StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_ERROR;
import static org.apache.kafka.common.requests.StreamsGroupDescribeResponse.TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED;

/**
 * Broker-level component that owns the streams-group topology description plugin reference
 * and the per-group re-solicitation back-off. The push-RPC chain (validate → convert →
 * plugin → metadata write → back-off mutation, see {@link #pushTopology}) and the periodic
 * cleanup cycle's body (list eligible groups → mark uncertain → plugin delete → finalize, see
 * {@link #runCleanupCycle}) both live here now, reading and writing group state through
 * {@link TopologyDescriptionRuntime} so the same chains run unchanged on any host that supplies
 * an implementation of that interface. {@code GroupCoordinatorService} still owns the broker
 * timer and starts the cycle via {@link #startCleanupCycle}, which wraps the supplied cycle body
 * with single-flight scheduling; it also keeps a thin same-named delegate to
 * {@link #runCleanupCycle} so existing tests that invoke it directly keep working.
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
    // All three plugin-outcome sensor groups (get/set/delete) are recorded here, next to the
    // single source of truth that classifies each outcome (applyGetTopologyOutcome, pushTopology,
    // and the delete call sites respectively).
    private final GroupCoordinatorMetrics metrics;
    private final TopologyDescriptionRuntime runtime;

    /**
     * True between {@link #startCleanupCycle} and {@link #close}. The {@link TimerTask}
     * checks this on each tick before invoking the cycle supplier, so {@code close} flips
     * the flag and the next tick refuses to fire even if {@code cancel} on the task races.
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Single-flight guard for the periodic cleanup cycle: a tick that fires while the
     * previous cycle is still settling is dropped with a warn-level log. Set true in
     * {@link #runOnce} before invoking the supplier; released by the terminal
     * {@code whenComplete} attached to the future the supplier returns.
     */
    private final AtomicBoolean cycleInFlight = new AtomicBoolean(false);

    /**
     * Guards {@link #close}'s plugin close so a repeated {@code close} call does not invoke the
     * user-supplied plugin's {@code close} more than once; {@code AutoCloseable} does not
     * require {@code close} to be idempotent.
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);

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
        GroupCoordinatorMetrics metrics,
        TopologyDescriptionRuntime runtime
    ) {
        this.log = logContext.logger(StreamsGroupTopologyDescriptionManager.class);
        this.plugin = plugin;
        this.backoff = new StreamsGroupTopologyDescriptionBackoff(time);
        this.metrics = metrics;
        this.runtime = runtime;
    }

    /**
     * Push pipeline: validate against live group state, write the UNCERTAIN barrier, invoke the
     * plugin, then record the outcome as a stored or failed epoch.
     */
    public CompletableFuture<StreamsGroupTopologyDescriptionUpdateResponseData> pushTopology(
        String groupId,
        String memberId,
        int topologyEpoch,
        StreamsGroupTopologyDescriptionUpdateRequestData.TopologyDescription topologyDescription
    ) {
        // The back-off is mutated where the disposition is known: pre-plugin failures (validate /
        // convert / runtime) never reach the arming code, so a fenced or unauthorized caller
        // cannot grief the back-off; a transient plugin failure arms it; and the post-plugin
        // bookkeeping write clears it on success, drops the whole entry if the group was deleted
        // underneath us, leaves it alone on a coordinator-moved error, or arms it (see
        // #completeEpochWrite).
        return runtime.validateSetTopology(groupId, memberId, topologyEpoch)
            .thenApply(unused -> StreamsGroupTopologyDescriptionConverter.fromRequest(topologyDescription))
            .thenCompose(description ->
                runtime.markTopologyUncertain(groupId, true)
                    .thenCompose(marked -> {
                        if (!marked) {
                            // The group vanished (or stopped being a streams group) between the
                            // validate read and the barrier write, so no UNCERTAIN barrier exists.
                            // Running the plugin op anyway would create an entry that no cleanup
                            // path ever reclaims (the cleanup scan and DeleteGroups only iterate
                            // live groups), so fail the push instead.
                            return CompletableFuture.failedFuture(
                                new GroupIdNotFoundException(String.format("Group %s not found.", groupId)));
                        }
                        return invokeSetTopology(groupId, topologyEpoch, description);
                    }))
            .thenCompose(pluginOutcome -> {
                recordPluginSetOutcome(pluginOutcome.kind());
                return switch (pluginOutcome.kind()) {
                    case SUCCESS -> runtime.setStoredTopologyEpoch(groupId, topologyEpoch)
                        .handle((unused, throwable) -> completeEpochWrite(
                            groupId, topologyEpoch, throwable,
                            new StreamsGroupTopologyDescriptionUpdateResponseData()));
                    case PERMANENT -> runtime.setFailedTopologyEpoch(groupId, topologyEpoch)
                        .handle((unused, throwable) -> completeEpochWrite(
                            groupId, topologyEpoch, throwable,
                            new StreamsGroupTopologyDescriptionUpdateResponseData()
                                .setErrorCode(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code())
                                .setErrorMessage(pluginOutcome.message())));
                    case TRANSIENT -> {
                        armBackoff(groupId, topologyEpoch);
                        yield CompletableFuture.completedFuture(new StreamsGroupTopologyDescriptionUpdateResponseData()
                            .setErrorCode(Errors.STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED.code())
                            .setErrorMessage(pluginOutcome.message()));
                    }
                };
            });
    }

    private void recordPluginSetOutcome(PluginOutcome.Kind kind) {
        String sensorName = kind == PluginOutcome.Kind.SUCCESS
            ? GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_SUCCESS_SENSOR_NAME
            : GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_SET_ERROR_SENSOR_NAME;
        metrics.recordSensor(sensorName);
    }

    /**
     * Arm the periodic cleanup cycle. The manager owns the scheduling harness — timer task,
     * single-flight guard, running flag — and fires the supplied {@code cycleSupplier} on
     * every tick; callers pass {@link #runCleanupCycle} itself, or (for existing tests) a
     * delegate to it. No-op when no plugin is configured. Must be called before
     * {@link #close}; a second call while already running logs and is otherwise a no-op.
     */
    public void startCleanupCycle(
        Timer timer,
        long cleanupCheckIntervalMs,
        Supplier<CompletableFuture<?>> cycleSupplier
    ) {
        if (plugin.isEmpty()) return;
        if (!running.compareAndSet(false, true)) {
            log.warn("Topology-description cleanup cycle is already started.");
            return;
        }
        scheduleNextTick(timer, cleanupCheckIntervalMs, cycleSupplier);
    }

    /** Stops the cleanup cycle without touching the plugin; see {@link #close}. */
    public void stopCleanupCycle() {
        if (running.compareAndSet(true, false)) {
            TimerTask snapshot = scheduledTask;
            if (snapshot != null) {
                snapshot.cancel();
            }
        }
    }

    /**
     * Stops the cleanup cycle and closes the plugin. Called by
     * {@code GroupCoordinatorService.shutdown} after {@code isActive} flips false, since request
     * handlers gate on {@code isActive} once and don't re-check it before reaching the plugin.
     */
    @Override
    public void close() throws Exception {
        stopCleanupCycle();
        if (closed.compareAndSet(false, true) && plugin.isPresent()) {
            plugin.get().close();
        }
    }

    private void scheduleNextTick(
        Timer timer,
        long cleanupCheckIntervalMs,
        Supplier<CompletableFuture<?>> cycleSupplier
    ) {
        if (!running.get()) return;
        TimerTask task = new TimerTask(cleanupCheckIntervalMs) {
            @Override
            public void run() {
                if (!running.get()) return;
                try {
                    runOnce(cycleSupplier);
                } catch (Throwable t) {
                    log.warn("Unexpected error running topology-description cleanup cycle.", t);
                }
                if (running.get()) scheduleNextTick(timer, cleanupCheckIntervalMs, cycleSupplier);
            }
        };
        scheduledTask = task;
        timer.add(task);
    }

    /**
     * Invoke {@code cycleSupplier} once under the single-flight guard: a call that fires
     * while a previous cycle is still settling its returned future is dropped with a
     * warn-level log. Released in the terminal {@code whenComplete}; a synchronous throw
     * from the supplier releases the flag before propagating so the next tick can run.
     */
    // Visible for testing.
    public void runOnce(Supplier<CompletableFuture<?>> cycleSupplier) {
        if (!cycleInFlight.compareAndSet(false, true)) {
            log.warn("Topology-description cleanup cycle skipped: previous cycle is still in flight.");
            return;
        }
        try {
            CompletableFuture<?> chain = cycleSupplier.get();
            if (chain == null) {
                cycleInFlight.set(false);
                return;
            }
            chain.whenComplete((__, throwable) -> {
                if (throwable != null) {
                    log.warn("Topology-description cleanup cycle failed to complete cleanly.", throwable);
                }
                cycleInFlight.set(false);
            });
        } catch (Throwable t) {
            cycleInFlight.set(false);
            throw t;
        }
    }

    // Visible for testing.
    TimerTask scheduledCleanupTask() {
        return scheduledTask;
    }

    // Visible for testing.
    boolean isRunning() {
        return running.get();
    }

    /**
     * @return true if a topology description plugin is configured on this broker.
     */
    public boolean isPluginConfigured() {
        return plugin.isPresent();
    }

    /**
     * One topology-description cleanup cycle: (1) reads the streams groups eligible for
     * plugin-side cleanup, one batch per locality {@link TopologyDescriptionRuntime} splits the
     * scan across; (2) writes a durable UNCERTAIN barrier for each batch, re-checking live state
     * and dropping any candidate revived since the scan; (3) calls {@code plugin.deleteTopology}
     * for the still-eligible subset; and (4) smart-finalizes the groups whose delete succeeded.
     * This is the cycle body {@link #startCleanupCycle} invokes on a timer. A direct call still
     * reads eligible batches but drops them at the {@code running} check below unless
     * {@link #startCleanupCycle} already flipped it true.
     */
    public CompletableFuture<?> runCleanupCycle() {
        if (plugin.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_CYCLE_RUNS_SENSOR_NAME);

        List<CompletableFuture<Set<String>>> batchFutures = runtime.listGroupsNeedingCleanupBatches();

        // ConcurrentLinkedQueue because per-batch .handle callbacks can append concurrently from
        // whichever thread completed each read.
        Queue<CompletableFuture<?>> perBatchFutures = new ConcurrentLinkedQueue<>();
        List<CompletableFuture<Void>> batchDoneFutures = new ArrayList<>(batchFutures.size());
        for (CompletableFuture<Set<String>> batchFuture : batchFutures) {
            batchDoneFutures.add(batchFuture.handle((eligible, throwable) -> {
                if (throwable != null) {
                    log.warn("Topology-description cleanup read failed for one batch.", throwable);
                    return null;
                }
                if (eligible == null || eligible.isEmpty()) return null;
                // Shutdown started after this read was scheduled. Skip the plugin dispatch so we
                // do not issue plugin.deleteTopology calls into a manager whose plugin is about
                // to be closed.
                if (!running.get()) {
                    log.debug("Skipping topology-description cleanup batch: cycle not running.");
                    return null;
                }
                metrics.recordSensor(
                    GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_CLEANUP_ELIGIBLE_GROUPS_SENSOR_NAME,
                    eligible.size()
                );
                perBatchFutures.add(cleanupBatch(eligible));
                return null;
            }));
        }

        return CompletableFuture.allOf(batchDoneFutures.toArray(new CompletableFuture<?>[0]))
            .thenCompose(__ -> CompletableFuture.allOf(perBatchFutures.toArray(new CompletableFuture<?>[0])));
    }

    /**
     * Drive one batch's topology cleanup for the groups the eligibility scan returned: mark the
     * UNCERTAIN(-2) barrier, run {@code plugin.deleteTopology} for the subset the mark confirmed
     * still eligible, then smart-finalize the groups whose delete succeeded. All groups in
     * {@code eligible} came from the same locality's read so one mark/finalize write covers them.
     */
    private CompletableFuture<Void> cleanupBatch(Set<String> eligible) {
        return runtime.markTopologyUncertainBatch(eligible)
            .exceptionally(throwable -> {
                // Same containment as the finalize write: one batch's routine write failure
                // (e.g. NOT_COORDINATOR during a move) must not fail the whole cycle's allOf.
                // Skipping the plugin delete is safe — the groups keep their stored epoch and
                // the next cycle retries.
                log.warn("Failed to write the UNCERTAIN barrier for groups {}; skipping their "
                    + "plugin delete. The next cleanup cycle will retry.", eligible, throwable);
                return Set.of();
            })
            .thenCompose(stillEligible -> {
                // The mark write re-checks the latest state and drops any candidate that was
                // revived or converted since the committed scan. Nothing to do for a batch
                // whose every candidate dropped out.
                if (stillEligible.isEmpty()) return CompletableFuture.completedFuture(null);
                return invokeDeleteTopologies(stillEligible)
                    .thenCompose(failures -> finalizeCleanupAfterDelete(stillEligible, failures));
            });
    }

    /**
     * Follow-up after the cleanup cycle's {@code plugin.deleteTopology}: record the outcome, drop
     * the push-path back-off for groups whose delete succeeded, and smart-finalize them. Groups
     * whose delete failed are left at {@code -2} (delete-eligible and re-soliciting) so the next
     * cycle retries while their back-off keeps throttling a rejoining member against the broken
     * plugin.
     */
    private CompletableFuture<Void> finalizeCleanupAfterDelete(Set<String> stillEligible, Map<String, ?> failures) {
        recordPluginDeleteOutcome(stillEligible.size(), failures.size());
        // Shutdown can have started between the plugin call and this follow-up write.
        if (!running.get()) return CompletableFuture.completedFuture(null);
        Set<String> toFinalize = new LinkedHashSet<>(stillEligible.size());
        for (String groupId : stillEligible) {
            if (failures.containsKey(groupId)) continue;
            clearBackoffGroup(groupId);
            toFinalize.add(groupId);
        }
        if (toFinalize.isEmpty()) return CompletableFuture.completedFuture(null);
        return finalizeAfterDelete(toFinalize);
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
            log.info("[GroupId {}] Requested topology description push at topology epoch {}.",
                groupId, currentEpoch);
        }
        return result;
    }

    /**
     * Decorates a streams group heartbeat's result with {@link #maybeSetTopologyDescriptionRequired},
     * once the underlying write completes. No-ops when no plugin is configured, without
     * scheduling the extra stage. If decoration itself fails (e.g. because of an unexpected
     * response shape), the heartbeat has already committed durably, so the failure is logged and
     * the committed result is returned unmodified rather than translated into an error; that
     * would mask a successful broker-side state change behind a client-visible failure.
     */
    public CompletableFuture<StreamsGroupHeartbeatResult> decorateHeartbeatResult(
        CompletableFuture<StreamsGroupHeartbeatResult> heartbeat,
        String groupId,
        int apiVersion,
        int memberEpoch
    ) {
        if (!isPluginConfigured()) return heartbeat;
        return heartbeat.thenApply(result -> {
            try {
                return maybeSetTopologyDescriptionRequired(result, groupId, apiVersion, memberEpoch);
            } catch (Throwable t) {
                log.warn("Failed to apply topology-description post-processing on the "
                    + "streams group heartbeat response for group {}; returning the response unmodified.",
                    groupId, t);
                return result;
            }
        });
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
     * @return true while the window armed by {@link #throttleConversionDelete} is in effect for
     *         the group, i.e. a recent classic-join conversion delete failed and the join path
     *         must not re-invoke the plugin yet.
     */
    public boolean isConversionDeleteThrottled(String groupId) {
        return backoff.isActive(groupId, StreamsGroup.STORED_TOPOLOGY_EPOCH_UNCERTAIN);
    }

    /**
     * Arm (or continue) the back-off chain that throttles the classic-join conversion delete
     * against a failing plugin. On {@code REBALANCE_IN_PROGRESS} the classic client retries the
     * join immediately (no client-side back-off), so without this window a broken plugin would
     * be hit with {@code deleteTopology} in a tight loop. Keyed at
     * {@link StreamsGroup#STORED_TOPOLOGY_EPOCH_UNCERTAIN} — the group's stored epoch after the
     * barrier write and never a real push epoch — so heartbeat/push windows are not disturbed;
     * a stale real-epoch entry left from before the group emptied is replaced. The window is
     * dropped by {@link #clearBackoffGroup} when a conversion or cleanup-cycle delete succeeds.
     */
    public void throttleConversionDelete(String groupId) {
        backoff.armIfNotActive(groupId, StreamsGroup.STORED_TOPOLOGY_EPOCH_UNCERTAIN);
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
     * <p>Pure plugin invocation: does not read group state, does not touch the back-off
     * map, and does not record metrics. The service layer pre-filters the input, records
     * delete-success / delete-error sensors on the returned failure count, and is
     * responsible for invoking {@link #clearBackoffGroup} for the groups it chose to clear.
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

    /**
     * Drive the topology-description plugin's pre-delete flow for an explicit {@code DeleteGroups}
     * request: identify which of {@code groupIds} carry a stored topology description, mark them
     * uncertain, call {@code plugin.deleteTopology} for each, finalize the successful deletes, and
     * drop their back-off entries. Short-circuits with an empty failure map when no plugin is
     * configured.
     */
    public CompletableFuture<Map<String, ApiError>> deleteTopologiesForDeleteGroups(List<String> groupIds) {
        if (plugin.isEmpty() || groupIds.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }
        return runtime.groupsWithStoredTopologyDescription(groupIds)
            .thenCompose(groupsWithStored -> {
                // Common case: the batch holds no streams groups with stored topology. Skip the
                // mark entirely instead of scheduling a no-op write on the shard's event loop.
                if (groupsWithStored.isEmpty()) {
                    return CompletableFuture.<Map<String, ApiError>>completedFuture(Map.of());
                }
                return runtime.markTopologyUncertainBatch(groupsWithStored)
                    .thenCompose(marked ->
                        invokeDeleteTopologies(marked)
                            .thenCompose(failures -> {
                                recordPluginDeleteOutcome(marked.size(), failures.size());
                                Set<String> succeeded = new LinkedHashSet<>(marked);
                                succeeded.removeAll(failures.keySet());
                                // Clear the push-path back-off only for groups whose plugin delete
                                // succeeded (the group is about to be tombstoned), mirroring the
                                // cleanup cycle. A failed delete leaves the group at UNCERTAIN(-2),
                                // which re-solicits a push on the next heartbeat — keep its back-off
                                // so a rejoining member does not immediately hit the still-broken
                                // plugin, and let the interval-throttled cleanup cycle retry it.
                                succeeded.forEach(this::clearBackoffGroup);
                                if (succeeded.isEmpty()) {
                                    return CompletableFuture.completedFuture(failures);
                                }
                                // Smart-finalize the successful deletes before the tombstone,
                                // mirroring the cleanup cycle: a group revived between the mark
                                // and the plugin delete survives the tombstone (NON_EMPTY_GROUP),
                                // and without the finalize a raced push's epoch write would land
                                // on stored == UNCERTAIN and record a real epoch over the plugin
                                // this delete just emptied. For groups the tombstone does remove
                                // the extra record is harmless.
                                return finalizeAfterDelete(succeeded)
                                    .thenApply(__ -> failures);
                            }));
            });
    }

    /**
     * Building block for callers driving a single-group barrier write outside the push and
     * cleanup pipelines (the classic-group-join conversion path).
     */
    public CompletableFuture<Boolean> markTopologyUncertain(String groupId, boolean markWhenNone) {
        return runtime.markTopologyUncertain(groupId, markWhenNone);
    }

    /**
     * Batched smart-finalize write after a plugin delete. Per group: if stored is still
     * UNCERTAIN no push raced and it is cleared to NONE (the next sweep tombstones it); if
     * stored advanced past UNCERTAIN a push raced our delete and it is forced back to UNCERTAIN
     * to re-solicit. All groups in the batch share one locality. Runtime write failures
     * (NOT_COORDINATOR etc.) are logged here and swallowed so a single failed write does not
     * poison the cycle's allOf — the next cycle retries because the persisted storedEpoch is
     * still non-default.
     */
    public CompletableFuture<Void> finalizeAfterDelete(Set<String> groupIds) {
        return runtime.finalizeAfterDeleteBatch(groupIds)
            .exceptionally(throwable -> {
                log.warn("Failed to finalize StoredDescriptionTopologyEpoch for groups {}; "
                    + "the next cleanup cycle will retry.", groupIds, throwable);
                return null;
            });
    }

    /** Building block mirroring {@link #markTopologyUncertain} for the post-delete finalize write. */
    public CompletableFuture<Void> finalizeAfterDelete(String groupId) {
        return finalizeAfterDelete(Set.of(groupId));
    }

    /**
     * Record per-call outcomes from a {@code plugin.deleteTopology} invocation against the
     * shared {@code delete-success} / {@code delete-error} sensors. Used by
     * {@link #deleteTopologiesForDeleteGroups}, {@link #runCleanupCycle}, and the
     * classic-group-join conversion path, so a single pair of meters tracks every
     * {@code plugin.deleteTopology} the broker drives, regardless of trigger.
     */
    public void recordPluginDeleteOutcome(int attempted, int errors) {
        int successes = attempted - errors;
        if (successes > 0) {
            metrics.recordSensor(
                GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_SUCCESS_SENSOR_NAME, successes);
        }
        if (errors > 0) {
            metrics.recordSensor(
                GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_DELETE_ERROR_SENSOR_NAME, errors);
        }
    }

    /**
     * Populate {@code TopologyDescription} and {@code TopologyDescriptionStatus} on each
     * {@code DescribedGroup} carried by {@code result}, calling {@code plugin.getTopology} only
     * for groups whose persisted {@code StoredDescriptionTopologyEpoch} matches the group's
     * current topology epoch. Returns a future that completes when every per-group plugin call
     * has settled; the future never completes exceptionally — per-group errors fold into
     * {@code TOPOLOGY_DESCRIPTION_STATUS_ERROR} on the corresponding describedGroup.
     *
     * <p>Per-group status decisions:
     * <ul>
     *   <li>{@code errorCode != NONE} — the group could not be resolved; leave the status
     *       field at its {@code NOT_REQUESTED} default since the client should consult the
     *       group's error code first.</li>
     *   <li>No plugin configured on this broker — every successful group becomes
     *       {@code NOT_STORED}: from the client's perspective the broker simply has no
     *       description to serve.</li>
     *   <li>{@code Topology} field is null on the response (group has not yet declared a
     *       topology), {@code storedEpoch} is missing or {@code -1}, or {@code storedEpoch}
     *       does not match {@code topology().epoch()} — {@code NOT_STORED}.</li>
     *   <li>Plugin call returns null (the plugin no longer has the data, e.g. backend wipe)
     *       — {@code NOT_STORED}.</li>
     *   <li>Plugin call completes exceptionally, throws synchronously, or returns a null
     *       future (SPI contract violation) — {@code ERROR}. Conversion failures from the
     *       returned POJO to the wire schema also fold into {@code ERROR} so a single
     *       malformed plugin response cannot poison the rest of the batch.</li>
     *   <li>Plugin call returns a non-null description — {@code AVAILABLE}, with the
     *       converted topology attached.</li>
     * </ul>
     */
    public CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> attachTopologyDescriptions(
        StreamsGroupDescribeResult result
    ) {
        if (plugin.isEmpty()) {
            for (StreamsGroupDescribeResponseData.DescribedGroup describedGroup : result.describedGroups()) {
                if (describedGroup.errorCode() == Errors.NONE.code()) {
                    describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
                }
            }
            return CompletableFuture.completedFuture(result.describedGroups());
        }
        final StreamsGroupTopologyDescriptionPlugin topologyDescriptionPlugin = plugin.get();
        List<CompletableFuture<Void>> pluginFutures = new ArrayList<>();
        for (StreamsGroupDescribeResponseData.DescribedGroup describedGroup : result.describedGroups()) {
            CompletableFuture<Void> outcome = maybeAttachOne(topologyDescriptionPlugin, describedGroup, result);
            if (outcome != null) pluginFutures.add(outcome);
        }
        if (pluginFutures.isEmpty()) {
            return CompletableFuture.completedFuture(result.describedGroups());
        }
        return CompletableFuture.allOf(pluginFutures.toArray(new CompletableFuture<?>[0]))
            .thenApply(unused -> result.describedGroups());
    }

    /**
     * Inspect one describedGroup and, if eligible, fire {@code plugin.getTopology}. Returns
     * null when no plugin call is needed (status has already been decided synchronously from
     * the response shape); otherwise returns a future that completes once the plugin call has
     * been folded into the describedGroup's status / topology fields.
     */
    private CompletableFuture<Void> maybeAttachOne(
        StreamsGroupTopologyDescriptionPlugin p,
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup,
        StreamsGroupDescribeResult result
    ) {
        if (describedGroup.errorCode() != Errors.NONE.code()) {
            return null;
        }
        if (describedGroup.topology() == null) {
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
            return null;
        }
        Integer storedEpoch = result.storedDescriptionTopologyEpochs().get(describedGroup.groupId());
        if (storedEpoch == null
            || !StreamsGroup.isReliablyStoredTopologyEpoch(storedEpoch)
            || storedEpoch != describedGroup.topology().epoch()) {
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
            return null;
        }
        int topologyEpoch = describedGroup.topology().epoch();
        CompletableFuture<StreamsGroupTopologyDescription> pluginFuture;
        try {
            pluginFuture = p.getTopology(describedGroup.groupId(), topologyEpoch);
        } catch (Exception e) {
            // SPI contract violation: synchronous throw treated as ERROR.
            recordGetError();
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_ERROR);
            return CompletableFuture.completedFuture(null);
        }
        if (pluginFuture == null) {
            recordGetError();
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_ERROR);
            return CompletableFuture.completedFuture(null);
        }
        return pluginFuture.handle((topology, throwable) -> {
            applyGetTopologyOutcome(describedGroup, topology, throwable);
            return null;
        });
    }

    private void applyGetTopologyOutcome(
        StreamsGroupDescribeResponseData.DescribedGroup describedGroup,
        StreamsGroupTopologyDescription topology,
        Throwable throwable
    ) {
        if (throwable != null) {
            Throwable cause = Errors.maybeUnwrapException(throwable);
            log.warn("Topology description plugin getTopology failed for group {}.",
                describedGroup.groupId(), cause != null ? cause : throwable);
            recordGetError();
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_ERROR);
            return;
        }
        // The plugin call itself completed normally. A null return is the documented
        // "plugin no longer has the data" path and surfaces as NOT_STORED, not an error,
        // so it still counts as a successful getTopology.
        if (topology == null) {
            recordGetSuccess();
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_NOT_STORED);
            return;
        }
        try {
            describedGroup.setTopologyDescription(
                StreamsGroupTopologyDescriptionConverter.toDescribeResponse(topology));
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_AVAILABLE);
            recordGetSuccess();
        } catch (Exception conversionError) {
            // Defensive catch, should be unreachable in practice. The outcome surfaces as
            // ERROR to the client, so count it as a failed getTopology to stay consistent.
            recordGetError();
            describedGroup.setTopologyDescription(null);
            describedGroup.setTopologyDescriptionStatus(TOPOLOGY_DESCRIPTION_STATUS_ERROR);
        }
    }

    private void recordGetSuccess() {
        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_SUCCESS_SENSOR_NAME);
    }

    private void recordGetError() {
        metrics.recordSensor(GroupCoordinatorMetrics.STREAMS_GROUP_TOPOLOGY_DESCRIPTION_GET_ERROR_SENSOR_NAME);
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
