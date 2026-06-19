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
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupTopologyDescriptionUpdateResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescription;
import org.apache.kafka.coordinator.group.api.streams.StreamsGroupTopologyDescriptionPlugin;
import org.apache.kafka.coordinator.group.api.streams.StreamsTopologyDescriptionPermanentFailureException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Broker-level component that owns the streams-group topology description plugin
 * reference and the per-group re-solicitation back-off. The chain that drives a push
 * RPC (validate → convert → plugin → metadata write → back-off mutation) lives on
 * {@code GroupCoordinatorService}; this class exposes the building blocks (plugin
 * invocation, back-off mutations) and the heartbeat-path gate, but does not assemble
 * the chain itself.
 *
 * <p>This class is broker-level (one instance per {@code GroupCoordinatorService}); the
 * back-off map is keyed by {@code groupId} and shared across all partitions hosted on the
 * broker. State here is intentionally non-timeline and non-replayed: it is rebuilt from
 * scratch on broker restart, and the persisted {@code StoredDescriptionTopologyEpoch} /
 * {@code FailedDescriptionTopologyEpoch} fields on each streams group drive
 * convergence after a restart.
 */
public class StreamsGroupTopologyDescriptionManager implements AutoCloseable {
    private final Optional<StreamsGroupTopologyDescriptionPlugin> plugin;
    private final StreamsGroupTopologyDescriptionBackoff backoff;

    public StreamsGroupTopologyDescriptionManager(
        Optional<StreamsGroupTopologyDescriptionPlugin> plugin,
        Time time
    ) {
        this.plugin = plugin;
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
