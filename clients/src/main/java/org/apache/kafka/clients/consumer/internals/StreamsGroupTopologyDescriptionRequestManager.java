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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.message.UpdateStreamsGroupTopologyDescriptionRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.UpdateStreamsGroupTopologyDescriptionRequest;
import org.apache.kafka.common.requests.UpdateStreamsGroupTopologyDescriptionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.slf4j.Logger;

import java.util.Collections;
import java.util.Objects;

import static org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult.EMPTY;

/**
 * Sends {@code UpdateStreamsGroupTopologyDescription} requests to the group coordinator when the
 * broker signals (via {@code TopologyDescriptionRequired=true} in a heartbeat response) that it
 * needs the topology description for this group.
 *
 * <p>This manager is polled by the consumer background thread. On each poll it checks
 * {@link StreamsRebalanceData#topologyDescriptionRequired()} and, if a request is not already
 * in-flight, sends one to the coordinator. Retries are driven by the heartbeat cycle: on failure
 * the flag is cleared and the broker will re-request on the next heartbeat.
 */
public class StreamsGroupTopologyDescriptionRequestManager implements RequestManager {

    private final Logger log;
    private final Time time;
    private final CoordinatorRequestManager coordinatorRequestManager;
    private final StreamsMembershipManager membershipManager;
    private final StreamsRebalanceData streamsRebalanceData;
    private final String groupId;

    // Prevents sending a second request while one is already in-flight.
    private boolean requestInFlight = false;

    // Earliest time at which the next request may be sent; advanced by ThrottleTimeMs from the broker.
    private long nextSendTimeMs = 0L;

    public StreamsGroupTopologyDescriptionRequestManager(
            final LogContext logContext,
            final Time time,
            final CoordinatorRequestManager coordinatorRequestManager,
            final StreamsMembershipManager membershipManager,
            final StreamsRebalanceData streamsRebalanceData,
            final String groupId) {
        this.log = logContext.logger(StreamsGroupTopologyDescriptionRequestManager.class);
        this.time = Objects.requireNonNull(time, "Time cannot be null");
        this.coordinatorRequestManager = Objects.requireNonNull(coordinatorRequestManager,
            "Coordinator request manager cannot be null");
        this.membershipManager = Objects.requireNonNull(membershipManager,
            "Streams membership manager cannot be null");
        this.streamsRebalanceData = Objects.requireNonNull(streamsRebalanceData,
            "Streams rebalance data cannot be null");
        this.groupId = Objects.requireNonNull(groupId, "Group ID cannot be null");
    }

    @Override
    public NetworkClientDelegate.PollResult poll(final long currentTimeMs) {
        if (!streamsRebalanceData.topologyDescriptionRequired()) {
            return EMPTY;
        }
        if (streamsRebalanceData.topologyDescription().isEmpty()) {
            // Topology description push not enabled (no description provided at startup).
            return EMPTY;
        }
        if (requestInFlight) {
            return EMPTY;
        }
        if (currentTimeMs < nextSendTimeMs) {
            return EMPTY;
        }
        if (coordinatorRequestManager.coordinator().isEmpty()) {
            return EMPTY;
        }
        final String memberId = membershipManager.memberId();
        if (memberId == null || memberId.isEmpty()) {
            // No memberId yet — first heartbeat hasn't returned. Wait.
            return EMPTY;
        }

        final int topologyEpoch = streamsRebalanceData.topologyEpoch();
        final UpdateStreamsGroupTopologyDescriptionRequestData requestData =
            new UpdateStreamsGroupTopologyDescriptionRequestData()
                .setGroupId(groupId)
                .setMemberId(memberId)
                .setTopologyEpoch(topologyEpoch)
                .setTopologyDescription(streamsRebalanceData.topologyDescription().get());

        final NetworkClientDelegate.UnsentRequest request = new NetworkClientDelegate.UnsentRequest(
            new UpdateStreamsGroupTopologyDescriptionRequest.Builder(requestData),
            coordinatorRequestManager.coordinator()
        ).whenComplete((response, exception) -> {
            requestInFlight = false;
            if (exception != null) {
                log.warn("Failed to send topology description to coordinator, will retry on next heartbeat", exception);
                // Flag stays — retry driven by broker re-requesting on next heartbeat.
            } else {
                onResponse((UpdateStreamsGroupTopologyDescriptionResponse) response.responseBody(), time.milliseconds());
            }
        });

        log.info("Sending topology description to coordinator for group {} at epoch {}", groupId, topologyEpoch);
        requestInFlight = true;
        return new NetworkClientDelegate.PollResult(Collections.singletonList(request));
    }

    private void onResponse(final UpdateStreamsGroupTopologyDescriptionResponse response, final long currentTimeMs) {
        final Errors error = Errors.forCode(response.data().errorCode());
        final int throttleTimeMs = response.data().throttleTimeMs();
        if (throttleTimeMs > 0) {
            nextSendTimeMs = currentTimeMs + throttleTimeMs;
        }

        switch (error) {
            case NONE:
                log.info("Successfully sent topology description to coordinator for group {}", groupId);
                streamsRebalanceData.setTopologyDescriptionRequired(false);
                break;

            case STREAMS_TOPOLOGY_DESCRIPTION_TOO_LARGE:
                log.warn("Topology description is too large for the plugin");
                streamsRebalanceData.setTopologyDescriptionRequired(false);
                break;

            case STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED:
                // The broker treats this as transient and will re-solicit via a future heartbeat
                // once its back-off elapses. Clearing the flag here just stops the local retry loop.
                log.warn("Topology description push failed transiently; broker will re-request on a later heartbeat: {}",
                    response.data().errorMessage());
                streamsRebalanceData.setTopologyDescriptionRequired(false);
                break;

            case NOT_COORDINATOR:
            case COORDINATOR_NOT_AVAILABLE:
                log.debug("UpdateStreamsGroupTopologyDescription failed with {}, rediscovering coordinator", error);
                coordinatorRequestManager.markCoordinatorUnknown(response.data().errorMessage(), currentTimeMs);
                // Flag stays — retry after coordinator is rediscovered.
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                log.debug("UpdateStreamsGroupTopologyDescription failed with {}, will retry on next heartbeat", error);
                // Flag stays — retry driven by broker re-requesting on next heartbeat.
                break;

            case UNKNOWN_MEMBER_ID:
                // Group was deleted or this member is no longer in it. The membership manager
                // detects the fence on the next heartbeat and triggers a clean rejoin; clearing
                // the local flag here prevents another push at the (now-fenced) member id.
                log.warn("UpdateStreamsGroupTopologyDescription was fenced (group deleted or member dropped): {}",
                    response.data().errorMessage());
                streamsRebalanceData.setTopologyDescriptionRequired(false);
                break;

            default:
                log.warn("UpdateStreamsGroupTopologyDescription failed with unexpected error {}: {}",
                    error, response.data().errorMessage());
                streamsRebalanceData.setTopologyDescriptionRequired(false);
                break;
        }
    }
}
