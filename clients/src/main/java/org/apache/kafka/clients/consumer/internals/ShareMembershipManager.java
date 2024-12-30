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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.metrics.ShareRebalanceMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ShareGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ShareGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Group manager for a single consumer that has a group id defined in the config
 * {@link ConsumerConfig#GROUP_ID_CONFIG} and the share group protocol to get automatically
 * assigned partitions when calling the subscribe API.
 * <p/>
 *
 * While the subscribe API hasn't been called (or if the consumer called unsubscribe), this manager
 * will only be responsible for keeping the member in the {@link MemberState#UNSUBSCRIBED} state,
 * without joining the group.
 * <p/>
 *
 * If the consumer subscribe API is called, this manager will use the {@link #groupId()} to join the
 * share group, and based on the share group protocol heartbeats, will handle the full
 * lifecycle of the member as it joins the group, reconciles assignments, handles fatal errors,
 * and leaves the group.
 * <p/>
 *
 * Reconciliation process:<p/>
 * The member accepts all assignments received from the broker, resolves topic names from
 * metadata, reconciles the resolved assignments, and keeps the unresolved to be reconciled when
 * discovered with a metadata update. Reconciliations of resolved assignments are executed
 * sequentially and acknowledged to the server as they complete. The reconciliation process
 * involves multiple async operations, so the member will continue to heartbeat while these
 * operations complete, to make sure that the member stays in the group while reconciling.
 * <p/>
 *
 * Reconciliation steps:
 * <ol>
 *     <li>Resolve topic names for all topic IDs received in the target assignment. Topic names
 *     found in metadata are then ready to be reconciled. Topic IDs not found are kept as
 *     unresolved, and the member request metadata updates until it resolves them (or the broker
 *     removes it from the target assignment).</li>
 *     <li>When the above steps complete, the member acknowledges the reconciled assignment,
 *     which is the subset of the target that was resolved from metadata and actually reconciled.
 *     The ack is performed by sending a heartbeat request back to the broker.</li>
 * </ol>
 *
 */
public class ShareMembershipManager extends AbstractMembershipManager<ShareGroupHeartbeatResponse> {

    /**
     * Rack ID of the member, if specified.
     */
    protected final String rackId;

    public ShareMembershipManager(LogContext logContext,
                                  String groupId,
                                  String rackId,
                                  SubscriptionState subscriptions,
                                  ConsumerMetadata metadata,
                                  Time time,
                                  Metrics metrics) {
        this(logContext,
                groupId,
                rackId,
                subscriptions,
                metadata,
                time,
                new ShareRebalanceMetricsManager(metrics));
    }

    // Visible for testing
    ShareMembershipManager(LogContext logContext,
                           String groupId,
                           String rackId,
                           SubscriptionState subscriptions,
                           ConsumerMetadata metadata,
                           Time time,
                           ShareRebalanceMetricsManager metricsManager) {
        super(groupId,
                subscriptions,
                metadata,
                logContext.logger(ShareMembershipManager.class),
                time,
                metricsManager);
        this.rackId = rackId;
    }

    /**
     * @return The rack ID of the member, if specified.
     */
    public String rackId() {
        return rackId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeartbeatSuccess(ShareGroupHeartbeatResponse response) {
        ShareGroupHeartbeatResponseData responseData = response.data();
        if (responseData.errorCode() != Errors.NONE.code()) {
            String errorMessage = String.format(
                    "Unexpected error in Heartbeat response. Expected no error, but received: %s",
                    Errors.forCode(responseData.errorCode())
            );
            throw new IllegalArgumentException(errorMessage);
        }
        MemberState state = state();
        if (state == MemberState.LEAVING) {
            log.debug("Ignoring heartbeat response received from broker. Member {} with epoch {} is " +
                    "already leaving the group.", memberId, memberEpoch);
            return;
        }
        if (state == MemberState.UNSUBSCRIBED && maybeCompleteLeaveInProgress()) {
            log.debug("Member {} with epoch {} received a successful response to the heartbeat " +
                    "to leave the group and completed the leave operation. ", memberId, memberEpoch);
            return;
        }
        if (isNotInGroup()) {
            log.debug("Ignoring heartbeat response received from broker. Member {} is in {} state" +
                    " so it's not a member of the group. ", memberId, state);
            return;
        }

        updateMemberEpoch(responseData.memberEpoch());

        ShareGroupHeartbeatResponseData.Assignment assignment = responseData.assignment();

        if (assignment != null) {
            if (!state.canHandleNewAssignment()) {
                // New assignment received but member is in a state where it cannot take new
                // assignments (ex. preparing to leave the group)
                log.debug("Ignoring new assignment {} received from server because member is in {} state.",
                        assignment, state);
                return;
            }

            Map<Uuid, SortedSet<Integer>> newAssignment = new HashMap<>();
            assignment.topicPartitions().forEach(topicPartition -> newAssignment.put(topicPartition.topicId(), new TreeSet<>(topicPartition.partitions())));
            processAssignmentReceived(newAssignment);
        }
    }

    @Override
    public int joinGroupEpoch() {
        return ShareGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH;
    }

    @Override
    public int leaveGroupEpoch() {
        return ShareGroupHeartbeatRequest.LEAVE_GROUP_MEMBER_EPOCH;
    }
}
