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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.RebalanceListenerInvocationCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.RebalanceListenerInvocationNeededEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;

import static org.apache.kafka.clients.consumer.internals.RebalanceStep.ASSIGN;
import static org.apache.kafka.clients.consumer.internals.RebalanceStep.LOSE;
import static org.apache.kafka.clients.consumer.internals.RebalanceStep.REVOKE;

/**
 * {@code AssignmentReconciler} performs the work of reconciling this consumer's partition assignment as directed
 * by the consumer group coordinator. When the coordinator determines that a change to the partition ownership of
 * the group is required, it will communicate with each consumer to relay its respective <em>target</em>
 * assignment, that is, the set of partitions for which that consumer should now assume ownership. It is the then the
 * responsibility of the consumer to work toward that target by performing the necessary internal modifications to
 * satisfy the assignment from the coordinator. In practical terms, this means that it must first determine the set
 * difference between the <em>{@link SubscriptionState#assignedPartitions() current assignment}</em> and the
 * <em>{@link Assignment#assignedTopicPartitions() target assignment}</em>.
 *
 * <p/>
 *
 * Internally, reconciliation requires the following steps:
 *
 * <ol>
 *     <li>
 *         On the background thread, upon receipt of a new assignment from the group coordinator, the heartbeat
 *         request manager should first call {@link #maybeRevoke(Optional)} to start revocation
 *     </li>
 *     <li>
 *         Internally, the partitions to revoke are determined via {@link #getPartitionsToRevoke(Optional)};
 *         these are the partitions in the current assignment that are <em>not in</em> the target assignment
 *     </li>
 *     <li>
 *         Send a {@link RebalanceListenerInvocationNeededEvent} so that the application thread will execute the
 *         {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} callback method
 *     </li>
 *     <li>
 *         On the application thread, when the {@link RebalanceListenerInvocationNeededEvent} is received, execute the
 *         {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} callback method
 *     </li>
 *     <li>
 *         Enqueue a corresponding {@link RebalanceListenerInvocationCompletedEvent} so that the background thread
 *         will know the listener was invoked and the result of the invocation
 *     </li>
 *     <li>
 *         On the background thread, process the {@link RebalanceListenerInvocationCompletedEvent}, which should
 *         call the {@link #postOnRevokedPartitions(Set, Optional)} method to remove the revoked partitions from the
 *         {@link SubscriptionState#assignFromSubscribed(Collection) current assignment}
 *     </li>
 *     <li>
 *         Next, we calculate the partitions to assign using {@link #getPartitionsToAssign(Optional)};
 *         these are the partitions in the target assignment that are <em>not in</em> the current assignment
 *     </li>
 *     <li>
 *         Send a {@link RebalanceListenerInvocationNeededEvent} so that the application thread will execute the
 *         {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} callback method
 *     </li>
 *     <li>
 *         Enqueue a corresponding {@link RebalanceListenerInvocationCompletedEvent} so that the background thread
 *         will know the listener was invoked and the result of that invocation
 *     </li>
 *     <li>
 *         On the background thread, process the {@link RebalanceListenerInvocationCompletedEvent}, which should
 *         call the {@link #postOnAssignedPartitions(Set, Optional)} method to add the assigned partitions to the
 *         {@link SubscriptionState#assignFromSubscribed(Collection) current assignment}
 *     </li>
 *     <li>
 *         Signal to the heartbeat request manager to perform a heartbeat acknowledgement with the group coordinator
 *     </li>
 * </ol>
 *
 * <p/>
 *
 * The comparison against the {@link SubscriptionState#assignedPartitions() current set of assigned partitions} and
 * the {@link Assignment#assignedTopicPartitions() target set of assigned partitions} is performed by essentially
 * <em>flattening</em> the respective entries into two sets of {@link TopicPartition partitions}
 * which are then compared using basic {@link Set} comparisons.
 */
public class AssignmentReconciler {

    private final static String ON_PARTITIONS_LOST_METHOD_NAME = String.format("%s.onPartitionsLost()", ConsumerRebalanceListener.class.getSimpleName());
    private final static String ON_PARTITIONS_REVOKED_METHOD_NAME = String.format("%s.onPartitionsRevoked()", ConsumerRebalanceListener.class.getSimpleName());
    private final static String ON_PARTITIONS_ASSIGNED_METHOD_NAME = String.format("%s.onPartitionsAssigned()", ConsumerRebalanceListener.class.getSimpleName());
    private final Logger log;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    AssignmentReconciler(LogContext logContext,
                         SubscriptionState subscriptions,
                         ConsumerMetadata metadata,
                         BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.backgroundEventQueue = backgroundEventQueue;
    }

    boolean maybeLose() {
        SortedSet<TopicPartition> partitionsToLose = getPartitionsToLose();

        if (partitionsToLose.isEmpty()) {
            log.debug("Skipping invocation of {} as no partitions were assigned", ON_PARTITIONS_LOST_METHOD_NAME);
            return false;
        }

        log.debug("Enqueuing event to invoke {} with the following partitions: {}",
                ON_PARTITIONS_LOST_METHOD_NAME,
                partitionsToLose);
        backgroundEventQueue.add(new RebalanceListenerInvocationNeededEvent(LOSE, partitionsToLose));
        return true;
    }

    /**
     * Determine which partitions should be "lost". This is simply the
     * {@link SubscriptionState#assignedPartitions() current set} of {@link TopicPartition partitions}.
     *
     * @return Set of partitions to "lose"
     */
    SortedSet<TopicPartition> getPartitionsToLose() {
        SortedSet<TopicPartition> partitions = new TreeSet<>(new TopicPartitionComparator());
        partitions.addAll(subscriptions.assignedPartitions());
        return partitions;
    }

    void postOnLosePartitions(Set<TopicPartition> partitions, Optional<Exception> error) {
        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("An error occurred when invoking {} with the following partitions: {}",
                    ON_PARTITIONS_LOST_METHOD_NAME,
                    partitions,
                    e);
        } else {
            log.debug("{} was successfully executed with the following partitions: {}",
                    ON_PARTITIONS_LOST_METHOD_NAME,
                    partitions);
        }

        // And finally... drop all partitions, but just in case something was added in the meantime.
        Set<TopicPartition> newAssignment = new HashSet<>(subscriptions.assignedPartitions());
        newAssignment.removeAll(partitions);
        subscriptions.assignFromSubscribed(newAssignment);
    }

    boolean maybeRevoke(Optional<Assignment> assignment) {
        SortedSet<TopicPartition> partitionsToRevoke = getPartitionsToRevoke(assignment);

        if (partitionsToRevoke.isEmpty()) {
            log.debug("Skipping invocation of {} as there are no partitions to revoke in the new assignment",
                    ON_PARTITIONS_REVOKED_METHOD_NAME);
            return false;
        }

        log.debug("Enqueuing event to invoke {} with the following partitions: {}",
                ON_PARTITIONS_REVOKED_METHOD_NAME,
                partitionsToRevoke);
        backgroundEventQueue.add(new RebalanceListenerInvocationNeededEvent(REVOKE, partitionsToRevoke));
        return true;
    }

    /**
     * Determine which partitions are newly revoked. This is done by comparing the
     * {@link Assignment#assignedTopicPartitions() target set from the assignment} against the
     * {@link SubscriptionState#assignedPartitions() current set}. The returned set of
     * {@link TopicPartition partitions} are composed of any partitions that are in the current set but
     * are no longer in the target set.
     *
     * @param assignment {@link Optional} that holds the {@link Assignment} which includes the target set of topics
     * @return Set of partitions to revoke
     */

    SortedSet<TopicPartition> getPartitionsToRevoke(Optional<Assignment> assignment) {
        SortedSet<TopicPartition> partitions = new TreeSet<>(new TopicPartitionComparator());
        partitions.addAll(subscriptions.assignedPartitions());
        partitions.removeAll(targetPartitions(assignment));
        return partitions;
    }

    void postOnRevokedPartitions(Set<TopicPartition> partitions, Optional<Exception> error) {
        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("An error occurred when invoking {} with the following partitions: {}",
                    ON_PARTITIONS_REVOKED_METHOD_NAME,
                    partitions,
                    e);
        } else {
            log.debug("{} was successfully executed with the following partitions: {}",
                    ON_PARTITIONS_REVOKED_METHOD_NAME,
                    partitions);
        }

        // Modify the current assignment by removing all the partitions that no longer appear in our assignment.
        Set<TopicPartition> newAssignment = new HashSet<>(subscriptions.assignedPartitions());
        newAssignment.removeAll(partitions);
        subscriptions.assignFromSubscribed(newAssignment);
    }

    /**
     * Performs the assignment phase of the reconciliation process:
     *
     * <ul>
     *     <li>
     *         On the background thread, determine which partitions are newly assigned
     *         via {@link #getPartitionsToAssign(Optional)}
     *     </li>
     *     <li>
     *         If the set of newly assigned partitions is non-empty, enqueue an {@link RebalanceListenerInvocationNeededEvent}
     *         on the event queue for the application thread
     *     </li>
     *     <li>
     *         As part of the next call to {@link Consumer#poll(Duration)} on the application thread,
     *         the presence of the above event in the queue will cause the
     *         {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} method
     *         to be invoked
     *     </li>
     *     <li>
     *         The result of the above {@link ConsumerRebalanceListener} method—success or failure—will then
     *         be communicated to the background thread via an {@link RebalanceListenerInvocationCompletedEvent}
     *     </li>
     *     <li>
     *         On the background thread, when the {@link RebalanceListenerInvocationCompletedEvent} is processed,
     *         call into the {@link #postOnAssignedPartitions(Set, Optional)} method to update the
     *         {@link SubscriptionState#assignFromSubscribed(Collection) set of assigned topics}
     *     </li>
     * </ul>
     *
     * @param assignment {@link Optional} that holds the {@link Assignment} which includes the target set of topics
     */

    boolean maybeAssign(Optional<Assignment> assignment) {
        SortedSet<TopicPartition> partitionsToAssign = getPartitionsToAssign(assignment);

        if (partitionsToAssign.isEmpty()) {
            log.debug("Skipping invocation of {} as no partitions were assigned in the new assignment",
                    ON_PARTITIONS_ASSIGNED_METHOD_NAME);
            return false;
        }

        log.debug("Enqueuing event to invoke {} to assign the following partitions: {}",
                ON_PARTITIONS_ASSIGNED_METHOD_NAME,
                partitionsToAssign);
        backgroundEventQueue.add(new RebalanceListenerInvocationNeededEvent(ASSIGN, partitionsToAssign));
        return true;
    }

    /**
     * Determine which partitions are newly assigned. This is done by comparing the
     * {@link Assignment#assignedTopicPartitions() target set from the assignment} against the
     * {@link SubscriptionState#assignedPartitions() current set}. Any {@link TopicPartition partitions} from the
     * target set that are not already in the current set are included in the returned set.
     *
     * @param assignment {@link Optional} that holds the {@link Assignment} which includes the target set of topics
     * @return Set of partitions to assign
     */

    SortedSet<TopicPartition> getPartitionsToAssign(Optional<Assignment> assignment) {
        SortedSet<TopicPartition> partitions = new TreeSet<>(new TopicPartitionComparator());
        partitions.addAll(targetPartitions(assignment));
        partitions.removeAll(subscriptions.assignedPartitions());
        return partitions;
    }

    void postOnAssignedPartitions(Set<TopicPartition> partitions, Optional<Exception> error) {
        if (error.isPresent()) {
            Exception e = error.get();
            log.warn("An error occurred when invoking {} with the following partitions: {}",
                    ON_PARTITIONS_ASSIGNED_METHOD_NAME,
                    partitions,
                    e);
        } else {
            log.debug("{} was successfully executed with the following partitions: {}",
                    ON_PARTITIONS_ASSIGNED_METHOD_NAME,
                    partitions);
        }

        // Modify the current assignment by adding all the partitions that are were newly assigned.
        Set<TopicPartition> newAssignment = new HashSet<>();
        newAssignment.addAll(subscriptions.assignedPartitions());
        newAssignment.addAll(partitions);
        subscriptions.assignFromSubscribed(newAssignment);
    }

    private SortedSet<TopicPartition> targetPartitions(Optional<Assignment> assignment) {
        Map<Uuid, String> topicIdToNameMap = new HashMap<>();

        for (Map.Entry<String, Uuid> entry : metadata.topicIds().entrySet())
            topicIdToNameMap.put(entry.getValue(), entry.getKey());

        SortedSet<TopicPartition> set = new TreeSet<>(new TopicPartitionComparator());

        assignment.ifPresent(a -> {
            for (TopicPartitions topicPartitions : a.assignedTopicPartitions()) {
                Uuid id = topicPartitions.topicId();
                String topic = Objects.requireNonNull(topicIdToNameMap.get(id), () -> String.format("No topic name was found in the metadata for topic ID %s", id));

                for (Integer partition : topicPartitions.partitions()) {
                    set.add(new TopicPartition(topic, partition));
                }
            }
        });

        return set;
    }
}
