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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentLostStartedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentChangedCallbacksInvokedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionAssignmentChangedStartedEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;

/**
 * {@code AssignmentReconciler} performs the work of reconciling this consumer's partition assignment as directed
 * by the consumer group coordinator. When the coordinator determines that a change to the partition ownership of
 * the group is required, it will communicate with each consumer to relay its respective <em>target</em>
 * assignment, that is, the set of partitions for which that consumer should now assume ownership. It is the then the
 * responsibility of the consumer to work toward that target by performing the necessary internal modifications to
 * satisfy the assignment from the coordinator. In practical terms, this means that it must first determine the set
 * difference between the <em>{@link SubscriptionState#assignedPartitions() current assignment}</em> and the
 * <em>{@link Assignment#topicPartitions() target assignment}</em>.
 *
 * <p/>
 *
 * Internally, reconciliation requires the following steps:
 *
 * <ol>
 *     <li>
 *         On the background thread, upon receipt of a new assignment from the group coordinator, the heartbeat
 *         request manager should first call {@link #reconcile(Assignment)} to start revocation
 *     </li>
 *     <li>
 *         Internally, the partitions to revoke are determined via {@link #getPartitionsToRevoke(Assignment)};
 *         these are the partitions in the current assignment that are <em>not in</em> the target assignment.
 *         Next, we calculate the partitions to assign using {@link #getPartitionsToAssign(Assignment)};
 *         these are the partitions in the target assignment that are <em>not in</em> the current assignment.
 *     </li>
 *     <li>
 *         Send a {@link PartitionAssignmentChangedStartedEvent} so that the application thread will execute the
 *         {@link ConsumerRebalanceListener} callback methods
 *     </li>
 *     <li>
 *         On the application thread, when the {@link PartitionAssignmentChangedStartedEvent} is received, execute the
 *         {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} and
 *         {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} callback methods
 *     </li>
 *     <li>
 *         Enqueue a corresponding {@link PartitionAssignmentChangedCallbacksInvokedEvent} so that the background thread
 *         will know the listener was invoked and the result of the invocation
 *     </li>
 *     <li>
 *         On the background thread, process the {@link PartitionAssignmentChangedCallbacksInvokedEvent}, which should
 *         call the {@link HeartbeatRequestManager#partitionAssignmentChangedCallbacksInvoked(Set, Set, Optional)}
 *         method. This method will call the {@link #reconciliationCallbacksInvoked(Set, Set)} method to remove the
 *         revoked partitions and add the assigned partitions in the
 *         {@link SubscriptionState#assignFromSubscribed(Collection) current assignment} and then make a note to
 *         send a {@link ConsumerGroupHeartbeatRequest} to the group coordinator on its next pass of
 *         {@link HeartbeatRequestManager#poll(long)}
 *     </li>
 * </ol>
 *
 * <p/>
 *
 * The comparison against the {@link SubscriptionState#assignedPartitions() current set of assigned partitions} and
 * the {@link Assignment#topicPartitions() target set of assigned partitions} is performed by essentially
 * <em>flattening</em> the respective entries into two sets of {@link TopicPartition partitions}
 * which are then compared using basic {@link Set} comparisons.
 */
public class AssignmentReconciler {

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

    void lost() {
        SortedSet<TopicPartition> partitionsToLose = getPartitionsToLose();

        if (partitionsToLose.isEmpty()) {
            log.debug("Skipping invocation of {} callbacks as no partitions changed in the new assignment",
                    ConsumerRebalanceListener.class.getSimpleName());
            return;
        }

        log.debug("Enqueuing event to invoke {} callbacks", ConsumerRebalanceListener.class.getSimpleName());
        backgroundEventQueue.add(new PartitionAssignmentLostStartedEvent(partitionsToLose));
    }

    void lostCallbackInvoked() {
        log.debug("{} callbacks were successfully invoked", ConsumerRebalanceListener.class.getSimpleName());
        subscriptions.assignFromSubscribed(Collections.emptySet());
    }

    /**
     * Performs the assignment phase of the reconciliation process as described in the top-level class documentation.
     *
     * @param assignment Holds the {@link Assignment} which includes the target set of topics
     */

    void reconcile(Assignment assignment) {
        SortedSet<TopicPartition> partitionsToRevoke = getPartitionsToRevoke(assignment);
        SortedSet<TopicPartition> partitionsToAssign = getPartitionsToAssign(assignment);

        if (partitionsToRevoke.isEmpty() && partitionsToAssign.isEmpty()) {
            log.debug("Skipping invocation of {} callbacks as no partitions changed in the new assignment",
                    ConsumerRebalanceListener.class.getSimpleName());
            return;
        }

        log.debug("Enqueuing event to invoke {} callbacks", ConsumerRebalanceListener.class.getSimpleName());
        PartitionAssignmentChangedStartedEvent event = new PartitionAssignmentChangedStartedEvent(
                partitionsToRevoke,
                partitionsToAssign
        );
        backgroundEventQueue.add(event);
    }

    void reconciliationCallbacksInvoked(Set<TopicPartition> revokedPartitions,
                                        Set<TopicPartition> assignedPartitions) {
        log.debug("{} callbacks were successfully invoked", ConsumerRebalanceListener.class.getSimpleName());

        Set<TopicPartition> newAssignment = new HashSet<>(subscriptions.assignedPartitions());
        newAssignment.addAll(assignedPartitions);
        newAssignment.removeAll(revokedPartitions);
        subscriptions.assignFromSubscribed(newAssignment);
    }

    /**
     * Determine which partitions are newly revoked. This is done by comparing the
     * {@link Assignment#topicPartitions() target set from the assignment} against the
     * {@link SubscriptionState#assignedPartitions() current set}. The returned set of
     * {@link TopicPartition partitions} are composed of any partitions that are in the current set but
     * are no longer in the target set.
     *
     * @param assignment Holds the {@link Assignment} which includes the target set of topics
     * @return Set of partitions to revoke
     */

    SortedSet<TopicPartition> getPartitionsToRevoke(Assignment assignment) {
        SortedSet<TopicPartition> partitions = new TreeSet<>(new TopicPartitionComparator());
        partitions.addAll(subscriptions.assignedPartitions());
        partitions.removeAll(targetPartitions(assignment));
        return partitions;
    }

    /**
     * Determine which partitions are newly assigned. This is done by comparing the
     * {@link Assignment#topicPartitions() target set from the assignment} against the
     * {@link SubscriptionState#assignedPartitions() current set}. Any {@link TopicPartition partitions} from the
     * target set that are not already in the current set are included in the returned set.
     *
     * @param assignment {@link Optional} that holds the {@link Assignment} which includes the target set of topics
     * @return Set of partitions to assign
     */

    SortedSet<TopicPartition> getPartitionsToAssign(Assignment assignment) {
        SortedSet<TopicPartition> partitions = new TreeSet<>(new TopicPartitionComparator());
        partitions.addAll(targetPartitions(assignment));
        partitions.removeAll(subscriptions.assignedPartitions());
        return partitions;
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

    private SortedSet<TopicPartition> targetPartitions(Assignment assignment) {
        Map<Uuid, String> topicIdToNameMap = uuidStringMap();

        SortedSet<TopicPartition> set = new TreeSet<>(new TopicPartitionComparator());

        for (TopicPartitions topicPartitions : assignment.topicPartitions()) {
            Uuid topicId = topicPartitions.topicId();
            String topicName = Objects.requireNonNull(topicIdToNameMap.get(topicId), () -> String.format("No topic name was found in the metadata for topic ID %s", topicId));

            for (Integer partition : topicPartitions.partitions()) {
                set.add(new TopicPartition(topicName, partition));
            }
        }

        return set;
    }

    private Map<Uuid, String> uuidStringMap() {
        Map<Uuid, String> topicIdToNameMap = new HashMap<>();

        for (Map.Entry<String, Uuid> entry : metadata.topicIds().entrySet())
            topicIdToNameMap.put(entry.getValue(), entry.getKey());
        return topicIdToNameMap;
    }
}
