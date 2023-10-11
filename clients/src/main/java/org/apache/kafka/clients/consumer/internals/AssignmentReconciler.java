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
import org.apache.kafka.clients.consumer.internals.events.AssignPartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.LosePartitionsEvent;
import org.apache.kafka.clients.consumer.internals.events.RebalanceCallbackEvent;
import org.apache.kafka.clients.consumer.internals.events.RevokePartitionsEvent;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.Assignment;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData.TopicPartitions;
import org.apache.kafka.clients.consumer.internals.Utils.TopicPartitionComparator;
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
import java.util.stream.Collectors;

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
 *         Calculate the partitions to revoke; these are the partitions in the current assignment that are
 *         <em>not in</em> the target assignment
 *     </li>
 *     <li>
 *         Send a {@link RevokePartitionsEvent} so that the application thread will execute the
 *         {@link ConsumerRebalanceListener#onPartitionsRevoked(Collection)} callback method
 *     </li>
 *     <li>
 *         On confirmation of the callback method execution, remove the revoked partitions from the
 *         {@link SubscriptionState#assignFromSubscribed(Collection) current assignment}
 *     </li>
 *     <li>
 *         Signal to the heartbeat request manager to perform a heartbeat acknowledgement with the group coordinator
 *     </li>
 *     <li>
 *         Calculate the partitions to add; these are the partitions in the target assignment that are
 *         <em>not in</em> the current assignment
 *     </li>
 *     <li>
 *         Send an {@link AssignPartitionsEvent} so that the application thread will execute the
 *         {@link ConsumerRebalanceListener#onPartitionsAssigned(Collection)} callback method
 *     </li>
 *     <li>
 *         On confirmation of the callback method execution, add the assigned partitions to the
 *         {@link SubscriptionState#assignFromSubscribed(Collection) current assignment}
 *     </li>
 *     <li>
 *         Signal to the heartbeat request manager to perform a heartbeat acknowledgement with the group coordinator
 *     </li>
 * </ol>
 *
 * <p/>
 *
 * Because the target assignment from the group coordinator is <em>declarative</em>, the implementation of the
 * reconciliation process is idempotent. The caller of this class is free to invoke {@link #maybeReconcile(Optional)}
 * repeatedly for as long as the group coordinator provides an {@link Assignment}.
 *
 * <ul>
 *     <li>
 *         {@link ReconciliationResult#UNCHANGED UNCHANGED}: no changes were made to the set of partitions.
 *     </li>
 *     <li>
 *         {@link ReconciliationResult#RECONCILING RECONCILING}: changes to the assignment have started. In practice
 *         this means that the appropriate {@link ConsumerRebalanceListener} callback method is being invoked.
 *     </li>
 *     <li>
 *         {@link ReconciliationResult#APPLIED_LOCALLY APPLIED_LOCALLY}: the {@link ConsumerRebalanceListener} callback
 *         method was made and the changes were applied locally. The heartbeat manager should acknowledge this to the
 *         group coordinator.
 *     </li>
 * </ul>
 *
 * The comparison against the {@link SubscriptionState#assignedPartitions() current set of assigned partitions} and
 * the {@link Assignment#assignedTopicPartitions() target set of assigned partitions} is performed by essentially
 * <em>flattening</em> the respective entries into two sets of {@link TopicPartition partitions}
 * which are then compared using basic {@link Set} comparisons.
 */
public class AssignmentReconciler {

    /**
     * The result of the {@link #revoke(Optional)} or {@link #assign(Optional)} methods being invoked.
     */
    enum ReconciliationResult {
        UNCHANGED,
        RECONCILING,
        APPLIED_LOCALLY
    }

    private final Logger log;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private Optional<RebalanceCallbackEvent> inflightCallback;

    AssignmentReconciler(LogContext logContext,
                         SubscriptionState subscriptions,
                         ConsumerMetadata metadata,
                         BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.log = logContext.logger(getClass());
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.backgroundEventQueue = backgroundEventQueue;
        this.inflightCallback = Optional.empty();
    }

    /**
     * Perform the reconciliation process, as necessary to meet the given {@link Assignment target assignment}. Note
     * that the reconciliation takes multiple steps, and this method should be invoked on each heartbeat if
     * the coordinator provides a {@link Assignment target assignment}.
     *
     * @param assignment Target {@link Assignment}
     * @return {@link ReconciliationResult}
     */
    ReconciliationResult maybeReconcile(Optional<Assignment> assignment) {
        // Check for any outstanding operations first. If a conclusive result has already been reached, return that
        // before processing any further.
        if (inflightCallback.isPresent()) {
            // We don't actually need the _result_ of the event, just to know that it's complete.
            if (inflightCallback.get().future().isDone()) {
                // This is the happy path--we completed the callback. Clear out our inflight callback first, though.
                inflightCallback = Optional.empty();
                return ReconciliationResult.APPLIED_LOCALLY;
            } else {
                // So we have a callback event out there, but it's neither complete nor expired, so wait.
                return ReconciliationResult.RECONCILING;
            }
        }

        // First, we need to determine if any partitions need to be revoked.
        switch (revoke(assignment)) {
            case APPLIED_LOCALLY:
                // If we've successfully revoked the partitions locally, we need to send an acknowledgement request
                // ASAP to let the coordinator know.
                log.debug("The revocation step of partition reconciliation has completed locally; will inform the consumer group coordinator");
                return ReconciliationResult.APPLIED_LOCALLY;

            case RECONCILING:
                // At this point, we've started the revocation, but it isn't complete, so there's nothing
                // to do here but wait until it's finished or expires.
                log.debug("The revocation step of partition reconciliation is in progress");
                return ReconciliationResult.RECONCILING;

            case UNCHANGED:
                log.debug("The revocation step of partition reconciliation has no changes; it was previously completed");
                break;
        }

        // Next, we need to determine the partitions to be added, if any.
        switch (assign(assignment)) {
            case APPLIED_LOCALLY:
                // If we've assigned one or more partitions, we need to send an acknowledgement request ASAP to
                // let the coordinator know that they've been added locally.
                log.debug("The assignment step of partition reconciliation has completed locally; will inform the consumer group coordinator");
                return ReconciliationResult.APPLIED_LOCALLY;

            case RECONCILING:
                // At this point, we've started the assignment, but it isn't complete, so we wait...
                log.debug("The assignment step of partition reconciliation is in progress");
                return ReconciliationResult.RECONCILING;

            case UNCHANGED:
                log.debug("The assignment step of partition reconciliation has no changes; it was previously completed");
                break;
        }

        log.debug("Both revocation and assignment steps of partition reconciliation are complete");
        return ReconciliationResult.UNCHANGED;
    }

    ReconciliationResult lose() {
        // Clear the inflight callback reference. This is done regardless of if one existed; if there was one it is
        // now abandoned because we're going to "lose" our partitions. This will also allow us to skip the inflight
        // check the other steps take.
        inflightCallback = Optional.empty();

        // For the "lose" operation, our partition diff is simply all the partitions in our current assignment.
        SetDifferenceGenerator setDifferenceGenerator = (assigned, target) -> sortPartitions(assigned);

        // For lose, we modify the "current" assignment by removing all its entries, full stop. We can thus ignore
        // the diff-ed partitions that are passed in.
        AssignmentGenerator assignmentGenerator = __ -> Collections.emptySortedSet();

        return reconcile(Optional.empty(),
                String.format("%s.onPartitionsLost()", ConsumerRebalanceListener.class.getSimpleName()),
                setDifferenceGenerator,
                LosePartitionsEvent::new,
                assignmentGenerator);
    }

    private ReconciliationResult revoke(Optional<Assignment> assignment) {
        // For the revocation step, our partition diff is calculated by filtering out any partitions from our current
        // assignment that aren't in the given target assignment.
        SetDifferenceGenerator setDifferenceGenerator = (assigned, target) -> sortPartitions(assigned
                .stream()
                .filter(tp -> !target.contains(tp))
                .collect(Collectors.toSet()));

        // For revocation, we modify the "current" assignment by removing all entries that were not present in the
        // "target" assignment.
        AssignmentGenerator modifier = diffPartitions -> {
            Set<TopicPartition> newAssignment = new HashSet<>(subscriptions.assignedPartitions());
            newAssignment.removeAll(diffPartitions);
            return sortPartitions(newAssignment);
        };

        return reconcile(assignment,
                String.format("%s.onPartitionsRevoked()", ConsumerRebalanceListener.class.getSimpleName()),
                setDifferenceGenerator,
                RevokePartitionsEvent::new,
                modifier);
    }

    private ReconciliationResult assign(Optional<Assignment> assignment) {
        // For the assignment step, our partition diff is calculated by filtering out any partitions from the given
        // target assignment that aren't presently in our current assignment.
        SetDifferenceGenerator setDifferenceGenerator = (assigned, target) -> sortPartitions(target
                .stream()
                .filter(tp -> !assigned.contains(tp))
                .collect(Collectors.toSet()));

        // For assignment, we modify the "current" assignment by adding all entries that were present in the
        // "target" assignment but not in the "current" assignment.
        AssignmentGenerator modifier = diffPartitions -> {
            Set<TopicPartition> newAssignment = new HashSet<>(subscriptions.assignedPartitions());
            newAssignment.addAll(diffPartitions);
            return sortPartitions(newAssignment);
        };

        return reconcile(assignment,
                String.format("%s.onPartitionsAssigned()", ConsumerRebalanceListener.class.getSimpleName()),
                setDifferenceGenerator,
                AssignPartitionsEvent::new,
                modifier);
    }

    private ReconciliationResult reconcile(Optional<Assignment> assignment,
                                           String listenerMethodName,
                                           SetDifferenceGenerator setDifferenceGenerator,
                                           EventGenerator eventGenerator,
                                           AssignmentGenerator assignmentGenerator) {
        // "diff" the two sets of partitions: our "current" assignment and the "target" assignment. The result is
        // sorted primarily so when the partitions show up in the logs, it's easier for us humans to understand.
        SortedSet<TopicPartition> diffPartitions = setDifferenceGenerator.generate(
                subscriptions.assignedPartitions(),
                targetPartitions(metadata, assignment)
        );

        if (diffPartitions.isEmpty()) {
            log.debug("Skipping invocation of {} as no partitions were changed in the new assignment",
                    listenerMethodName);
            return ReconciliationResult.UNCHANGED;
        }

        // Set up our callback invocation. We don't block here waiting on it its completion, though.
        log.debug("Preparing to invoke {} with the following partitions: {}", listenerMethodName, diffPartitions);
        RebalanceCallbackEvent event = eventGenerator.generate(diffPartitions);
        inflightCallback = Optional.of(event);

        // Enqueue it in our background->application shared queue. This should be invoked in the Consumer.poll() method.
        backgroundEventQueue.add(event);

        // It is only after the appropriate ConsumerRebalanceListener has been executed on the application thread
        // (via Consumer.poll()), can we "commit" the change to the assigned partitions in SubscriptionState.
        event.future().whenComplete((result, error) -> {
            if (error != null) {
                log.warn("An error occurred when invoking {} with the following partitions: {}",
                        listenerMethodName,
                        diffPartitions,
                        error);
                // TODO: should we proceed or abort?
            } else {
                log.debug("{} was successfully executed with the following partitions: {}",
                        listenerMethodName,
                        diffPartitions);
            }

            SortedSet<TopicPartition> newAssignment = assignmentGenerator.generate(diffPartitions);

            // And finally... assign the new set of partitions! In keeping with the existing KafkaConsumer
            // implementation, this can only be done after the callback was successful.
            subscriptions.assignFromSubscribed(newAssignment);
        });

        return ReconciliationResult.RECONCILING;
    }

    private static SortedSet<TopicPartition> sortPartitions(Set<TopicPartition> topicPartitions) {
        if (topicPartitions instanceof SortedSet)
            return (SortedSet<TopicPartition>) topicPartitions;

        SortedSet<TopicPartition> set = new TreeSet<>(new TopicPartitionComparator());
        set.addAll(topicPartitions);
        return set;
    }

    private static SortedSet<TopicPartition> targetPartitions(ConsumerMetadata metadata, Optional<Assignment> assignment) {
        Map<Uuid, String> topicIdToNameMap = new HashMap<>();

        for (Map.Entry<String, Uuid> entry : metadata.topicIds().entrySet())
            topicIdToNameMap.put(entry.getValue(), entry.getKey());

        Set<TopicPartition> set = new HashSet<>();

        assignment.ifPresent(a -> {
            for (TopicPartitions topicPartitions : a.assignedTopicPartitions()) {
                Uuid id = topicPartitions.topicId();
                String topic = Objects.requireNonNull(topicIdToNameMap.get(id), () -> String.format("No topic name was found in the metadata for topic ID %s", id));

                for (Integer partition : topicPartitions.partitions()) {
                    set.add(new TopicPartition(topic, partition));
                }
            }
        });

        return sortPartitions(set);
    }

    private interface SetDifferenceGenerator {

        SortedSet<TopicPartition> generate(Set<TopicPartition> assigned, Set<TopicPartition> target);
    }

    private interface AssignmentGenerator {

        SortedSet<TopicPartition> generate(Set<TopicPartition> diffedPartitions);
    }

    private interface EventGenerator {

        RebalanceCallbackEvent generate(SortedSet<TopicPartition> diffedPartitions);
    }
}
