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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.internals.ConsumerCoordinator;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.CoordinatorRequestManager;
import org.apache.kafka.clients.consumer.internals.FetchRequestManager;
import org.apache.kafka.clients.consumer.internals.NoopBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.OffsetFetchRequestManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ApplicationEventProcessor {

    private Logger log;

    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    private Time time;

    private FetchRequestManager fetchRequestManager;

    private OffsetFetchRequestManager offsetFetchRequestManager;

    private CoordinatorRequestManager coordinatorRequestManager;

    private ConsumerCoordinator coordinator;

    private ConsumerMetadata metadata;

    private SubscriptionState subscriptions;

    private List<ConsumerPartitionAssignor> assignors;

    public ApplicationEventProcessor(final BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.backgroundEventQueue = backgroundEventQueue;
    }

    /**
     * TODO: What does the returned {@code boolean} signify, exactly?
     */
    public boolean process(final ApplicationEvent event) {
        Objects.requireNonNull(event);
        switch (event.type()) {
            case NOOP:
                return process((NoopApplicationEvent) event);
            case ASSIGN_PARTITIONS:
                return process((AssignPartitionsEvent) event);
            case SUBSCRIBE:
                return process((SubscribeEvent) event);
            case UNSUBSCRIBE:
                return process((UnsubscribeEvent) event);
            case FETCH_OFFSETS:
                return process((FetchOffsetsEvent) event);
        }
        return false;
    }

    /**
     * Processes {@link NoopApplicationEvent} and equeue a
     * {@link NoopBackgroundEvent}. This is intentionally left here for
     * demonstration purpose.
     *
     * @param event a {@link NoopApplicationEvent}
     */
    private boolean process(final NoopApplicationEvent event) {
        return backgroundEventQueue.add(new NoopBackgroundEvent(event.message));
    }

    /**
     * Perform the following when this event is received:
     *
     * <ol>
     *     <li>Determine the topics that were removed and clear any previously-fetched data</li>
     *     <li>Run the 'maybe auto commit offsets' logic</li>
     *     <li>Request any metadata for the topics that were added</li>
     * </ol>
     *
     * @param event {@link AssignPartitionsEvent}
     */
    private boolean process(final AssignPartitionsEvent event) {
        Collection<TopicPartition> partitions = event.partitions();
        fetchRequestManager.clearBufferedDataForUnassignedPartitions(partitions);

        // make sure the offsets of topic partitions the consumer is unsubscribing from
        // are committed since there will be no following rebalance
        if (coordinator != null)
            coordinator.maybeAutoCommitOffsetsAsync(time.milliseconds());

        log.info("Assigned to partition(s): {}", Utils.join(partitions, ", "));

        if (subscriptions.assignFromUser(new HashSet<>(partitions)))
            metadata.requestUpdateForNewTopics();

        return true;
    }

    /**
     * Perform the following when this event is received:
     *
     * <ol>
     *     <li>Determine the topics that were removed and clear any previously-fetched data</li>
     *     <li>Request any metadata for the topics that were added</li>
     * </ol>
     *
     * TODO:    Question 1: who "owns" the callback? The rebalance happens on the background thread,
     *          but on which thread do we want to invoke the callback--background or foreground?
     *          --
     *          Question 2: Do we want to send the list of topics to the background thread and have it reconcile
     *          the set of topics, or should we let the foreground state resolve the subscriptions and then just
     *          pass that to the background thread to let it update its state?
     *
     * @param event {@link SubscribeTopicsEvent}
     */
    private boolean process(final SubscribeEvent event) {
        ConsumerRebalanceListener listener = event.rebalanceListener();

        if (event instanceof SubscribeTopicsEvent) {
            Collection<String> topics = ((SubscribeTopicsEvent) event).topics();
            fetchRequestManager.clearBufferedDataForUnassignedTopics(topics);
            log.info("Subscribed to topic(s): {}", Utils.join(topics, ", "));

            if (subscriptions.subscribe(new HashSet<>(topics), listener))
                metadata.requestUpdateForNewTopics();
        } else if (event instanceof SubscribePatternEvent) {
            Pattern pattern = ((SubscribePatternEvent) event).pattern();
            log.info("Subscribed to pattern: '{}'", pattern);
            subscriptions.subscribe(pattern, listener);
            coordinator.updatePatternSubscription(metadata.fetch());
            metadata.requestUpdateForNewTopics();
        }

        return true;
    }

    private boolean process(final UnsubscribeEvent event) {
        fetchRequestManager.clearBufferedDataForUnassignedPartitions(Collections.emptySet());

        if (coordinator != null) {
            coordinator.onLeavePrepare();
            coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
        }

        subscriptions.unsubscribe();
        log.info("Unsubscribed all topics or patterns and assigned partitions");
        return true;
    }

    private boolean process(final FetchOffsetsEvent event) {
        Collection<TopicPartition> partitions = event.partitions();

        Map<TopicPartition, Long> timestampsToSearch = partitions.stream()
                .distinct()
                .collect(Collectors.toMap(Function.identity(), tp -> event.timestamp()));

        // TODO: these calls to add and clear the transient topics needs to be handled by the implementation
        //       of OffsetFetchRequestManager.fetchOffsetsByTimes, not here...
        metadata.addTransientTopics(topicsForPartitions(partitions));
        offsetFetchRequestManager.fetchOffsetsByTimes(timestampsToSearch, false, event.future());
        metadata.clearTransientTopics();

        return true;
    }

    private Set<String> topicsForPartitions(Collection<TopicPartition> partitions) {
        return partitions.stream().map(TopicPartition::topic).collect(Collectors.toSet());
    }

}
