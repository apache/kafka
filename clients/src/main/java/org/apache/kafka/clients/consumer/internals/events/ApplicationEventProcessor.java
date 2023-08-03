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

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.CompletedFetch;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class ApplicationEventProcessor<K, V> {

    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    private final Logger log;

    private final ConsumerMetadata metadata;

    private final RequestManagers<K, V> requestManagers;

    public ApplicationEventProcessor(final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                     final RequestManagers<K, V> requestManagers,
                                     final ConsumerMetadata metadata,
                                     final LogContext logContext) {
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.backgroundEventQueue = backgroundEventQueue;
        this.requestManagers = requestManagers;
        this.metadata = metadata;
    }

    @SuppressWarnings("unchecked")
    public boolean process(final ApplicationEvent event) {
        Objects.requireNonNull(event, "Attempt to process null ApplicationEvent");
        Objects.requireNonNull(event.type(), "Attempt to process ApplicationEvent with null type: " + event);

        log.debug("Processing event {}", event);

        // Make sure to use the event's type() method, not the type variable directly. This causes problems when
        // unit tests mock the EventType.
        switch (event.type()) {
            case NOOP:
                return process((NoopApplicationEvent) event);
            case COMMIT:
                return process((CommitApplicationEvent) event);
            case POLL:
                return process((PollApplicationEvent) event);
            case FETCH_COMMITTED_OFFSET:
                return process((OffsetFetchApplicationEvent) event);
            case METADATA_UPDATE:
                return process((NewTopicsMetadataUpdateRequestEvent) event);
            case TOPIC_METADATA:
                return process((TopicMetadataApplicationEvent) event);
            case UNSUBSCRIBE:
                return process((UnsubscribeApplicationEvent) event);
            case LIST_OFFSETS:
                return process((ListOffsetsApplicationEvent) event);
            case FETCH:
                return process((FetchEvent<K, V>) event);
            case RESET_POSITIONS:
                return process((ResetPositionsApplicationEvent) event);
            case VALIDATE_POSITIONS:
                return process((ValidatePositionsApplicationEvent) event);
            case ASSIGNMENT_CHANGE:
                return process((AssignmentChangeApplicationEvent) event);
        }
        return false;
    }

    /**
     * Processes {@link NoopApplicationEvent} and enqueue a
     * {@link NoopBackgroundEvent}. This is intentionally left here for
     * demonstration purpose.
     *
     * @param event a {@link NoopApplicationEvent}
     */
    private boolean process(final NoopApplicationEvent event) {
        return backgroundEventQueue.add(new NoopBackgroundEvent(event.message()));
    }

    private boolean process(final PollApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return true;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.pollTimeMs());
        return true;
    }

    private boolean process(final CommitApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            // Leaving this error handling here, but it is a bit strange as the commit API should enforce the group.id
            // upfront so we should never get to this block.
            Exception exception = new KafkaException("Unable to commit offset. Most likely because the group.id wasn't set");
            event.future().completeExceptionally(exception);
            return false;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        event.chain(manager.addOffsetCommitRequest(event.offsets()));
        return true;
    }

    private boolean process(final FetchEvent<K, V> event) {
        // The request manager keeps track of the completed fetches, so we pull any that are ready off, and return
        // them to the application.
        Queue<CompletedFetch<K, V>> completedFetches = requestManagers.fetchRequestManager.drain();
        event.future().complete(completedFetches);
        return true;
    }

    private boolean process(final OffsetFetchApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed offset because the " +
                    "CommittedRequestManager is not available. Check if group.id was set correctly"));
            return false;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        event.chain(manager.addOffsetFetchRequest(event.partitions()));
        return true;
    }

    private boolean process(final NewTopicsMetadataUpdateRequestEvent event) {
        metadata.requestUpdateForNewTopics();
        return true;
    }

    private boolean process(final UnsubscribeApplicationEvent event) {
        /*
                this.coordinator.onLeavePrepare();
                this.coordinator.maybeLeaveGroup("the consumer unsubscribed from all topics");
         */
        return true;
    }

    private boolean process(final ListOffsetsApplicationEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future =
                requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(),
                        event.requireTimestamps());
        event.chain(future);
        return true;
    }

    /**
     * To process a ResetPositionsApplicationEvent, this will determine the partitions needing
     * reset and, if any, it will perform a ListOffset request to retrieve its offsets based on
     * the reset strategy defined. It will also update in-memory positions based on the retrieved
     * offsets.
     * <p>
     * This may throw an exception cached in memory from the previous request if it failed.
     */
    private boolean process(final ResetPositionsApplicationEvent event) {
        requestManagers.offsetsRequestManager.resetPositionsIfNeeded();
        event.future().complete(null);
        // TODO: chain future to process failures at a higher level once the caching logic for
        //  exceptions is reviewed/removed. For now the event future indicates only that the
        //  event has been processed.
        return true;
    }

    /**
     * This will determine the partitions needing validation and, if any, it will perform an
     * OffsetForLeaderEpoch request. It will also update in-memory positions based on the retrieved
     * offsets.
     * <p>
     * This may throw an exception cached in memory from the previous request if it failed.
     */
    private boolean process(final ValidatePositionsApplicationEvent event) {
        requestManagers.offsetsRequestManager.validatePositionsIfNeeded();
        event.future().complete(null);
        // TODO: chain future to process failures at a higher level once the caching logic for
        //  exceptions is reviewed/removed. For now the event future indicates only that the
        //  event has been processed.
        return true;
    }

    private boolean process(final TopicMetadataApplicationEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                this.requestManagers.topicMetadataRequestManager.requestTopicMetadata(event.topic());
        event.chain(future);
        return true;
    }

    private boolean process(final AssignmentChangeApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return false;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs);
        manager.maybeAutoCommit(event.offsets);
        return true;
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link org.apache.kafka.clients.consumer.internals.DefaultBackgroundThread}.
     */
    public static <K, V> Supplier<ApplicationEventProcessor<K, V>> supplier(final LogContext logContext,
                                                                            final ConsumerMetadata metadata,
                                                                            final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                                                            final Supplier<RequestManagers<K, V>> requestManagersSupplier) {
        return new CachedSupplier<ApplicationEventProcessor<K, V>>() {
            @Override
            protected ApplicationEventProcessor<K, V> create() {
                RequestManagers<K, V> requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor<>(backgroundEventQueue, requestManagers, metadata, logContext);
            }
        };
    }
}
