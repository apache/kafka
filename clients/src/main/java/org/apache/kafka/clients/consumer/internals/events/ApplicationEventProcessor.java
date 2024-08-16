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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.Acknowledgements;
import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMembershipManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.clients.consumer.internals.ShareConsumeRequestManager;
import org.apache.kafka.clients.consumer.internals.ShareMembershipManager;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.LogContext;

import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.maybeWrapAsKafkaException;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.refreshCommittedOffsets;

/**
 * An {@link EventProcessor} that is created and executes in the {@link ConsumerNetworkThread network thread}
 * which processes {@link ApplicationEvent application events} generated by the application thread.
 */
public class ApplicationEventProcessor implements EventProcessor<ApplicationEvent> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final SubscriptionState subscriptions;
    private final RequestManagers requestManagers;
    private final Time time;

    /**
     * OffsetFetch request triggered to update fetch positions. The request is kept. It will be
     * cleared every time a response with the committed offsets is received and used to update
     * fetch positions. If the response cannot be used because the UpdateFetchPositions expired,
     * it will be kept to be used on the next attempt to update fetch positions if partitions
     * remain the same.
     */
    private FetchCommittedOffsetsEvent pendingOffsetFetchEvent;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata,
                                     final SubscriptionState subscriptions,
                                     final Time time) {
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.requestManagers = requestManagers;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.time = time;
    }

    @SuppressWarnings({"CyclomaticComplexity"})
    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT_ASYNC:
                process((AsyncCommitEvent) event);
                return;

            case COMMIT_SYNC:
                process((SyncCommitEvent) event);
                return;

            case POLL:
                process((PollEvent) event);
                return;

            case FETCH_COMMITTED_OFFSETS:
                process((FetchCommittedOffsetsEvent) event);
                return;

            case NEW_TOPICS_METADATA_UPDATE:
                process((NewTopicsMetadataUpdateRequestEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataEvent) event);
                return;

            case ALL_TOPICS_METADATA:
                process((AllTopicsMetadataEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsEvent) event);
                return;

            case UPDATE_FETCH_POSITIONS:
                process((UpdateFetchPositionsEvent) event);
                return;

            case SUBSCRIPTION_CHANGE:
                process((SubscriptionChangeEvent) event);
                return;

            case UNSUBSCRIBE:
                process((UnsubscribeEvent) event);
                return;

            case CONSUMER_REBALANCE_LISTENER_CALLBACK_COMPLETED:
                process((ConsumerRebalanceListenerCallbackCompletedEvent) event);
                return;

            case COMMIT_ON_CLOSE:
                process((CommitOnCloseEvent) event);
                return;

            case SHARE_FETCH:
                process((ShareFetchEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_SYNC:
                process((ShareAcknowledgeSyncEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_ASYNC:
                process((ShareAcknowledgeAsyncEvent) event);
                return;

            case SHARE_SUBSCRIPTION_CHANGE:
                process((ShareSubscriptionChangeEvent) event);
                return;

            case SHARE_UNSUBSCRIBE:
                process((ShareUnsubscribeEvent) event);
                return;

            case SHARE_ACKNOWLEDGE_ON_CLOSE:
                process((ShareAcknowledgeOnCloseEvent) event);
                return;

            default:
                log.warn("Application event type {} was not expected", event.type());
        }
    }

    private void process(final PollEvent event) {
        if (requestManagers.commitRequestManager.isPresent()) {
            requestManagers.commitRequestManager.ifPresent(m -> m.updateAutoCommitTimer(event.pollTimeMs()));
            requestManagers.heartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(event.pollTimeMs()));
        } else {
            requestManagers.shareHeartbeatRequestManager.ifPresent(hrm -> hrm.resetPollTimer(event.pollTimeMs()));
        }
    }

    private void process(final AsyncCommitEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Void> future = manager.commitAsync(event.offsets());
        future.whenComplete(complete(event.future()));
    }

    private void process(final SyncCommitEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Void> future = manager.commitSync(event.offsets(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final FetchCommittedOffsetsEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommittedRequestManager is not available. Check if group.id was set correctly"));
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> future = manager.fetchOffsets(event.partitions(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final NewTopicsMetadataUpdateRequestEvent ignored) {
        metadata.requestUpdateForNewTopics();
    }

    /**
     * Commit all consumed if auto-commit is enabled. Note this will trigger an async commit,
     * that will not be retried if the commit request fails.
     */
    private void process(final AssignmentChangeEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs());
        manager.maybeAutoCommitAsync();
    }

    /**
     * Handles ListOffsetsEvent by fetching the offsets for the given partitions and timestamps.
     */
    private void process(final ListOffsetsEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestampInternal>> future =
            requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(), event.requireTimestamps());
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event that indicates that the subscription changed. This will make the
     * consumer join the group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void process(final SubscriptionChangeEvent ignored) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        ConsumerMembershipManager membershipManager = requestManagers.heartbeatRequestManager.get().membershipManager();
        membershipManager.onSubscriptionUpdated();
    }

    /**
     * Process event indicating that the consumer unsubscribed from all topics. This will make
     * the consumer release its assignment and send a request to leave the group.
     *
     * @param event Unsubscribe event containing a future that will complete when the callback
     *              execution for releasing the assignment completes, and the request to leave
     *              the group is sent out.
     */
    private void process(final UnsubscribeEvent event) {
        if (requestManagers.heartbeatRequestManager.isPresent()) {
            ConsumerMembershipManager membershipManager = requestManagers.heartbeatRequestManager.get().membershipManager();
            CompletableFuture<Void> future = membershipManager.leaveGroup();
            future.whenComplete(complete(event.future()));
        } else {
            // If the consumer is not using the group management capabilities, we still need to clear all assignments it may have.
            subscriptions.unsubscribe();
            event.future().complete(null);
        }
    }

    /**
     *
     * Fetch committed offsets and use them to update positions in the subscription state. If no
     * committed offsets available, fetch offsets from the leader.
     */
    private void process(final UpdateFetchPositionsEvent updateFetchPositionsEvent) {
        try {
            // The event could be completed in the app thread before it got to be
            // processed in the background (ex. interrupted)
            if (updateFetchPositionsEvent.future().isCompletedExceptionally()) {
                log.debug("UpdateFetchPositions event {} was completed exceptionally before it " +
                    "got time to be processed.", updateFetchPositionsEvent);
                return;
            }

            // Validate positions using the partition leader end offsets, to detect if any partition
            // has been truncated due to a leader change. This will trigger an OffsetForLeaderEpoch
            // request, retrieve the partition end offsets, and validate the current position
            // against it. It will throw an exception if log truncation is detected.
            requestManagers.offsetsRequestManager.validatePositionsIfNeeded();

            boolean hasAllFetchPositions = subscriptions.hasAllFetchPositions();
            if (hasAllFetchPositions) {
                updateFetchPositionsEvent.future().complete(true);
                return;
            }

            // Reset positions using committed offsets retrieved from the group coordinator, for any
            // partitions which do not have a valid position and are not awaiting reset. This will
            // trigger an OffsetFetch request and update positions with the offsets retrieved. This
            // will only do a coordinator lookup if there are partitions which have missing
            // positions, so a consumer with manually assigned partitions can avoid a coordinator
            // dependence by always ensuring that assigned partitions have an initial position.
            if (requestManagers.commitRequestManager.isPresent()) {
                CompletableFuture<Void> initWithCommittedOffsetsResult = initWithCommittedOffsetsIfNeeded(updateFetchPositionsEvent);
                initWithCommittedOffsetsResult.whenComplete((__, error) -> {
                    if (error == null) {
                        // Retrieve partition offsets to init positions for partitions that still
                        // don't have a valid position
                        initWithPartitionOffsetsIfNeeded(updateFetchPositionsEvent);
                    } else {
                        updateFetchPositionsEvent.future().completeExceptionally(error);
                    }
                });
            } else {
                initWithPartitionOffsetsIfNeeded(updateFetchPositionsEvent);
            }

        } catch (Exception e) {
            updateFetchPositionsEvent.future().completeExceptionally(maybeWrapAsKafkaException(e));
        }
    }

    private void maybeWakeupPendingOffsetFetch(Throwable error) {
        if (error instanceof WakeupException) {
            log.warn("Application event to update fetch positions was completed" +
                "Clearing background offsetFetchEvent", error);
            if (pendingOffsetFetchEvent != null) {
                log.warn("UpdateFetchPosition event got wakeup exception. Passing wake up" +
                    " to the long-running pendingOffsetFetchEvent to abort it");
                pendingOffsetFetchEvent.future().completeExceptionally(error);
            }
        }
    }

    private void initWithPartitionOffsetsIfNeeded(final UpdateFetchPositionsEvent updateFetchPositionsEvent) {
        try {
            // If there are partitions still needing a position and a reset policy is defined,
            // request reset using the default policy. If no reset strategy is defined and there
            // are partitions with a missing position, then we will raise a NoOffsetForPartitionException exception.
            subscriptions.resetInitializingPositions();

            // Reset positions using partition offsets retrieved from the leader, for any partitions
            // which are awaiting reset. This will trigger a ListOffset request, retrieve the
            // partition offsets according to the strategy (ex. earliest, latest), and update the
            // positions.
            CompletableFuture<Void> resetPositionsFuture = requestManagers.offsetsRequestManager.resetPositionsIfNeeded();

            resetPositionsFuture.whenComplete((result, error) -> {
                if (updateFetchPositionsEvent.future().isDone()) {
                    log.debug("UpdateFetchPositions event {} had already expired when reset " +
                        "positions completed.", updateFetchPositionsEvent);
                    return;
                }
                if (error == null) {
                    updateFetchPositionsEvent.future().complete(false);
                } else {
                    updateFetchPositionsEvent.future().completeExceptionally(error);
                }
            });
        } catch (Exception e) {
            updateFetchPositionsEvent.future().completeExceptionally(e);
        }
    }

    /**
     * Fetch the committed offsets for partitions that require initialization. Use them to set
     * the fetch positions in the subscription state.
     *
     * @throws TimeoutException If offsets could not be retrieved within the timeout
     */
    private CompletableFuture<Void> initWithCommittedOffsetsIfNeeded(UpdateFetchPositionsEvent updateFetchPositionsEvent) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        if (initializingPartitions.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        updateFetchPositionsEvent.future().whenComplete((__, error) -> maybeWakeupPendingOffsetFetch(error));

        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        CompletableFuture<Void> result = new CompletableFuture<>();

        // The shorter the timeout provided to poll(), the more likely the offsets fetch will time out. To handle
        // this case, on the first attempt to fetch the committed offsets, a FetchCommittedOffsetsEvent is created
        // (with potentially a longer timeout) and stored. The event is used for the first attempt, but in the
        // case it times out, subsequent attempts will also use the event in order to wait for the results.
        if (!canReusePendingOffsetFetchEvent(initializingPartitions)) {
            final long deadlineMs = Math.max(updateFetchPositionsEvent.deadlineMs(), updateFetchPositionsEvent.fetchOffsetsDeadlineMs());
            pendingOffsetFetchEvent = new FetchCommittedOffsetsEvent(initializingPartitions, deadlineMs);
            pendingOffsetFetchEvent.future().whenComplete((offsets, error) -> {
                if (!updateFetchPositionsEvent.future().isDone()) {
                    updatePositionsAndClearPendingOffsetsFetch(offsets, error, result);
                }
            });
            process(pendingOffsetFetchEvent);
        } else {
            // Using previous fetch issued for the same set of partitions, that hasn't been used yet
            if (pendingOffsetFetchEvent.future().isDone()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = null;
                Throwable error = null;
                try {
                    offsets = pendingOffsetFetchEvent.future().get();
                } catch (ExecutionException e) {
                    error = e.getCause();
                } catch (Exception e) {
                    error = e;
                }
                updatePositionsAndClearPendingOffsetsFetch(offsets, error, result);
            }
        }

        return result;
    }

    // Visible for testing
    boolean hasPendingOffsetFetchEvent() {
        return pendingOffsetFetchEvent != null;
    }

    private void updatePositionsAndClearPendingOffsetsFetch(Map<TopicPartition, OffsetAndMetadata> offsets,
                                                            Throwable error,
                                                            CompletableFuture<Void> result) {
        pendingOffsetFetchEvent = null;
        if (error == null) {
            refreshCommittedOffsets(offsets, metadata, subscriptions);
            result.complete(null);
        } else {
            log.error("Error fetching committed offsets to update positions", error);
            result.completeExceptionally(error);
        }
    }

    /**
     * This determines if the {@link #pendingOffsetFetchEvent pending offset fetch event} can be reused. Reuse
     * is only possible if all the following conditions are true:
     *
     * <ul>
     *     <li>A pending offset fetch event exists</li>
     *     <li>The partition set of the pending offset fetch event is the same as the given partition set</li>
     *     <li>The pending offset fetch event has not expired</li>
     * </ul>
     */
    private boolean canReusePendingOffsetFetchEvent(Set<TopicPartition> partitions) {
        if (pendingOffsetFetchEvent == null) {
            return false;
        }

        if (!pendingOffsetFetchEvent.partitions().equals(partitions)) {
            return false;
        }

        return pendingOffsetFetchEvent.deadlineMs() > time.milliseconds();
    }

    private void process(final TopicMetadataEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestTopicMetadata(event.topic(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final AllTopicsMetadataEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                requestManagers.topicMetadataRequestManager.requestAllTopicsMetadata(event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private void process(final ConsumerRebalanceListenerCallbackCompletedEvent event) {
        if (!requestManagers.heartbeatRequestManager.isPresent()) {
            log.warn(
                "An internal error occurred; the group membership manager was not present, so the notification of the {} callback execution could not be sent",
                event.methodName()
            );
            return;
        }
        ConsumerMembershipManager manager = requestManagers.heartbeatRequestManager.get().membershipManager();
        manager.consumerRebalanceListenerCallbackCompleted(event);
    }

    private void process(@SuppressWarnings("unused") final CommitOnCloseEvent event) {
        if (!requestManagers.commitRequestManager.isPresent())
            return;
        log.debug("Signal CommitRequestManager closing");
        requestManagers.commitRequestManager.get().signalClose();
    }

    /**
     * Process event that tells the share consume request manager to fetch more records.
     */
    private void process(final ShareFetchEvent event) {
        requestManagers.shareConsumeRequestManager.ifPresent(scrm -> scrm.fetch(event.acknowledgementsMap()));
    }

    /**
     * Process event that indicates the consumer acknowledged delivery of records synchronously.
     */
    private void process(final ShareAcknowledgeSyncEvent event) {
        if (!requestManagers.shareConsumeRequestManager.isPresent()) {
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        CompletableFuture<Map<TopicIdPartition, Acknowledgements>> future =
                manager.commitSync(event.acknowledgementsMap(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event that indicates the consumer acknowledged delivery of records asynchronously.
     */
    private void process(final ShareAcknowledgeAsyncEvent event) {
        if (!requestManagers.shareConsumeRequestManager.isPresent()) {
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        manager.commitAsync(event.acknowledgementsMap());
    }

    /**
     * Process event that indicates that the subscription changed for a share group. This will make the
     * consumer join the share group if it is not part of it yet, or send the updated subscription if
     * it is already a member.
     */
    private void process(final ShareSubscriptionChangeEvent ignored) {
        if (!requestManagers.shareHeartbeatRequestManager.isPresent()) {
            log.warn("Group membership manager not present when processing a subscribe event");
            return;
        }
        ShareMembershipManager membershipManager = requestManagers.shareHeartbeatRequestManager.get().membershipManager();
        membershipManager.onSubscriptionUpdated();
    }

    /**
     * Process event indicating that the consumer unsubscribed from all topics. This will make
     * the consumer release its assignment and send a request to leave the share group.
     *
     * @param event Unsubscribe event containing a future that will complete when the callback
     *              execution for releasing the assignment completes, and the request to leave
     *              the group is sent out.
     */
    private void process(final ShareUnsubscribeEvent event) {
        if (!requestManagers.shareHeartbeatRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an unsubscribe event");
            event.future().completeExceptionally(error);
            return;
        }
        ShareMembershipManager membershipManager = requestManagers.shareHeartbeatRequestManager.get().membershipManager();
        CompletableFuture<Void> future = membershipManager.leaveGroup();
        // The future will be completed on heartbeat sent
        future.whenComplete(complete(event.future()));
    }

    /**
     * Process event indicating that the consumer is closing. This will make the consumer
     * complete pending acknowledgements.
     *
     * @param event Acknowledge-on-close event containing a future that will complete when
     *              the acknowledgements have responses.
     */
    private void process(final ShareAcknowledgeOnCloseEvent event) {
        if (!requestManagers.shareConsumeRequestManager.isPresent()) {
            KafkaException error = new KafkaException("Group membership manager not present when processing an acknowledge-on-close event");
            event.future().completeExceptionally(error);
            return;
        }

        ShareConsumeRequestManager manager = requestManagers.shareConsumeRequestManager.get();
        CompletableFuture<Void> future = manager.acknowledgeOnClose(event.acknowledgementsMap(), event.deadlineMs());
        future.whenComplete(complete(event.future()));
    }

    private <T> BiConsumer<? super T, ? super Throwable> complete(final CompletableFuture<T> b) {
        return (value, exception) -> {
            if (exception != null)
                b.completeExceptionally(exception);
            else
                b.complete(value);
        };
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final SubscriptionState subscriptions,
                                                               final Supplier<RequestManagers> requestManagersSupplier,
                                                               final Time time) {
        return new CachedSupplier<ApplicationEventProcessor>() {
            @Override
            protected ApplicationEventProcessor create() {
                RequestManagers requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor(
                        logContext,
                        requestManagers,
                        metadata,
                        subscriptions,
                        time
                );
            }
        };
    }
}
