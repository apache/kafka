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
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.RequestManager;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * An {@link EventProcessor} that is created and executes in the {@link ConsumerNetworkThread network thread}
 * which processes {@link ApplicationEvent application events} generated by the application thread.
 */
public class ApplicationEventProcessor extends EventProcessor<ApplicationEvent> {

    private final Logger log;
    private final ConsumerMetadata metadata;
    private final RequestManagers requestManagers;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata) {
        super(logContext, applicationEventQueue);
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.requestManagers = requestManagers;
        this.metadata = metadata;
    }

    /**
     * Process the events—if any—that were produced by the application thread. It is possible that when processing
     * an event generates an error. In such cases, the processor will log an exception, but we do not want those
     * errors to be propagated to the caller.
     */
    @Override
    public void process() {
        process((event, error) -> { });
    }

    @Override
    public void process(ApplicationEvent event) {
        switch (event.type()) {
            case COMMIT:
                process((CommitApplicationEvent) event);
                return;

            case POLL:
                process((PollApplicationEvent) event);
                return;

            case FETCH_COMMITTED_OFFSET:
                process((OffsetFetchApplicationEvent) event);
                return;

            case METADATA_UPDATE:
                process((NewTopicsMetadataUpdateRequestEvent) event);
                return;

            case ASSIGNMENT_CHANGE:
                process((AssignmentChangeApplicationEvent) event);
                return;

            case TOPIC_METADATA:
                process((TopicMetadataApplicationEvent) event);
                return;

            case LIST_OFFSETS:
                process((ListOffsetsApplicationEvent) event);
                return;

            case RESET_POSITIONS:
                processResetPositionsEvent();
                return;

            case VALIDATE_POSITIONS:
                processValidatePositionsEvent();
                return;

            default:
                log.warn("Application event type " + event.type() + " was not expected");
        }
    }

    @Override
    protected Class<ApplicationEvent> getEventClass() {
        return ApplicationEvent.class;
    }

    private void process(final PollApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.pollTimeMs());
    }

    private void process(final CommitApplicationEvent event) {
        process(
                requestManagers.commitRequestManager,
                event,
                crm -> event.chain(crm.addOffsetCommitRequest(event.offsets())),
                () -> "Unable to commit offset"
        );
    }

    private void process(final OffsetFetchApplicationEvent event) {
        process(
                requestManagers.commitRequestManager,
                event,
                crm -> event.chain(crm.addOffsetFetchRequest(event.partitions())),
                () -> "Unable to fetch committed offset"
        );
    }

    private void process(final NewTopicsMetadataUpdateRequestEvent ignored) {
        metadata.requestUpdateForNewTopics();
    }

    private void process(final AssignmentChangeApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs());
        manager.maybeAutoCommit(event.offsets());
    }

    private void process(final ListOffsetsApplicationEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future =
                requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(),
                        event.requireTimestamps());
        event.chain(future);
    }

    private void processResetPositionsEvent() {
        requestManagers.offsetsRequestManager.resetPositionsIfNeeded();
    }

    private void processValidatePositionsEvent() {
        requestManagers.offsetsRequestManager.validatePositionsIfNeeded();
    }

    private void process(final TopicMetadataApplicationEvent event) {
        final CompletableFuture<Map<String, List<PartitionInfo>>> future =
                this.requestManagers.topicMetadataRequestManager.requestTopicMetadata(Optional.of(event.topic()));
        event.chain(future);
    }

    private <T extends RequestManager> void process(final Optional<T> requestManager,
                                                    final CompletableApplicationEvent<?> event,
                                                    final Consumer<T> ifPresentOperation,
                                                    final Supplier<String> ifNotPresentMessage) {
        if (requestManager.isPresent()) {
            ifPresentOperation.accept(requestManager.get());
        } else {
            // Leaving this error handling here, but it is a bit strange as the Consumer implementation should enforce
            // the group.id upfront, so we should never get here.
            String message = ifNotPresentMessage.get();
            Exception exception = new KafkaException(message + ". Most likely because the group.id wasn't set");
            event.future().completeExceptionally(exception);
        }
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<ApplicationEventProcessor> supplier(final LogContext logContext,
                                                               final ConsumerMetadata metadata,
                                                               final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                                               final Supplier<RequestManagers> requestManagersSupplier) {
        return new CachedSupplier<ApplicationEventProcessor>() {
            @Override
            protected ApplicationEventProcessor create() {
                RequestManagers requestManagers = requestManagersSupplier.get();
                return new ApplicationEventProcessor(
                        logContext,
                        applicationEventQueue,
                        requestManagers,
                        metadata
                );
            }
        };
    }
}
