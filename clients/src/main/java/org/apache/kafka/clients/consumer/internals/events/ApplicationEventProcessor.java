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

import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.CompletedFetch;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.NoopBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.Queue;

public class ApplicationEventProcessor<K, V> {

    private final Logger log;
    private final Queue<BackgroundEvent> backgroundEventQueue;
    private final RequestManagers<K, V> requestManagers;
    private final ConsumerMetadata metadata;

    public ApplicationEventProcessor(final LogContext logContext,
                                     final Queue<BackgroundEvent> backgroundEventQueue,
                                     final RequestManagers<K, V> requestManagers,
                                     final ConsumerMetadata metadata) {
        this.log = logContext.logger(ApplicationEventProcessor.class);
        this.backgroundEventQueue = backgroundEventQueue;
        this.requestManagers = requestManagers;
        this.metadata = metadata;
    }

    @SuppressWarnings("unchecked")
    public boolean process(final ApplicationEvent event) {
        Objects.requireNonNull(event);
        Objects.requireNonNull(event.type, "Null type for event " + event);

        log.debug("Processing event {}", event);

        switch (event.type) {
            case COMMIT:
                return process((CommitApplicationEvent) event);
            case POLL:
                return process((PollApplicationEvent) event);
            case FETCH_COMMITTED_OFFSET:
                return process((OffsetFetchApplicationEvent) event);
            case METADATA_UPDATE:
                return process((MetadataUpdateApplicationEvent) event);
            case UNSUBSCRIBE:
                return process((UnsubscribeApplicationEvent) event);
            case FETCH:
                return process((FetchEvent<K, V>) event);
            case NOOP:
                return process((NoopApplicationEvent) event);
        }

        return false;
    }

    private boolean process(final NoopApplicationEvent event) {
        return backgroundEventQueue.add(new NoopBackgroundEvent(event.message));
    }

    private boolean process(final PollApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return true;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.pollTimeMs);
        return true;
    }

    private boolean process(final CommitApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            // Leaving this error handling here, but it is a bit strange as the commit API should enforce the group.id
            // upfront so we should never get to this block.
            Exception exception = new KafkaException("Unable to commit offset. Most likely because the group.id wasn't set");
            event.future.completeExceptionally(exception);
            return false;
        }

        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.addOffsetCommitRequest(event.offsets).whenComplete((r, e) -> {
            if (e != null) {
                event.future.completeExceptionally(e);
                return;
            }
            event.future.complete(null);
        });
        return true;
    }

    private boolean process(final FetchEvent<K, V> event) {
        // The request manager keeps track of the completed fetches, so we pull any that are ready off, and return
        // them to the application.
        Queue<CompletedFetch<K, V>> completedFetches = requestManagers.fetchRequestManager.drain();
        event.future.complete(completedFetches);
        return true;
    }

    private boolean process(final OffsetFetchApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future.completeExceptionally(new KafkaException("Unable to fetch committed offset because the " +
                    "CommittedRequestManager is not available. Check if group.id was set correctly"));
            return false;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.addOffsetFetchRequest(event.partitions);
        return true;
    }

    private boolean process(final MetadataUpdateApplicationEvent event) {
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
}
