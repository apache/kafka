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
import org.apache.kafka.clients.consumer.internals.CommitRequestManager;
import org.apache.kafka.clients.consumer.internals.ConsumerMetadata;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public class ApplicationEventProcessor {

    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;

    private final ConsumerMetadata metadata;

    private final RequestManagers requestManagers;

    public ApplicationEventProcessor(final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                     final RequestManagers requestManagers,
                                     final ConsumerMetadata metadata) {
        this.backgroundEventQueue = backgroundEventQueue;
        this.requestManagers = requestManagers;
        this.metadata = metadata;
    }

    public boolean process(final ApplicationEvent event) {
        Objects.requireNonNull(event);
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
            case ASSIGNMENT_CHANGE:
                return process((AssignmentChangeApplicationEvent) event);
            case LIST_OFFSETS:
                return process((ListOffsetsApplicationEvent) event);
            case RESET_POSITIONS:
                return processResetPositionsEvent();
            case VALIDATE_POSITIONS:
                return processValidatePositionsEvent();
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
        manager.addOffsetCommitRequest(event.offsets()).whenComplete((r, e) -> {
            if (e != null) {
                event.future().completeExceptionally(e);
                return;
            }
            event.future().complete(null);
        });
        return true;
    }

    private boolean process(final OffsetFetchApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            event.future().completeExceptionally(new KafkaException("Unable to fetch committed " +
                    "offset because the CommittedRequestManager is not available. Check if group.id was set correctly"));
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

    private boolean process(final AssignmentChangeApplicationEvent event) {
        if (!requestManagers.commitRequestManager.isPresent()) {
            return false;
        }
        CommitRequestManager manager = requestManagers.commitRequestManager.get();
        manager.updateAutoCommitTimer(event.currentTimeMs());
        manager.maybeAutoCommit(event.offsets());
        return true;
    }

    private boolean process(final ListOffsetsApplicationEvent event) {
        final CompletableFuture<Map<TopicPartition, OffsetAndTimestamp>> future =
                requestManagers.offsetsRequestManager.fetchOffsets(event.timestampsToSearch(),
                        event.requireTimestamps());
        event.chain(future);
        return true;
    }

    private boolean processResetPositionsEvent() {
        requestManagers.offsetsRequestManager.resetPositionsIfNeeded();
        return true;
    }

    private boolean processValidatePositionsEvent() {
        requestManagers.offsetsRequestManager.validatePositionsIfNeeded();
        return true;
    }
}
