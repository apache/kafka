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

import org.apache.kafka.clients.consumer.internals.events.ApplicationEventHandler;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnAllTasksLostCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksAssignedCallbackNeededEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.StreamsOnTasksRevokedCallbackNeededEvent;
import org.apache.kafka.common.KafkaException;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Processes events from the Streams rebalance protocol.
 * <p>
 * The Streams rebalance processor receives events from the background thread of the async consumer, more precisely
 * from the Streams membership manager and handles them.
 * For example, events are requests for invoking the task assignment and task revocation callbacks.
 * Results of the event handling are passed back to the background thread.
 */
public class StreamsRebalanceEventsProcessor {

    private final BlockingQueue<BackgroundEvent> onCallbackRequests = new LinkedBlockingQueue<>();
    private ApplicationEventHandler applicationEventHandler = null;
    private final StreamsGroupRebalanceCallbacks rebalanceCallbacks;
    private final StreamsRebalanceData streamsRebalanceData;

    /**
     * Constructs the Streams rebalance processor.
     *
     * @param streamsRebalanceData
     * @param rebalanceCallbacks
     */
    public StreamsRebalanceEventsProcessor(StreamsRebalanceData streamsRebalanceData,
                                           StreamsGroupRebalanceCallbacks rebalanceCallbacks) {
        this.streamsRebalanceData = streamsRebalanceData;
        this.rebalanceCallbacks = rebalanceCallbacks;
    }

    /**
     * Requests the invocation of the task assignment callback.
     *
     * @param assignment The tasks to be assigned to the member of the Streams group.
     * @return A future that will be completed when the callback has been invoked.
     */
    public CompletableFuture<Void> requestOnTasksAssignedCallbackInvocation(final StreamsRebalanceData.Assignment assignment) {
        final StreamsOnTasksAssignedCallbackNeededEvent onTasksAssignedCallbackNeededEvent = new StreamsOnTasksAssignedCallbackNeededEvent(assignment);
        onCallbackRequests.add(onTasksAssignedCallbackNeededEvent);
        return onTasksAssignedCallbackNeededEvent.future();
    }

    /**
     * Requests the invocation of the task revocation callback.
     *
     * @param activeTasksToRevoke The tasks to revoke from the member of the Streams group
     * @return A future that will be completed when the callback has been invoked.
     */
    public CompletableFuture<Void> requestOnTasksRevokedCallbackInvocation(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        final StreamsOnTasksRevokedCallbackNeededEvent onTasksRevokedCallbackNeededEvent = new StreamsOnTasksRevokedCallbackNeededEvent(activeTasksToRevoke);
        onCallbackRequests.add(onTasksRevokedCallbackNeededEvent);
        return onTasksRevokedCallbackNeededEvent.future();
    }

    /**
     * Requests the invocation of the all tasks lost callback.
     *
     * @return A future that will be completed when the callback has been invoked.
     */
    public CompletableFuture<Void> requestOnAllTasksLostCallbackInvocation() {
        final StreamsOnAllTasksLostCallbackNeededEvent onAllTasksLostCallbackNeededEvent = new StreamsOnAllTasksLostCallbackNeededEvent();
        onCallbackRequests.add(onAllTasksLostCallbackNeededEvent);
        return onAllTasksLostCallbackNeededEvent.future();
    }

    /**
     * Sets the application event handler.
     *
     * The application handler sends the results of the callbacks to the background thread.
     *
     * @param applicationEventHandler The application handler.
     */
    public void setApplicationEventHandler(final ApplicationEventHandler applicationEventHandler) {
        this.applicationEventHandler = applicationEventHandler;
    }

    private void process(final BackgroundEvent event) {
        switch (event.type()) {
            case ERROR:
                throw ((ErrorEvent) event).error();

            case STREAMS_ON_TASKS_REVOKED_CALLBACK_NEEDED:
                processStreamsOnTasksRevokedCallbackNeededEvent((StreamsOnTasksRevokedCallbackNeededEvent) event);
                break;

            case STREAMS_ON_TASKS_ASSIGNED_CALLBACK_NEEDED:
                processStreamsOnTasksAssignedCallbackNeededEvent((StreamsOnTasksAssignedCallbackNeededEvent) event);
                break;

            case STREAMS_ON_ALL_TASKS_LOST_CALLBACK_NEEDED:
                processStreamsOnAllTasksLostCallbackNeededEvent((StreamsOnAllTasksLostCallbackNeededEvent) event);
                break;

            default:
                throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");

        }
    }

    private void processStreamsOnTasksRevokedCallbackNeededEvent(final StreamsOnTasksRevokedCallbackNeededEvent event) {
        StreamsOnTasksRevokedCallbackCompletedEvent invokedEvent = invokeOnTasksRevokedCallback(event.activeTasksToRevoke(), event.future());
        applicationEventHandler.add(invokedEvent);
        if (invokedEvent.error().isPresent()) {
            throw invokedEvent.error().get();
        }
    }

    private void processStreamsOnTasksAssignedCallbackNeededEvent(final StreamsOnTasksAssignedCallbackNeededEvent event) {
        StreamsOnTasksAssignedCallbackCompletedEvent invokedEvent = invokeOnTasksAssignedCallback(event.assignment(), event.future());
        applicationEventHandler.add(invokedEvent);
        if (invokedEvent.error().isPresent()) {
            throw invokedEvent.error().get();
        }
    }

    private void processStreamsOnAllTasksLostCallbackNeededEvent(final StreamsOnAllTasksLostCallbackNeededEvent event) {
        StreamsOnAllTasksLostCallbackCompletedEvent invokedEvent = invokeOnAllTasksLostCallback(event.future());
        applicationEventHandler.add(invokedEvent);
        if (invokedEvent.error().isPresent()) {
            throw invokedEvent.error().get();
        }
    }

    private StreamsOnTasksRevokedCallbackCompletedEvent invokeOnTasksRevokedCallback(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke,
                                                                                     final CompletableFuture<Void> future) {
        final Optional<KafkaException> error;
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onTasksRevoked(activeTasksToRevoke);
        if (exceptionFromCallback.isPresent()) {
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task revocation callback throws an error"));
        } else {
            error = Optional.empty();
        }
        return new StreamsOnTasksRevokedCallbackCompletedEvent(future, error);
    }

    private StreamsOnTasksAssignedCallbackCompletedEvent invokeOnTasksAssignedCallback(final StreamsRebalanceData.Assignment assignment,
                                                                                       final CompletableFuture<Void> future) {
        final Optional<KafkaException> error;
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onTasksAssigned(assignment);
        if (exceptionFromCallback.isPresent()) {
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task assignment callback throws an error"));
        } else {
            error = Optional.empty();
            streamsRebalanceData.setReconciledAssignment(assignment);
        }
        return new StreamsOnTasksAssignedCallbackCompletedEvent(future, error);
    }

    private StreamsOnAllTasksLostCallbackCompletedEvent invokeOnAllTasksLostCallback(final CompletableFuture<Void> future) {
        final Optional<KafkaException> error;
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onAllTasksLost();
        if (exceptionFromCallback.isPresent()) {
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "All tasks lost callback throws an error"));
        } else {
            error = Optional.empty();
            streamsRebalanceData.setReconciledAssignment(StreamsRebalanceData.Assignment.EMPTY);
        }
        return new StreamsOnAllTasksLostCallbackCompletedEvent(future, error);
    }

    /**
     * Processes all events received from the background thread so far.
     */
    public void process() {
        LinkedList<BackgroundEvent> events = new LinkedList<>();
        onCallbackRequests.drainTo(events);
        for (BackgroundEvent event : events) {
            process(event);
        }
    }

}
