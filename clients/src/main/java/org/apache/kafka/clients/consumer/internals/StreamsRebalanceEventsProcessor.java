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

public class StreamsRebalanceEventsProcessor {

    private final BlockingQueue<BackgroundEvent> onCallbackRequests = new LinkedBlockingQueue<>();
    private ApplicationEventHandler applicationEventHandler = null;
    private final StreamsGroupRebalanceCallbacks rebalanceCallbacks;
    private final StreamsRebalanceData streamsRebalanceData;

    public StreamsRebalanceEventsProcessor(StreamsRebalanceData streamsRebalanceData,
                                           StreamsGroupRebalanceCallbacks rebalanceCallbacks) {
        this.streamsRebalanceData = streamsRebalanceData;
        this.rebalanceCallbacks = rebalanceCallbacks;
    }

    public CompletableFuture<Void> requestOnTasksAssignedCallbackInvocation(final StreamsRebalanceData.Assignment assignment) {
        final StreamsOnTasksAssignedCallbackNeededEvent onTasksAssignedCallbackNeededEvent = new StreamsOnTasksAssignedCallbackNeededEvent(assignment);
        onCallbackRequests.add(onTasksAssignedCallbackNeededEvent);
        return onTasksAssignedCallbackNeededEvent.future();
    }

    public CompletableFuture<Void> requestOnTasksRevokedCallbackInvocation(final Set<StreamsRebalanceData.TaskId> activeTasksToRevoke) {
        final StreamsOnTasksRevokedCallbackNeededEvent onTasksRevokedCallbackNeededEvent = new StreamsOnTasksRevokedCallbackNeededEvent(activeTasksToRevoke);
        onCallbackRequests.add(onTasksRevokedCallbackNeededEvent);
        return onTasksRevokedCallbackNeededEvent.future();
    }

    public CompletableFuture<Void> requestOnAllTasksLostCallbackInvocation() {
        final StreamsOnAllTasksLostCallbackNeededEvent onAllTasksLostCallbackNeededEvent = new StreamsOnAllTasksLostCallbackNeededEvent();
        onCallbackRequests.add(onAllTasksLostCallbackNeededEvent);
        return onAllTasksLostCallbackNeededEvent.future();
    }

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
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onTasksRevoked(activeTasksToRevoke);
        return exceptionFromCallback
            .map(exception ->
                new StreamsOnTasksRevokedCallbackCompletedEvent(
                    future,
                    Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exception, "Task revocation callback throws an error"))
                ))
            .orElseGet(() -> new StreamsOnTasksRevokedCallbackCompletedEvent(future, Optional.empty()));
    }

    private StreamsOnTasksAssignedCallbackCompletedEvent invokeOnTasksAssignedCallback(final StreamsRebalanceData.Assignment assignment,
                                                                                       final CompletableFuture<Void> future) {
        Optional<KafkaException> error = Optional.empty();
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onTasksAssigned(assignment);
        if (exceptionFromCallback.isPresent()) {
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "Task assignment callback throws an error"));
        } else {
            streamsRebalanceData.setReconciledAssignment(assignment);
        }
        return new StreamsOnTasksAssignedCallbackCompletedEvent(future, error);
    }

    private StreamsOnAllTasksLostCallbackCompletedEvent invokeOnAllTasksLostCallback(final CompletableFuture<Void> future) {
        final Optional<Exception> exceptionFromCallback = rebalanceCallbacks.onAllTasksLost();
        final Optional<KafkaException> error;
        if (exceptionFromCallback.isPresent()) {
            error = Optional.of(ConsumerUtils.maybeWrapAsKafkaException(exceptionFromCallback.get(), "All tasks lost callback throws an error"));
        } else {
            error = Optional.empty();
            streamsRebalanceData.setReconciledAssignment(StreamsRebalanceData.Assignment.EMPTY);
        }

        return new StreamsOnAllTasksLostCallbackCompletedEvent(future, error);
    }

    public void process() {
        LinkedList<BackgroundEvent> events = new LinkedList<>();
        onCallbackRequests.drainTo(events);
        for (BackgroundEvent event : events) {
            process(event);
        }
    }

}
