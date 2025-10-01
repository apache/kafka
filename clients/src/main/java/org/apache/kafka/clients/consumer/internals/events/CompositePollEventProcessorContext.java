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

import org.apache.kafka.clients.consumer.internals.CachedSupplier;
import org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.FetchBuffer;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvoker;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * This provides the context for the {@link ApplicationEventProcessor#process(ApplicationEvent)} that invokes the
 * {@link CompositePollEvent} process method. This is mostly to avoid polluting the {@link ApplicationEventProcessor}
 * with instance variables and logic that's specific only to the background {@link CompositePollEvent} processing.
 */
public class CompositePollEventProcessorContext {

    private final Logger log;
    private final NetworkClientDelegate networkClientDelegate;
    private final BackgroundEventHandler backgroundEventHandler;
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final CompletableEventReaper applicationEventReaper;
    private final FetchBuffer fetchBuffer;

    private CompositePollEventProcessorContext(LogContext logContext,
                                               NetworkClientDelegate networkClientDelegate,
                                               BackgroundEventHandler backgroundEventHandler,
                                               OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
                                               CompletableEventReaper applicationEventReaper,
                                               FetchBuffer fetchBuffer) {
        this.log = logContext.logger(getClass());
        this.networkClientDelegate = networkClientDelegate;
        this.backgroundEventHandler = backgroundEventHandler;
        this.offsetCommitCallbackInvoker = offsetCommitCallbackInvoker;
        this.applicationEventReaper = applicationEventReaper;
        this.fetchBuffer = fetchBuffer;
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<CompositePollEventProcessorContext> supplier(LogContext logContext,
                                                                        Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                                                        BackgroundEventHandler backgroundEventHandler,
                                                                        OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
                                                                        CompletableEventReaper applicationEventReaper,
                                                                        FetchBuffer fetchBuffer) {
        return new CachedSupplier<>() {
            @Override
            protected CompositePollEventProcessorContext create() {
                NetworkClientDelegate networkClientDelegate = networkClientDelegateSupplier.get();

                return new CompositePollEventProcessorContext(
                    logContext,
                    networkClientDelegate,
                    backgroundEventHandler,
                    offsetCommitCallbackInvoker,
                    applicationEventReaper,
                    fetchBuffer
                );
            }
        };
    }

    /**
     * To maintain the flow from {@link ClassicKafkaConsumer}, the logic to check and update positions should be
     * allowed to time out before moving on to the logic for sending fetch requests. This achieves that by reusing
     * the {@link CompletableEventReaper} and allowing it to expire the {@link CompletableFuture} for the check and
     * update positions stage.
     */
    public void trackCheckAndUpdatePositionsForTimeout(CompletableFuture<Boolean> updatePositionsFuture, long deadlineMs) {
        CompletableEvent<Boolean> event = new CompletableEvent<>() {
            @Override
            public CompletableFuture<Boolean> future() {
                return updatePositionsFuture;
            }

            @Override
            public long deadlineMs() {
                return deadlineMs;
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + "{updatePositionsFuture=" + updatePositionsFuture + ", deadlineMs=" + deadlineMs + '}';
            }
        };

        applicationEventReaper.add(event);
    }

    /**
     * Helper method that will check if any application thread user callbacks need to be executed. If so, the
     * current event will be completed with {@link CompositePollEvent.State#CALLBACKS_REQUIRED} and this method
     * will return {@code true}. Otherwise, it will return {@code false}.
     */
    public boolean maybeCompleteWithCallbackRequired(CompositePollEvent event, ApplicationEvent.Type nextEventType) {
        // If there are background events to process or enqueued callbacks to invoke, exit to
        // the application thread.
        if (backgroundEventHandler.size() > 0 || offsetCommitCallbackInvoker.size() > 0) {
            log.trace(
                "Pausing polling by completing {} with the state of {} and the next stage of {}",
                event,
                CompositePollEvent.State.CALLBACKS_REQUIRED,
                nextEventType
            );
            event.completeWithCallbackRequired(nextEventType);
            fetchBuffer.wakeup();
            return true;
        }

        return false;
    }

    /**
     * Helper method that checks if there's a non-null error from
     * {@link NetworkClientDelegate#getAndClearMetadataError()} or if the provided exception is not a timeout-based
     * exception. If there's an error to report to the user, the current event will be completed with
     * {@link CompositePollEvent.State#FAILED} and this method will return {@code true}. Otherwise, it will
     * return {@code false}.
     */
    public boolean maybeCompleteExceptionally(CompositePollEvent event, Throwable t) {
        if (maybeCompleteExceptionally(event))
            return true;

        if (t == null)
            return false;

        if (t instanceof org.apache.kafka.common.errors.TimeoutException || t instanceof java.util.concurrent.TimeoutException) {
            log.debug("Ignoring timeout for CompositePollEvent {}: {}", event, t.getMessage());
            return false;
        }

        if (t instanceof CompletionException) {
            t = t.getCause();
        }

        completeExceptionally(event, t);
        return true;
    }

    /**
     * Helper method that checks if there's a non-null error from
     * {@link NetworkClientDelegate#getAndClearMetadataError()}, and if so, reports it to the user by completing the
     * current event with {@link CompositePollEvent.State#FAILED} and returning {@code true}. Otherwise, it will
     * return {@code false}.
     */
    public boolean maybeCompleteExceptionally(CompositePollEvent event) {
        Optional<Exception> exception = networkClientDelegate.getAndClearMetadataError();

        if (exception.isPresent()) {
            completeExceptionally(event, exception.get());
            return true;
        }

        return false;
    }

    /**
     * Helper method to complete the given event with {@link CompositePollEvent.State#FAILED}.
     */
    public void completeExceptionally(CompositePollEvent event, Throwable error) {
        KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(error);
        event.completeExceptionally(e);
        log.trace("Failing event processing for {}", event, e);
    }

    /**
     * Helper method to complete the given event with {@link CompositePollEvent.State#SUCCEEDED}.
     */
    public void complete(CompositePollEvent event) {
        event.completeSuccessfully();
        log.trace("Completed event processing for {}", event);
    }
}