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
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.OffsetCommitCallbackInvoker;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

public class CompositePollEventProcessorContext {

    private final Logger log;
    private final NetworkClientDelegate networkClientDelegate;
    private final BackgroundEventHandler backgroundEventHandler;
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final CompletableEventReaper applicationEventReaper;

    private CompositePollEventProcessorContext(LogContext logContext,
                                               NetworkClientDelegate networkClientDelegate,
                                               BackgroundEventHandler backgroundEventHandler,
                                               OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
                                               CompletableEventReaper applicationEventReaper) {
        this.log = logContext.logger(getClass());
        this.networkClientDelegate = networkClientDelegate;
        this.backgroundEventHandler = backgroundEventHandler;
        this.offsetCommitCallbackInvoker = offsetCommitCallbackInvoker;
        this.applicationEventReaper = applicationEventReaper;
    }

    public static Supplier<CompositePollEventProcessorContext> supplier(LogContext logContext,
                                                                        Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                                                        BackgroundEventHandler backgroundEventHandler,
                                                                        OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
                                                                        CompletableEventReaper applicationEventReaper) {
        return new CachedSupplier<>() {
            @Override
            protected CompositePollEventProcessorContext create() {
                NetworkClientDelegate networkClientDelegate = networkClientDelegateSupplier.get();

                return new CompositePollEventProcessorContext(
                    logContext,
                    networkClientDelegate,
                    backgroundEventHandler,
                    offsetCommitCallbackInvoker,
                    applicationEventReaper
                );
            }
        };
    };

    public <T> void trackExpirableEvent(CompletableFuture<T> future, long deadlineMs) {
        CompletableEvent<T> event = new CompletableEvent<>() {
            @Override
            public CompletableFuture<T> future() {
                return future;
            }

            @Override
            public long deadlineMs() {
                return deadlineMs;
            }

            @Override
            public String toString() {
                return getClass().getSimpleName() + "{future=" + future + ", deadlineMs=" + deadlineMs + '}';
            }
        };

        applicationEventReaper.add(event);
    }

    public boolean maybePauseCompositePoll(CompositePollEvent event, ApplicationEvent.Type nextEventType) {
        // If there are background events to process or enqueued callbacks to invoke, exit to
        // the application thread.
        if (backgroundEventHandler.size() > 0 || offsetCommitCallbackInvoker.size() > 0) {
            CompositePollEvent.State state = CompositePollEvent.State.CALLBACKS_REQUIRED;
            log.debug("Pausing event processing for {} with {} as next step", state, nextEventType);
            event.complete(state, Optional.of(nextEventType));
            return true;
        }

        return false;
    }

    public boolean maybeFailCompositePoll(CompositePollEvent event, Throwable t) {
        if (maybeFailCompositePoll(event))
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

        KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);
        event.completeExceptionally(e);
        log.debug("Failing event processing for {}", event, e);
        return true;
    }

    public boolean maybeFailCompositePoll(CompositePollEvent event) {
        Optional<Exception> exception = networkClientDelegate.getAndClearMetadataError();

        if (exception.isPresent()) {
            KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(exception.get());
            event.completeExceptionally(e);
            log.debug("Failing event processing for {}", event, e);
            return true;
        }

        return false;
    }
}