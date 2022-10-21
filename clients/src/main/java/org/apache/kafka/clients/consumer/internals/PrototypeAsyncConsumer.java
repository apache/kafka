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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * This prototype consumer uses the EventHandler to process application events so that the network IO can be processed in a background thread. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 */
public abstract class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {
    private final EventHandler eventHandler;
    private final Time time;

    public PrototypeAsyncConsumer(final Time time, final EventHandler eventHandler) {
        this.time = time;
        this.eventHandler = eventHandler;
    }

    /**
     * poll implementation using {@link EventHandler}.
     *  1. Poll for background events. If there's a fetch response event, process the record and return it. If it is
     *  another type of event, process it.
     *  2. Send fetches if needed.
     *  If the timeout expires, return an empty ConsumerRecord.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord.  It can be empty if time timeout expires.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        try {
            do {
                if (!eventHandler.isEmpty()) {
                    final Optional<BackgroundEvent> backgroundEvent = eventHandler.poll();
                    // processEvent() may process 3 types of event:
                    // 1. Errors
                    // 2. Callback Invocation
                    // 3. Fetch responses
                    // Errors will be handled or rethrown.
                    // Callback invocation will trigger callback function execution, which is blocking until completion.
                    // Successful fetch responses will be added to the completedFetches in the fetcher, which will then
                    // be processed in the collectFetches().
                    backgroundEvent.ifPresent(event -> processEvent(event, timeout));
                }
                // The idea here is to have the background thread sending fetches autonomously, and the fetcher
                // uses the poll loop to retrieve successful fetchResponse and process them on the polling thread.
                final Fetch<K, V> fetch = collectFetches();
                if (!fetch.isEmpty()) {
                    return processFetchResults(fetch);
                }
                // We will wait for retryBackoffMs
            } while (time.timer(timeout).notExpired());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return ConsumerRecords.empty();
    }

    abstract void processEvent(BackgroundEvent backgroundEvent, Duration timeout);

    abstract ConsumerRecords<K, V> processFetchResults(Fetch<K, V> fetch);

    abstract Fetch<K, V> collectFetches();

    /**
     * This method sends a commit event to the EventHandler and return.
     */
    @Override
    public void commitAsync() {
        final ApplicationEvent commitEvent = new CommitApplicationEvent();
        eventHandler.add(commitEvent);
    }

    /**
     * This method sends a commit event to the EventHandler and waits for the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent();
        eventHandler.add(commitEvent);

        final CompletableFuture<Void> commitFuture = commitEvent.commitFuture;
        try {
            commitFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            throw new org.apache.kafka.common.errors.TimeoutException("timeout");
        } catch (final Exception e) {
            // handle exception here
            throw new RuntimeException(e);
        }
    }

    /**
     * A stubbed ApplicationEvent for demonstration purpose
     */
    private class CommitApplicationEvent extends ApplicationEvent {
        // this is the stubbed commitAsyncEvents
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();

        public CommitApplicationEvent() {
        }

        @Override
        public boolean process() {
            return true;
        }
    }
}
