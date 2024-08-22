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

import org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread;
import org.apache.kafka.clients.consumer.internals.ConsumerUtils;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate;
import org.apache.kafka.clients.consumer.internals.RequestManagers;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.io.Closeable;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * An event handler that receives {@link ApplicationEvent application events} from the application thread which
 * are then readable from the {@link ApplicationEventProcessor} in the {@link ConsumerNetworkThread network thread}.
 */
public class ApplicationEventHandler implements Closeable {

    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final ConsumerNetworkThread networkThread;
    private final IdempotentCloser closer = new IdempotentCloser();

    public ApplicationEventHandler(final LogContext logContext,
                                   final Time time,
                                   final BlockingQueue<ApplicationEvent> applicationEventQueue,
                                   final CompletableEventReaper applicationEventReaper,
                                   final Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier,
                                   final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                   final Supplier<RequestManagers> requestManagersSupplier) {
        this.log = logContext.logger(ApplicationEventHandler.class);
        this.applicationEventQueue = applicationEventQueue;
        this.networkThread = new ConsumerNetworkThread(logContext,
                time,
                applicationEventQueue,
                applicationEventReaper,
                applicationEventProcessorSupplier,
                networkClientDelegateSupplier,
                requestManagersSupplier);
        this.networkThread.start();
    }

    /**
     * Add an {@link ApplicationEvent} to the handler and then internally invoke {@link #wakeupNetworkThread()}
     * to alert the network I/O thread that it has something to process.
     *
     * @param event An {@link ApplicationEvent} created by the application thread
     */
    public void add(final ApplicationEvent event) {
        Objects.requireNonNull(event, "ApplicationEvent provided to add must be non-null");
        applicationEventQueue.add(event);
        wakeupNetworkThread();
    }

    /**
     * Wakeup the {@link ConsumerNetworkThread network I/O thread} to pull the next event(s) from the queue.
     */
    public void wakeupNetworkThread() {
        networkThread.wakeup();
    }

    /**
     * Returns the delay for which the application thread can safely wait before it should be responsive
     * to results from the request managers. For example, the subscription state can change when heartbeats
     * are sent, so blocking for longer than the heartbeat interval might mean the application thread is not
     * responsive to changes.
     *
     * @return The maximum delay in milliseconds
     */
    public long maximumTimeToWait() {
        return networkThread.maximumTimeToWait();
    }

    /**
     * Add a {@link CompletableApplicationEvent} to the handler. The method blocks waiting for the result, and will
     * return the result value upon successful completion; otherwise throws an error.
     *
     * <p/>
     *
     * See {@link ConsumerUtils#getResult(Future)} for more details.
     *
     * @param event A {@link CompletableApplicationEvent} created by the polling thread
     * @return      Value that is the result of the event
     * @param <T>   Type of return value of the event
     */
    public <T> T addAndGet(final CompletableApplicationEvent<T> event) {
        Objects.requireNonNull(event, "CompletableApplicationEvent provided to addAndGet must be non-null");
        add(event);
        // Check if the thread was interrupted before we start waiting, to ensure that we
        // propagate the exception even if we end up not having to wait (the event could complete
        // between the time it's added and the time we attempt to getResult)
        if (Thread.interrupted()) {
            throw new InterruptException("Interrupted waiting for results for application event " + event);
        }
        return ConsumerUtils.getResult(event.future());
    }

    @Override
    public void close() {
        close(Duration.ZERO);
    }

    public void close(final Duration timeout) {
        closer.close(
                () -> Utils.closeQuietly(() -> networkThread.close(timeout), "consumer network thread"),
                () -> log.warn("The application event handler was already closed")
        );
    }
}
