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
package org.apache.kafka.coordinator.group.runtime;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.util.ShutdownableThread;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A multithreaded {{@link CoordinatorEvent}} processor which uses a {{@link EventAccumulator}}
 * which guarantees that events sharing a partition key are not processed concurrently.
 */
public class MultiThreadedEventProcessor implements CoordinatorEventProcessor {

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The accumulator.
     */
    private final EventAccumulator<Integer, CoordinatorEvent> accumulator;

    /**
     * The processing threads.
     */
    private final List<EventProcessorThread> threads;

    /**
     * The lock for protecting access to the resources.
     */
    private final ReentrantLock lock;

    /**
     * A boolean indicated whether the event processor is shutting down.
     */
    private volatile boolean shuttingDown;

    /**
     * Constructor.
     *
     * @param logContext    The log context.
     * @param threadPrefix  The thread prefix.
     * @param numThreads    The number of threads.
     */
    public MultiThreadedEventProcessor(
        LogContext logContext,
        String threadPrefix,
        int numThreads
    ) {
        this.log = logContext.logger(MultiThreadedEventProcessor.class);
        this.shuttingDown = false;
        this.lock = new ReentrantLock();
        this.accumulator = new EventAccumulator<>();
        this.threads = IntStream.range(0, numThreads).mapToObj(threadId ->
            new EventProcessorThread(threadPrefix + threadId)
        ).collect(Collectors.toList());
        this.threads.forEach(EventProcessorThread::start);
    }

    /**
     * The event processor thread. The thread pulls events from the
     * accumulator and runs them.
     */
    class EventProcessorThread extends ShutdownableThread {
        EventProcessorThread(
            String name
        ) {
            super(name, false);
        }

        @Override
        public void doWork() {
            while (!shuttingDown) {
                CoordinatorEvent event = accumulator.poll();
                if (event == null) continue;

                try {
                    log.debug("Executing event " + event);
                    event.run();
                } catch (Throwable t) {
                    log.error("Failed to run event " + event + " due to: " + t, t);
                    event.complete(t);
                } finally {
                    accumulator.done(event);
                }
            }

            // The accumulator is drained and all the pending events are rejected
            // when the event processor is shutdown.
            CoordinatorEvent event = accumulator.poll(0, TimeUnit.MILLISECONDS);
            while (event != null) {
                try {
                    log.debug("Draining event " + event);
                    event.complete(new RejectedExecutionException("EventProcessor is closed."));
                } catch (Throwable t) {
                    log.error("Failed to reject event " + event + " due to: " + t, t);
                } finally {
                    accumulator.done(event);
                }

                event = accumulator.poll(0, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * Enqueues a new {{@link CoordinatorEvent}}.
     *
     * @param event The event.
     * @throws RejectedExecutionException If the event processor. is closed.
     */
    @Override
    public void enqueue(CoordinatorEvent event) throws RejectedExecutionException {
        lock.lock();
        try {
            if (shuttingDown)
                throw new RejectedExecutionException("EventProcessor is closed.");

            accumulator.add(event);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Begins the shutdown of the event processor.
     */
    public void beginShutdown() {
        lock.lock();
        try {
            if (shuttingDown) {
                log.debug("Event processor is already shutting down.");
                return;
            }

            log.info("Shutting down event processor.");
            shuttingDown = true;

            threads.forEach(ShutdownableThread::initiateShutdown);
            accumulator.close();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Closes the event processor.
     */
    @Override
    public void close() throws Exception {
        lock.lock();
        try {
            beginShutdown();

            threads.forEach(thread -> {
                try {
                    thread.awaitShutdown();
                } catch (InterruptedException e) {
                    // Ignore.
                }
            });

            log.info("Event processor closed.");
        } finally {
            lock.unlock();
        }
    }
}
