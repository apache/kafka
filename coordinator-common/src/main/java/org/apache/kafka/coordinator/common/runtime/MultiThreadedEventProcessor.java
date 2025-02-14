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
package org.apache.kafka.coordinator.common.runtime;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

/**
 * A multithreaded {{@link CoordinatorEvent}} processor which uses a {{@link EventAccumulator}}
 * which guarantees that events sharing a partition key are not processed concurrently.
 */
public final class MultiThreadedEventProcessor implements CoordinatorEventProcessor {

    /**
     * The poll timeout to wait for an event by the EventProcessorThread.
     */
    private static final long POLL_TIMEOUT_MS = 300L;

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The accumulator.
     */
    private final EventAccumulator<TopicPartition, CoordinatorEvent> accumulator;

    /**
     * The processing threads.
     */
    private final List<EventProcessorThread> threads;

    /**
     * A boolean indicated whether the event processor is shutting down.
     */
    private volatile boolean shuttingDown;

    /**
     * The coordinator runtime metrics.
     */
    private final CoordinatorRuntimeMetrics metrics;

    /**
     * The time.
     */
    private final Time time;

    public MultiThreadedEventProcessor(
        LogContext logContext,
        String threadPrefix,
        int numThreads,
        Time time,
        CoordinatorRuntimeMetrics metrics
    ) {
        this(logContext, threadPrefix, numThreads, time, metrics, new EventAccumulator<>());
    }

    /**
     * Constructor.
     *
     * @param logContext        The log context.
     * @param threadPrefix      The thread prefix.
     * @param numThreads        The number of threads.
     * @param metrics           The coordinator runtime metrics.
     * @param time              The time.
     * @param eventAccumulator  The event accumulator.
     */
    public MultiThreadedEventProcessor(
        LogContext logContext,
        String threadPrefix,
        int numThreads,
        Time time,
        CoordinatorRuntimeMetrics metrics,
        EventAccumulator<TopicPartition, CoordinatorEvent> eventAccumulator
    ) {
        this.log = logContext.logger(MultiThreadedEventProcessor.class);
        this.shuttingDown = false;
        this.accumulator = eventAccumulator;
        this.time = Objects.requireNonNull(time);
        this.metrics = Objects.requireNonNull(metrics);
        this.metrics.registerEventQueueSizeGauge(accumulator::size);
        this.threads = IntStream.range(0, numThreads).mapToObj(threadId ->
            new EventProcessorThread(
                threadPrefix + threadId
            )
        ).toList();
        this.threads.forEach(EventProcessorThread::start);
    }

    /**
     * The event processor thread. The thread pulls events from the
     * accumulator and runs them.
     */
    private class EventProcessorThread extends Thread {
        private final Logger log;

        EventProcessorThread(
            String name
        ) {
            super(name);
            log = new LogContext("[" + name + "]: ").logger(EventProcessorThread.class);
            setDaemon(false);
        }

        private void handleEvents() {
            while (!shuttingDown) {
                // We use a single meter for aggregate idle percentage for the thread pool.
                // Since meter is calculated as total_recorded_value / time_window and
                // time_window is independent of the number of threads, each recorded idle
                // time should be discounted by # threads.

                long idleStartTimeMs = time.milliseconds();
                CoordinatorEvent event = accumulator.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                long idleEndTimeMs = time.milliseconds();
                long idleTimeMs = idleEndTimeMs - idleStartTimeMs;
                metrics.recordThreadIdleTime(idleTimeMs / threads.size());
                if (event != null) {
                    try {
                        log.debug("Executing event: {}.", event);
                        long dequeuedTimeMs = time.milliseconds();
                        metrics.recordEventQueueTime(dequeuedTimeMs - event.createdTimeMs());
                        event.run();
                        metrics.recordEventProcessingTime(time.milliseconds() - dequeuedTimeMs);
                    } catch (Throwable t) {
                        log.error("Failed to run event {} due to: {}.", event, t.getMessage(), t);
                        event.complete(t);
                    } finally {
                        accumulator.done(event);
                    }
                }
            }
        }

        private void drainEvents() {
            CoordinatorEvent event;
            while ((event = accumulator.poll()) != null) {
                try {
                    log.debug("Draining event: {}.", event);
                    metrics.recordEventQueueTime(time.milliseconds() - event.createdTimeMs());
                    event.complete(new RejectedExecutionException("EventProcessor is closed."));
                } catch (Throwable t) {
                    log.error("Failed to reject event {} due to: {}.", event, t.getMessage(), t);
                } finally {
                    accumulator.done(event);
                }
            }
        }

        @Override
        public void run() {
            log.info("Starting");

            try {
                handleEvents();
            } catch (Throwable t) {
                log.error("Exiting with exception.", t);
            }

            // The accumulator is drained and all the pending events are rejected
            // when the event processor is shutdown.
            if (shuttingDown) {
                log.info("Shutting down. Draining the remaining events.");
                try {
                    drainEvents();
                } catch (Throwable t) {
                    log.error("Draining threw exception.", t);
                }
                log.info("Shutdown completed");
            }
        }
    }

    /**
     * Enqueues a new {{@link CoordinatorEvent}} at the end of the processor.
     *
     * @param event The event.
     * @throws RejectedExecutionException If the event processor is closed.
     */
    @Override
    public void enqueueLast(CoordinatorEvent event) throws RejectedExecutionException {
        accumulator.addLast(event);
    }

    /**
     * Enqueues a new {{@link CoordinatorEvent}} at the front of the processor.
     *
     * @param event The event.
     * @throws RejectedExecutionException If the event processor is closed.
     */
    @Override
    public void enqueueFirst(CoordinatorEvent event) throws RejectedExecutionException {
        accumulator.addFirst(event);
    }

    /**
     * Begins the shutdown of the event processor.
     */
    public synchronized void beginShutdown() {
        if (shuttingDown) {
            log.debug("Event processor is already shutting down.");
            return;
        }

        log.info("Shutting down event processor.");
        // The accumulator must be closed first to ensure that new events are
        // rejected before threads are notified to shutdown and start to drain
        // the accumulator.
        accumulator.close();
        shuttingDown = true;
    }

    /**
     * Closes the event processor.
     */
    @Override
    public void close() throws InterruptedException {
        beginShutdown();
        for (Thread t : threads) {
            t.join();
        }
        log.info("Event processor closed.");
    }
}
