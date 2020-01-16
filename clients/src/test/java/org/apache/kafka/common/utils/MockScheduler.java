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
package org.apache.kafka.common.utils;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public class MockScheduler implements Scheduler, MockTime.MockTimeListener {
    private static final Logger log = LoggerFactory.getLogger(MockScheduler.class);

    /**
     * The MockTime object.
     */
    private final MockTime time;

    /**
     * Futures which are waiting for a specified wall-clock time to arrive.
     */
    private final TreeMap<Long, List<KafkaFutureImpl<Long>>> waiters = new TreeMap<>();

    public MockScheduler(MockTime time) {
        this.time = time;
        time.addListener(this);
    }

    @Override
    public Time time() {
        return time;
    }

    @Override
    public synchronized void tick() {
        long timeMs = time.milliseconds();
        while (true) {
            Map.Entry<Long, List<KafkaFutureImpl<Long>>> entry = waiters.firstEntry();
            if ((entry == null) || (entry.getKey() > timeMs)) {
                break;
            }
            for (KafkaFutureImpl<Long> future : entry.getValue()) {
                future.complete(timeMs);
            }
            waiters.remove(entry.getKey());
        }
    }

    public synchronized void addWaiter(long delayMs, KafkaFutureImpl<Long> waiter) {
        long timeMs = time.milliseconds();
        if (delayMs <= 0) {
            waiter.complete(timeMs);
        } else {
            long triggerTimeMs = timeMs + delayMs;
            List<KafkaFutureImpl<Long>> futures = waiters.get(triggerTimeMs);
            if (futures == null) {
                futures = new ArrayList<>();
                waiters.put(triggerTimeMs, futures);
            }
            futures.add(waiter);
        }
    }

    @Override
    public <T> Future<T> schedule(final ScheduledExecutorService executor,
                                  final Callable<T> callable, long delayMs) {
        final KafkaFutureImpl<T> future = new KafkaFutureImpl<>();
        KafkaFutureImpl<Long> waiter = new KafkaFutureImpl<>();
        waiter.thenApply(new KafkaFuture.BaseFunction<Long, Void>() {
            @Override
            public Void apply(final Long now) {
                executor.submit(new Callable<Void>() {
                    @Override
                    public Void call() {
                        // Note: it is possible that we'll execute Callable#call right after
                        // the future is cancelled.  This is a valid sequence of events
                        // that the author of the Callable needs to be able to handle.
                        //
                        // Note 2: If the future is cancelled, we will not remove the waiter
                        // from this MockTime object.  This small bit of inefficiency is acceptable
                        // in testing code (at least we aren't polling!)
                        if (!future.isCancelled()) {
                            try {
                                log.trace("Invoking {} at {}", callable, now);
                                future.complete(callable.call());
                            } catch (Throwable throwable) {
                                future.completeExceptionally(throwable);
                            }
                        }
                        return null;
                    }
                });
                return null;
            }
        });
        log.trace("Scheduling {} for {} ms from now.", callable, delayMs);
        addWaiter(delayMs, waiter);
        return future;
    }
}
