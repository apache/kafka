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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.KafkaThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class KafkaAsyncConsumerBackend extends KafkaThread implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(KafkaAsyncConsumerBackend.class);

    private final AtomicLong requestCounter = new AtomicLong();

    private final BlockingQueue<Request> eventQueue;

    private volatile boolean running;

    public KafkaAsyncConsumerBackend() {
        super("consumer_background_thread", true);
        this.eventQueue = new ArrayBlockingQueue<>(500);
    }

    @Override
    public void close() {
        this.running = false;
        log.warn("close invoked - running: {}", running, new Exception("here's who closed me"));
    }

    public synchronized <T> Future<T> submit(Supplier<T> supplier) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Runnable runnable = () -> {
            try {
                T value = supplier.get();
                future.complete(value);
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        };

        Request request = new Request(runnable);
        log.warn("submit - supplier: {}, running: {}, request: {}", supplier, running, request.id);

        if (!eventQueue.offer(request))
            throw new KafkaException("Can't offer runnable");

        return future;
    }

    public void submit(Runnable runnable) {
        submit(() -> {
            runnable.run();
            return null;
        });
    }

    @Override
    public void run() {
        try {
            log.warn("run - started");
            running = true;

            while (running) {
                try {
                    Request request = eventQueue.poll(1000, TimeUnit.MILLISECONDS);

                    if (request != null) {
                        request.run();
                    }
                } catch (final WakeupException e) {
                    log.warn("Exception thrown, background thread won't terminate", e);
                    // swallow the wakeup exception to prevent killing the
                    // background thread.
                }
            }
        } catch (final Throwable t) {
            log.error("The background thread failed due to unexpected error", t);
        } finally {
            log.warn("run - finished");
        }
    }

    private class Request implements Runnable {

        private final long id;

        private final Runnable runnable;

        public Request(Runnable runnable) {
            this.id = requestCounter.incrementAndGet();
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                log.warn("run - starting run for request {}", id);
                runnable.run();
            } finally {
                log.warn("run - finished run for request {}", id);
            }
        }

    }

}
