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

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The NoopEventHandler uses a background thread to process events in the ApplicationEventQueue. The background thread
 * performs two simple tasks. First, it polls ApplicationEvents off the queue and logs a message for each event it
 * consumes. Second, it handles the exception by sending an ExceptionBackgroundEvent to the backgroundEventQueue.
 */
public class NoopEventHandler implements EventHandler {
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final Thread backgroundThread;
    private Runnable noopProcessor;

    public NoopEventHandler() {
        LogContext logContext = new LogContext("stubbed_event_handler");
        this.log = logContext.logger(NoopEventHandler.class);
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        this.noopProcessor = new NoopProcessor(this.applicationEventQueue, this.backgroundEventQueue);
        this.backgroundThread = new KafkaThread("stubbed_backgroundThread", this.noopProcessor, true);
        backgroundThread.start();
    }

    @Override
    public Optional<BackgroundEvent> poll() {
        return Optional.ofNullable(backgroundEventQueue.poll());
    }

    @Override
    public boolean isEmpty() {
        return backgroundEventQueue.isEmpty();
    }

    @Override
    public boolean add(ApplicationEvent event) {
        return applicationEventQueue.add(event);
    }

    private class NoopProcessor implements Runnable {
        private static final long RETRY_BACKOFF_MS = 100;
        private final BlockingQueue<ApplicationEvent> applicationEventQueue;
        private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
        private volatile boolean running;

        NoopProcessor(BlockingQueue<ApplicationEvent> applicationEventQueue,
                      BlockingQueue<BackgroundEvent> backgroundEventQueue) {
            this.applicationEventQueue = applicationEventQueue;
            this.backgroundEventQueue = backgroundEventQueue;
            this.running = true;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    if (!applicationEventQueue.isEmpty()) {
                        ApplicationEvent event = applicationEventQueue.poll();
                        processEvent(event);
                    }
                    this.wait(RETRY_BACKOFF_MS);
                } catch (InterruptException e) {
                    Thread.interrupted();
                    log.error("unexpected interruption", e);
                } catch (Exception e) {
                    backgroundEventQueue.add(new ExceptionBackgroundEvent(e));
                    log.error("Exception while processing events", e);
                }
            }
        }

        private void processEvent(ApplicationEvent event) {
            log.info("processing event: " + event);
        }
    }

    static class ExceptionBackgroundEvent extends BackgroundEvent {
        public final static String EVENT_TYPE = "exception";
        public final Exception exception;
        public ExceptionBackgroundEvent(Exception exception) {
            this.exception = exception;
        }
    }
}
