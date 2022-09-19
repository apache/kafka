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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * The background thread runs in the background and consumes the {@code ApplicationEvent} from the {@link org.apache.kafka.clients.consumer.KafkaConsumer} APIs. This class uses an event loop to drive the following important tasks:
 * <ul>
 *  <li>Consuming and executing the {@code ApplicationEvent}.</li>
 *  <li>Maintaining the connection to the coordinator.</li>
 *  <li>Sending heartbeat.</li>
 *  <li>Autocommitting.</li>
 *  <li>Executing the rebalance flow.</li>
 * </ul>
 */
public class ConsumerBackgroundThread extends KafkaThread {
    private final Logger log;
    private static final String CONSUMER_BACKGROUND_THREAD_PREFIX = "consumer_background_thread";
    private long retryBackoffMs;
    private BackgroundState state = BackgroundState.DOWN;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final Time time;

    private final ConsumerConfig config;

    // control variables
    private boolean running = false;
    private Optional<ApplicationEvent> inflightEvent;
    private boolean needCoordinator;

    public ConsumerBackgroundThread(ConsumerConfig config,
                                    LogContext logContext,
                                    BlockingQueue<ApplicationEvent> applicationEventQueue,
                                    BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this(Time.SYSTEM,
                config,
                logContext,
                applicationEventQueue,
                backgroundEventQueue);
    }

    public ConsumerBackgroundThread(Time time,
                                    ConsumerConfig config,
                                    LogContext logContext,
                                    BlockingQueue<ApplicationEvent> applicationEventQueue,
                                    BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        super(CONSUMER_BACKGROUND_THREAD_PREFIX, true);
        this.time = Time.SYSTEM;
        this.log = logContext.logger(ConsumerBackgroundThread.class);
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.config = config;
        setConfig(config);
        this.state = BackgroundState.INITIALIZED;
    }

    private void setConfig(ConsumerConfig config) {
        this.retryBackoffMs = this.config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    @Override
    public void run() {
        try {
            log.debug("Background thread started");
            while(running) {
                if (!inflightEvent.isPresent() && !applicationEventQueue.isEmpty()) {
                    inflightEvent = Optional.ofNullable(applicationEventQueue.poll());
                }
                if(inflightEvent.isPresent()) {
                    needCoordinator = inflightEvent.get().needCoordinator;
                    tryConsumeInflightEvent();
                }
                runStateMachine();
                this.wait(retryBackoffMs);
            }
        } catch(Exception e) {
            // TODO: We need more fine grain exception and handle them differently
        } finally {
            log.debug("Background thread closed");
        }
    }

    public void close() throws Exception {

        this.running = false;
        transitionTo(BackgroundState.DOWN);
    }

    private void runStateMachine() {
        switch (state) {
            case DOWN:
                // closing
                this.running = false;
                return;
            case INITIALIZED:
                maybeFindCoordinator();
                break;
            case FINDING_COORDINATOR:
                maybeTransitionToStable();
                break;
            case STABLE:
                // poll coordinator
                break;
        }
    }

    /**
     * BackgroundState represents BackgroundThread's connection status to the coordinator.  It can be in one of the four
     * possible states.  The state transition can be represented here:
     * <pre>
     *                 +--------------+
     *         +-----&gt; |     DOWN     |
     *         |       +-----+--------+
     *         |              |
     *         |              v
     *         |       +----+--+------+
     *         |&lt;------| INITIALIZED  | &lt;----+
     *         |       +-----+-+------+      |
     *         |             | ^             |
     *         |             v |             |
     *         |       +--------------+      |
     *         |&lt;------| FINDING_COOD |      |
     *         |       +------+-------+      |
     *         |              |              |
     *         |              v              |
     *         |       +------+-------+      |
     *(closed) +------ |   STABLE     | -----+ (disconnected)
     *                 +------+-------+
     * <pre/>
     */
    enum BackgroundState {
        DOWN(1),
        INITIALIZED(0, 2),
        FINDING_COORDINATOR(1, 2, 3),
        STABLE(0, 1, 2);

        private final Set<Integer> validTransition = new HashSet<>();
        BackgroundState(final Integer... validTransitions) {
            this.validTransition.addAll(Arrays.asList(validTransitions));
        }
        boolean isValidTransition(final BackgroundState newState) {
            return validTransition.contains(newState.ordinal());
        }
    }

    private void maybeTransitionToStable() {
        // TODO: to be implemented
    }
    private void maybeFindCoordinator() {
        if (!needCoordinator) {
            return;
        }
        // find coordinator
        transitionTo(BackgroundState.FINDING_COORDINATOR);
    }

    private void transitionTo(BackgroundState newState) {
        if (!state.isValidTransition(newState)) {
            throw new IllegalStateException("unable to transition from " + state + " to " + newState);
        }
        state = newState;
    }

    public void tryConsumeInflightEvent() {
        // TODO: to be implemented
    }
}