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
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

/**
 * This processor consumes {@code ApplicationEvent} and produces {@code BackgroundEvent}. The processor uses a state
 * machine to manage its connection to the coordinator.  Because the coordinator connection is on demand, the processor
 * can stay in the INITIALIZED state and continue to function.  When the coordinator is needed, the processor will
 * issue a FindCoordinatorRequest and transition the state to the FINDING_COORDINATIOR stage, and may eventually end
 * up in either INITALIZED state if the request failed, or STABLE if the request succeed.  It is also possible to
 * transition to DOWN from any STATE.
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
 *         |&lt;------|  FIND_COORD  |      |
 *         |       +------+-------+      |
 *         |              |              |
 *         |              v              |
 *         |       +------+-------+      |
 *(closed) +------ |   STABLE     | -----+ (disconnected)
 *                 +------+-------+
 * </pre>
 * <ul>
 * <li>DOWN: The processor is closed or uninitialized.</li>
 * <li>INITIALIZED: The processor has been initialized. It can start to consume {@code ApplicationEvent}.</li>
 * <li>FINDING_COORDINATOR: The processor send out a {@code FindCoordinatorRequest} and is waiting for a
 * response.</li>
 * <li>STABLE: The background thread is connected to the coordinator.  Note that rebalancing can only happen
 * in this state.</li>
 * </ul>
 */
public class DefaultEventProcessor implements EventProcessor {
    private final Logger log;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final Time time;
    private final ConsumerConfig config;

    private long retryBackoffMs;
    private BackgroundState state;
    private boolean running = false;
    private Optional<ApplicationEvent> inflightEvent;

    public DefaultEventProcessor(ConsumerConfig config,
                                 LogContext logContext,
                                 BlockingQueue<ApplicationEvent> applicationEventQueue,
                                 BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this(Time.SYSTEM,
                config,
                logContext,
                applicationEventQueue,
                backgroundEventQueue);
    }

    public DefaultEventProcessor(Time time,
                                 ConsumerConfig config,
                                 LogContext logContext,
                                 BlockingQueue<ApplicationEvent> applicationEventQueue,
                                 BlockingQueue<BackgroundEvent> backgroundEventQueue) {
        this.time = time;
        this.log = logContext.logger(DefaultEventProcessor.class);
        this.applicationEventQueue = applicationEventQueue;
        this.backgroundEventQueue = backgroundEventQueue;
        this.config = config;
        setConfig();
        this.state = BackgroundState.INITIALIZED;
        this.inflightEvent = Optional.empty();
    }

    private void setConfig() {
        this.retryBackoffMs = this.config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
    }

    /**
     * The main processor loop.
     */
    @Override
    public void run() {
        try {
            log.debug("DefaultEventProcessor started");
            while (running) {
                pollOnce();
                this.wait(retryBackoffMs);
            }
        } catch (Exception e) {
            // TODO: Define fine grain exceptions here
        } finally {
            log.debug("DefaultEventProcessor closed");
        }
    }

    /**
     * Process event from a single poll
     */
    void pollOnce() {
        inflightEvent = maybePollEvent();
        maybeConsumeInflightEvent();
    }

    public Optional<ApplicationEvent> maybePollEvent() {
        if (inflightEvent.isPresent()) {
            return Optional.empty();
        }
        return Optional.ofNullable(applicationEventQueue.poll());
    }

    private void transitionTo(BackgroundState newState) {
        if (!state.isValidTransition(newState)) {
            throw new IllegalStateException("unable to transition from " + state + " to " + newState);
        }
        state = newState;
    }

    public void maybeConsumeInflightEvent() {
        if (!inflightEvent.isPresent()) {
            return;
        }
        ApplicationEvent event = inflightEvent.get();
        if (!tryConsumeEvent(event)) {
            return;
        }
        // clear inflight event upon successful consumption
        inflightEvent = Optional.empty();
    }

    public boolean tryConsumeEvent(ApplicationEvent event) {
        // consumption maybe return false when
        // 1. need a coordinator and it's not available
        // 2. other errors
        return true;
    }

    @Override
    public void close() {
        this.running = false;
        transitionTo(BackgroundState.DOWN);
    }

    /**
     * Four states that the background thread can be in.
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
}