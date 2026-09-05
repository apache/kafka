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
package org.apache.kafka.streams.integration.utils;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

/**
 * A single fault to apply to responses (or, for {@code BLACKHOLE}, requests) of one {@link ApiKeys}. Built
 * through the fluent DSL on {@link KafkaProtocolFaultProxy} ({@code injectError(...)} / {@code disconnectOn(...)}
 * / {@code delayResponse(...)} / {@code blackholeOn(...)} then a trigger), and returned to the caller as a
 * handle so a test can inspect {@link #timesTriggered()} or {@link #remove()} it.
 *
 * <p>A rule counts every request/response of its API it sees ("matches"); the {@link IntPredicate} trigger
 * decides, from the 1-based match count, whether the fault fires on that match — e.g. {@code n -> n == 1} is
 * {@code once()}, {@code n -> n <= 3} is {@code times(3)}.
 */
public final class FaultRule {

    enum Action { INJECT_ERROR, DISCONNECT, DELAY, BLACKHOLE }

    private final KafkaProtocolFaultProxy owner;
    private final ApiKeys apiKey;
    private final Action action;
    private final Errors error; // only for INJECT_ERROR
    private final Duration delay; // only for DELAY
    private final IntPredicate trigger;
    private final String clientIdFilter; // null = any client; otherwise the request clientId must contain it
    private final String description;

    private final AtomicInteger matches = new AtomicInteger(0);
    private final AtomicInteger triggered = new AtomicInteger(0);

    FaultRule(final KafkaProtocolFaultProxy owner,
              final ApiKeys apiKey,
              final Action action,
              final Errors error,
              final Duration delay,
              final IntPredicate trigger,
              final String clientIdFilter,
              final String description) {
        this.owner = owner;
        this.apiKey = apiKey;
        this.action = action;
        this.error = error;
        this.delay = delay;
        this.trigger = trigger;
        this.clientIdFilter = clientIdFilter;
        this.description = description;
    }

    ApiKeys apiKey() {
        return apiKey;
    }

    Action action() {
        return action;
    }

    Errors error() {
        return error;
    }

    Duration delay() {
        return delay;
    }

    /**
     * Whether this rule applies to a request from {@code clientId}. Lets a rule be scoped to a specific
     * client — e.g. {@code forClient("restore")} to fault only the restore consumer's fetches, leaving the
     * main consumer untouched. A null filter matches any client.
     */
    boolean matchesClient(final String clientId) {
        return clientIdFilter == null || (clientId != null && clientId.contains(clientIdFilter));
    }

    /** Called by the proxy for each response of this rule's API; returns true if the fault should fire. */
    boolean shouldFire() {
        final int n = matches.incrementAndGet();
        if (trigger.test(n)) {
            triggered.incrementAndGet();
            return true;
        }
        return false;
    }

    /** How many times this fault actually fired — for test assertions. */
    public int timesTriggered() {
        return triggered.get();
    }

    /** How many responses of this API the rule has observed (whether or not it fired). */
    public int timesMatched() {
        return matches.get();
    }

    /** Deregister this rule from the proxy. */
    public void remove() {
        owner.removeFault(this);
    }

    @Override
    public String toString() {
        return "FaultRule(" + description + ", triggered=" + triggered.get() + "/" + matches.get() + ")";
    }

    // ------------------------------------------------------------------
    // Fluent trigger step: returned by injectError(...)/disconnectOn(...)/delayResponse(...)/blackholeOn(...).
    // Each terminal registers the built rule with the proxy and returns the handle.
    // ------------------------------------------------------------------
    public static final class Builder {
        private final KafkaProtocolFaultProxy owner;
        private final ApiKeys apiKey;
        private final Action action;
        private final Errors error;
        private final Duration delay;
        private String clientIdFilter; // null = any client

        Builder(final KafkaProtocolFaultProxy owner, final ApiKeys apiKey, final Action action,
                final Errors error, final Duration delay) {
            this.owner = owner;
            this.apiKey = apiKey;
            this.action = action;
            this.error = error;
            this.delay = delay;
        }

        /**
         * Scope this fault to requests whose clientId contains {@code substring} — e.g.
         * {@code forClient("restore")} to fault only the restore consumer (leaving the main consumer and
         * producer untouched). Without this, the rule matches any client.
         */
        public Builder forClient(final String substring) {
            this.clientIdFilter = substring;
            return this;
        }

        private FaultRule register(final IntPredicate trigger, final String triggerDesc) {
            final String verb;
            switch (action) {
                case DISCONNECT:
                    verb = "disconnect";
                    break;
                case BLACKHOLE:
                    verb = "blackhole (drop request before it reaches the broker)";
                    break;
                case DELAY:
                    verb = "delay response by " + delay;
                    break;
                default:
                    verb = "inject " + error;
            }
            final String scope = clientIdFilter == null ? "" : " client~" + clientIdFilter;
            final FaultRule rule = new FaultRule(owner, apiKey, action, error, delay, trigger, clientIdFilter,
                    verb + " on " + apiKey + scope + " [" + triggerDesc + "]");
            owner.addFault(rule);
            return rule;
        }

        /** Fire on the first matching response only. */
        public FaultRule once() {
            return register(Occurrence.once(), "once");
        }

        /** Fire on exactly the {@code n}-th matching response (1-based). */
        public FaultRule onCall(final int n) {
            return register(Occurrence.call(n), "call #" + n);
        }

        /** Fire on the first {@code n} matching responses. */
        public FaultRule times(final int n) {
            return register(Occurrence.first(n), "first " + n);
        }

        /** Fire on every matching response until removed. */
        public FaultRule everyTime() {
            return register(Occurrence.always(), "every time");
        }

        /** Fire on each matching response with the given probability (chaos mode; non-deterministic). */
        public FaultRule withProbability(final double p) {
            return register(Occurrence.withProbability(p), "p=" + p);
        }
    }
}
