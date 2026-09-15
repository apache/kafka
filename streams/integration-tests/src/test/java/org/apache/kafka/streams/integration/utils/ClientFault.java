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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

/**
 * A single client-side exception fault applied to one {@link FaultInjectingClientSupplier.ProducerCall}.
 * Built through the fluent DSL on {@link FaultInjectingClientSupplier}
 * ({@code failOn(call, exceptionSupplier)} then a trigger) and returned to the caller as a handle so a test
 * can inspect {@link #timesTriggered()} or {@link #remove()} it.
 *
 * <p>Unlike {@link FaultRule} (which rewrites a broker <em>response</em> on the wire), a {@code ClientFault}
 * makes the wrapped producer throw a Java exception <em>before</em> the call reaches the network — modelling
 * client-library failures (fenced producers, timeouts, invalid-txn-state) that never appear as a broker
 * error code. This is the leaner, Streams-specific complement to {@link KafkaProtocolFaultProxy}.
 */
public final class ClientFault {

    private final FaultInjectingClientSupplier owner;
    private final FaultInjectingClientSupplier.ProducerCall call;
    private final Supplier<? extends RuntimeException> exception;
    private final IntPredicate trigger;
    private final String description;

    private final AtomicInteger matches = new AtomicInteger(0);
    private final AtomicInteger triggered = new AtomicInteger(0);

    ClientFault(final FaultInjectingClientSupplier owner,
                final FaultInjectingClientSupplier.ProducerCall call,
                final Supplier<? extends RuntimeException> exception,
                final IntPredicate trigger,
                final String description) {
        this.owner = owner;
        this.call = call;
        this.exception = exception;
        this.trigger = trigger;
        this.description = description;
    }

    FaultInjectingClientSupplier.ProducerCall call() {
        return call;
    }

    /**
     * Called by the wrapped producer before it invokes {@code call}. If the trigger fires, returns the
     * exception to throw; otherwise returns {@code null} and the real call proceeds.
     */
    RuntimeException maybeFail() {
        final int n = matches.incrementAndGet();
        if (trigger.test(n)) {
            triggered.incrementAndGet();
            return exception.get();
        }
        return null;
    }

    /** How many times this fault actually fired — for test assertions. */
    public int timesTriggered() {
        return triggered.get();
    }

    /** How many invocations of this call the fault has observed (whether or not it fired). */
    public int timesMatched() {
        return matches.get();
    }

    /** Deregister this fault from the supplier. */
    public void remove() {
        owner.removeFault(this);
    }

    @Override
    public String toString() {
        return "ClientFault(" + description + ", triggered=" + triggered.get() + "/" + matches.get() + ")";
    }

    // ------------------------------------------------------------------
    // Fluent trigger step: returned by failOn(...). Each terminal registers
    // the built fault with the supplier and returns the handle.
    // ------------------------------------------------------------------
    public static final class Builder {
        private final FaultInjectingClientSupplier owner;
        private final FaultInjectingClientSupplier.ProducerCall call;
        private final Supplier<? extends RuntimeException> exception;

        Builder(final FaultInjectingClientSupplier owner,
                final FaultInjectingClientSupplier.ProducerCall call,
                final Supplier<? extends RuntimeException> exception) {
            this.owner = owner;
            this.call = call;
            this.exception = exception;
        }

        private ClientFault register(final IntPredicate trigger, final String triggerDesc) {
            final ClientFault fault = new ClientFault(owner, call, exception, trigger,
                    "throw on " + call + " [" + triggerDesc + "]");
            owner.addFault(fault);
            return fault;
        }

        /** Throw on the first invocation only. */
        public ClientFault once() {
            return register(Occurrence.once(), "once");
        }

        /** Throw on exactly the {@code n}-th invocation (1-based). */
        public ClientFault onCall(final int n) {
            return register(Occurrence.call(n), "call #" + n);
        }

        /** Throw on the first {@code n} invocations. */
        public ClientFault times(final int n) {
            return register(Occurrence.first(n), "first " + n);
        }

        /** Throw on every invocation until removed. */
        public ClientFault everyTime() {
            return register(Occurrence.always(), "every time");
        }

        /** Throw on each invocation with the given probability (chaos mode; non-deterministic). */
        public ClientFault withProbability(final double p) {
            return register(Occurrence.withProbability(p), "p=" + p);
        }
    }
}
