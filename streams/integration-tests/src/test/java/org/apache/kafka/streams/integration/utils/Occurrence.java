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

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.IntPredicate;

/**
 * Shared trigger predicates for the fault-injection DSLs. A trigger decides, from the 1-based count of
 * matching events seen so far, whether the fault should fire on this one. Used by both
 * {@link FaultRule.Builder} (wire-protocol faults) and {@link ClientFault.Builder} (client-exception faults)
 * so the two DSLs read identically ({@code once()}, {@code onCall(n)}, {@code times(n)}, {@code everyTime()},
 * {@code withProbability(p)}).
 */
final class Occurrence {

    private Occurrence() {
    }

    /** Fire on the first matching event only. */
    static IntPredicate once() {
        return n -> n == 1;
    }

    /** Fire on exactly the {@code n}-th matching event (1-based). */
    static IntPredicate call(final int n) {
        return matchNo -> matchNo == n;
    }

    /** Fire on the first {@code n} matching events. */
    static IntPredicate first(final int n) {
        return matchNo -> matchNo <= n;
    }

    /** Fire on every matching event. */
    static IntPredicate always() {
        return n -> true;
    }

    /** Fire on each matching event with probability {@code p} (chaos mode; non-deterministic). */
    static IntPredicate withProbability(final double p) {
        if (p < 0.0 || p > 1.0) {
            throw new IllegalArgumentException("probability must be in [0.0, 1.0], got " + p);
        }
        return n -> ThreadLocalRandom.current().nextDouble() < p;
    }
}
