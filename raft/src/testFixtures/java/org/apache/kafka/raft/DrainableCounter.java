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
package org.apache.kafka.raft;

import java.util.function.IntSupplier;

/**
 * Tracks a cumulative, monotonically increasing counter (e.g. the work counters on the raft mocks) as
 * a drainable delta against a baseline. {@link #reset()} snapshots the current value (so the next
 * {@link #delta()} starts from zero), and {@link #delta()} returns the increase since the last
 * reset/delta and advances the baseline.
 */
final class DrainableCounter {
    private final IntSupplier source;
    private int baseline;

    DrainableCounter(IntSupplier source) {
        this.source = source;
    }

    void reset() {
        baseline = source.getAsInt();
    }

    int delta() {
        int current = source.getAsInt();
        int delta = current - baseline;
        baseline = current;
        return delta;
    }
}
