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

/**
 * Tracks a cumulative, monotonically increasing counter (e.g. the work counters on the raft
 * mocks) as a drainable delta against a baseline. The baseline is set at construction, and
 * {@link #drainDelta(long)} returns the increase of the supplied reading since construction or the
 * previous drain, consuming it so nothing is counted twice. Draining with the result ignored
 * therefore re-baselines the counter, e.g. to exclude setup work from the next measurement.
 */
final class DrainableCounter {
    private long baseline;

    DrainableCounter(long initial) {
        this.baseline = initial;
    }

    long drainDelta(long value) {
        if (value < baseline) {
            throw new IllegalStateException(
                "Counter is not monotonically increasing: read " + value
                    + " after " + baseline);
        }
        long delta = value - baseline;
        baseline = value;
        return delta;
    }
}
