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
package org.apache.kafka.common.telemetry.internals;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A LastValueTracker uses a ConcurrentHashMap to maintain historic values for a given key, and return
 * a previous value and an Instant for that value.
 *
 * @param <T> The type of the value.
 */
public class LastValueTracker<T> {
    private final Map<MetricKey, InstantAndValue<T>> counters = new ConcurrentHashMap<>();

    /**
     * Return the last instant/value for the given MetricKey, or Optional.empty if there isn't one.
     *
     * @param metricKey the key for which to calculate a getAndSet.
     * @param now the timestamp for the new value.
     * @param value the current value.
     * @return the timestamp of the previous entry and its value. If there
     *     isn't a previous entry, then this method returns {@link Optional#empty()}
     */
    public Optional<InstantAndValue<T>> getAndSet(MetricKey metricKey, Instant now, T value) {
        InstantAndValue<T> instantAndValue = new InstantAndValue<>(now, value);
        InstantAndValue<T> valueOrNull = counters.put(metricKey, instantAndValue);

        // there wasn't already an entry, so return empty.
        if (valueOrNull == null) {
            return Optional.empty();
        }

        // Return the previous instance and the value.
        return Optional.of(valueOrNull);
    }

    public InstantAndValue<T> remove(MetricKey metricKey) {
        return counters.remove(metricKey);
    }

    public boolean contains(MetricKey metricKey) {
        return counters.containsKey(metricKey);
    }

    public void reset() {
        counters.clear();
    }

    public static class InstantAndValue<T> {

        private final Instant intervalStart;
        private final T value;

        public InstantAndValue(Instant intervalStart, T value) {
            this.intervalStart = Objects.requireNonNull(intervalStart);
            this.value = Objects.requireNonNull(value);
        }

        public Instant getIntervalStart() {
            return intervalStart;
        }

        public T getValue() {
            return value;
        }
    }

}