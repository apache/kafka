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
package org.apache.kafka.common.telemetry.collector;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.telemetry.metrics.MetricKey;

/**
 * A LastValueTracker uses a ConcurrentMap to maintain historic values for a given key, and return
 * a previous value and an Instant for that value.
 *
 * @param <T> The type of the value.
 */
public class LastValueTracker<T> {

    private final ConcurrentMap<MetricKey, AtomicReference<InstantAndValue<T>>> counters = new ConcurrentHashMap<>();

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
        AtomicReference<InstantAndValue<T>> valueOrNull = counters
            .putIfAbsent(metricKey, new AtomicReference<>(instantAndValue));

        // there wasn't already an entry, so return empty.
        if (valueOrNull == null) {
            return Optional.empty();
        }

        // Update the atomic ref to point to our new InstantAndValue, but get the previous value
        InstantAndValue<T> previousValue = valueOrNull.getAndSet(instantAndValue);

        // Return the instance and the value.
        return Optional.of(previousValue);
    }

    public AtomicReference<InstantAndValue<T>> remove(MetricKey metricKey) {
        return counters.remove(metricKey);
    }

    public boolean contains(MetricKey metricKey) {
        return counters.containsKey(metricKey);
    }

}
