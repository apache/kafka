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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.util.function.Function;

/**
 * Utility class providing common value converter functions for facade patterns.
 * These converters are used to transform between different value representations
 * (e.g., ValueAndTimestamp, ValueTimestampHeaders, plain values).
 */
public final class ValueConverters {

    private ValueConverters() {
        // Utility class - prevent instantiation
    }

    /**
     * Converts {@link ValueAndTimestamp} to plain value, discarding timestamp.
     *
     * @param <V> value type
     * @return converter function that extracts the value or returns null
     */
    public static <V> Function<ValueAndTimestamp<V>, V> extractValue() {
        return ValueAndTimestamp::getValueOrNull;
    }

    /**
     * Converts {@link ValueTimestampHeaders} to plain value, discarding timestamp and headers.
     *
     * @param <V> value type
     * @return converter function that extracts the value or returns null
     */
    public static <V> Function<ValueTimestampHeaders<V>, V> extractValueFromHeaders() {
        return vth -> vth == null ? null : vth.value();
    }

    /**
     * Converts {@link ValueTimestampHeaders} to {@link ValueAndTimestamp}, discarding headers.
     *
     * @param <V> value type
     * @return converter function that creates ValueAndTimestamp or returns null
     */
    public static <V> Function<ValueTimestampHeaders<V>, ValueAndTimestamp<V>> extractValueAndTimestampFromHeaders() {
        return vth -> vth == null ? null :
            ValueAndTimestamp.make(vth.value(), vth.timestamp());
    }
}