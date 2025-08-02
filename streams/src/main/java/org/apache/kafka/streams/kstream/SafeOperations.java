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
package org.apache.kafka.streams.kstream;

import java.util.Optional;
import java.util.function.Function;

/**
 * Utility class providing null-safe operations for Kafka Streams transformations.
 * These operations help prevent NullPointerExceptions and provide cleaner error handling.
 */
public final class SafeOperations {

    private SafeOperations() {
        // Utility class
    }

    /**
     * Creates a null-safe ValueMapper that returns Optional.empty() for null values
     * instead of throwing NullPointerException.
     *
     * @param mapper the original mapper function
     * @param <V> input value type
     * @param <VOut> output value type
     * @return null-safe ValueMapper
     */
    public static <V, VOut> ValueMapper<V, Optional<VOut>> nullSafe(final ValueMapper<V, VOut> mapper) {
        return value -> {
            if (value == null) {
                return Optional.empty();
            }
            try {
                return Optional.ofNullable(mapper.apply(value));
            } catch (Exception e) {
                return Optional.empty();
            }
        };
    }

    /**
     * Creates a null-safe KeyValueMapper that returns Optional.empty() for null keys or values.
     *
     * @param mapper the original mapper function
     * @param <K> key type
     * @param <V> value type
     * @param <VOut> output type
     * @return null-safe KeyValueMapper
     */
    public static <K, V, VOut> KeyValueMapper<K, V, Optional<VOut>> nullSafe(final KeyValueMapper<K, V, VOut> mapper) {
        return (key, value) -> {
            if (key == null || value == null) {
                return Optional.empty();
            }
            try {
                return Optional.ofNullable(mapper.apply(key, value));
            } catch (Exception e) {
                return Optional.empty();
            }
        };
    }

    /**
     * Creates a null-safe Predicate that returns false for null values.
     *
     * @param predicate the original predicate
     * @param <K> key type
     * @param <V> value type
     * @return null-safe Predicate
     */
    public static <K, V> Predicate<K, V> nullSafe(final Predicate<K, V> predicate) {
        return (key, value) -> {
            if (key == null || value == null) {
                return false;
            }
            try {
                return predicate.test(key, value);
            } catch (Exception e) {
                return false;
            }
        };
    }

    /**
     * Creates a ValueMapper that applies a default value when the result is null.
     *
     * @param mapper the original mapper
     * @param defaultValue the default value to use when result is null
     * @param <V> input value type
     * @param <VOut> output value type
     * @return ValueMapper with default value
     */
    public static <V, VOut> ValueMapper<V, VOut> withDefault(final ValueMapper<V, VOut> mapper, final VOut defaultValue) {
        return value -> {
            if (value == null) {
                return defaultValue;
            }
            final VOut result = mapper.apply(value);
            return result != null ? result : defaultValue;
        };
    }
}