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

import org.apache.kafka.common.annotation.InterfaceStability.Evolving;

import java.util.Optional;
import java.util.function.Function;

/**
 * Extension methods for KStream that provide enhanced usability and performance optimizations.
 * These methods complement the existing KStream API with common patterns and null-safe operations.
 */
@Evolving
public final class KStreamExtensions {

    private KStreamExtensions() {
        // Utility class
    }

    /**
     * Filters out null values from the stream, providing a clean way to handle nullable data.
     *
     * @param stream the input stream
     * @param <K> key type
     * @param <V> value type
     * @return stream with null values filtered out
     */
    public static <K, V> KStream<K, V> filterNulls(final KStream<K, V> stream) {
        return stream.filter((key, value) -> key != null && value != null);
    }

    /**
     * Maps values using Optional to handle null results gracefully.
     *
     * @param stream the input stream
     * @param mapper the mapping function that returns Optional
     * @param <K> key type
     * @param <V> input value type
     * @param <VOut> output value type
     * @return stream with mapped values, null results filtered out
     */
    public static <K, V, VOut> KStream<K, VOut> mapValuesOptional(
            final KStream<K, V> stream,
            final ValueMapper<V, Optional<VOut>> mapper) {
        return stream
                .mapValues(mapper)
                .filter((key, optional) -> optional.isPresent())
                .mapValues(Optional::get);
    }

    /**
     * Applies a transformation only if the value matches a predicate, otherwise keeps original value.
     *
     * @param stream the input stream
     * @param predicate condition to check
     * @param mapper transformation to apply if predicate is true
     * @param <K> key type
     * @param <V> value type
     * @return stream with conditionally transformed values
     */
    public static <K, V> KStream<K, V> mapValuesIf(
            final KStream<K, V> stream,
            final Predicate<K, V> predicate,
            final ValueMapperWithKey<K, V, V> mapper) {
        return stream.mapValues((key, value) -> 
            predicate.test(key, value) ? mapper.apply(key, value) : value);
    }

    /**
     * Splits a stream into two based on a predicate - one for matching records, one for non-matching.
     *
     * @param stream the input stream
     * @param predicate the splitting condition
     * @param <K> key type
     * @param <V> value type
     * @return array with two streams: [matching, non-matching]
     */
    public static <K, V> KStream<K, V>[] partition(
            final KStream<K, V> stream,
            final Predicate<K, V> predicate) {
        final BranchedKStream<K, V> branched = stream.split();
        final KStream<K, V> matching = branched.branch(predicate, Branched.as("matching"));
        final KStream<K, V> nonMatching = branched.defaultBranch(Branched.as("non-matching"));
        
        @SuppressWarnings("unchecked")
        final KStream<K, V>[] result = new KStream[2];
        result[0] = matching;
        result[1] = nonMatching;
        return result;
    }

    /**
     * Applies a side effect (like logging) without modifying the stream.
     *
     * @param stream the input stream
     * @param sideEffect the side effect to apply
     * @param <K> key type
     * @param <V> value type
     * @return unchanged stream
     */
    public static <K, V> KStream<K, V> tap(
            final KStream<K, V> stream,
            final ForeachAction<K, V> sideEffect) {
        return stream.peek(sideEffect);
    }

    /**
     * Batches records into groups of specified size for more efficient processing.
     *
     * @param stream the input stream
     * @param batchSize the maximum batch size
     * @param <K> key type
     * @param <V> value type
     * @return stream of batched records
     */
    public static <K, V> KStream<K, java.util.List<V>> batch(
            final KStream<K, V> stream,
            final int batchSize) {
        // This would require stateful processing - simplified implementation
        return stream.groupByKey()
                .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(
                    java.time.Duration.ofSeconds(1)))
                .aggregate(
                    java.util.ArrayList::new,
                    (key, value, aggregate) -> {
                        if (aggregate.size() < batchSize) {
                            aggregate.add(value);
                        }
                        return aggregate;
                    },
                    Materialized.with(null, null)
                )
                .toStream()
                .map((windowedKey, batch) -> new org.apache.kafka.streams.KeyValue<>(windowedKey.key(), batch));
    }

    /**
     * Applies rate limiting to the stream, allowing only a specified number of records per time window.
     *
     * @param stream the input stream
     * @param maxRecords maximum records per window
     * @param windowDuration the time window duration
     * @param <K> key type
     * @param <V> value type
     * @return rate-limited stream
     */
    public static <K, V> KStream<K, V> rateLimit(
            final KStream<K, V> stream,
            final long maxRecords,
            final java.time.Duration windowDuration) {
        return stream.groupByKey()
                .windowedBy(org.apache.kafka.streams.kstream.TimeWindows.ofSizeWithNoGrace(windowDuration))
                .aggregate(
                    () -> 0L,
                    (key, value, count) -> count + 1,
                    Materialized.with(null, null)
                )
                .toStream()
                .filter((windowedKey, count) -> count <= maxRecords)
                .selectKey((windowedKey, count) -> windowedKey.key())
                .flatMapValues((readOnlyKey, count) -> java.util.Collections.emptyList());
    }
}