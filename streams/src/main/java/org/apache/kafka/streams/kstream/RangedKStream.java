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

/**
 * {@code RangedKStream} is an intermediate representation of a grouped record stream that has been
 * annotated with a {@link Range} definition. It is obtained from a {@link KGroupedStream} via
 * {@link KGroupedStream#rangeOver(Range)}.
 *
 * <p>Multiple independent aggregations can be applied to the same {@code RangedKStream} instance.
 * The underlying buffer store is shared across all aggregations and is written to once per incoming
 * record.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public interface RangedKStream<K, V> {

    /**
     * Perform an aggregation on the records in the range defined for this stream.
     * The aggregation is triggered for each incoming record and includes all records that fall
     * within the defined range of that record.
     *
     * @param aggregator the aggregator function to apply to the records in the range
     * @param <VR>       the type of the aggregated value
     * @return a {@link KStream} containing the aggregated result for each anchor record
     */
    <VR> KStream<K, VR> aggregate(final RangeAggregator<K, V, VR> aggregator);

    /**
     * Count the number of records in this range by the grouped key.
     *
     * @return a {@link KStream} that contains records with unmodified keys and {@link Long} values
     * that represent the count of records in the defined range for each anchor record
     */
    KStream<K, Long> count();
}
