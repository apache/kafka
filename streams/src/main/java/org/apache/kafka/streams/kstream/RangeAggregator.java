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

import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;

/**
 * A functional interface for range aggregations on a {@link RangedKStream}.
 *
 * @param <K>  the key type
 * @param <V>  the value type of the input records
 * @param <VR> the result type of the aggregation
 */
@FunctionalInterface
public interface RangeAggregator<K, V, VR> {

    /**
     * Apply the aggregation logic to the records in the defined range for a given key.
     *
     * @param anchor       the record that triggered the aggregation
     * @param rangeRecords an iterable of records that fall within the defined range of the anchor
     *                     record, including the anchor record itself. Records are ordered by
     *                     timestamp in ascending order.
     *                     Note: headers are not preserved in {@code rangeRecords} as they are not
     *                     stored in the underlying {@link WindowStore}. Headers are only available
     *                     on the {@code anchor} record.
     * @return the result of the aggregation
     */
    VR apply(Record<K, V> anchor, Iterable<Record<K, V>> rangeRecords);
}
