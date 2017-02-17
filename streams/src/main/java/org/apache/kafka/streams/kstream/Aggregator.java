/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * The {@code Aggregator} interface for aggregating values of the given key.
 * This is a generalization of {@link Reducer} and allows to have different types for input value and aggregation
 * result.
 * {@code Aggregator} is used in combination with {@link Initializer} that provides an initial aggregation value.
 * <p>
 * {@code Aggregator} can be used to implement aggregation functions like count.

 * @param <K> key type
 * @param <V> input value type
 * @param <VA> aggregate value type
 * @see Initializer
 * @see KGroupedStream#aggregate(Initializer, Aggregator, org.apache.kafka.common.serialization.Serde, String)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Windows, org.apache.kafka.common.serialization.Serde, String)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Windows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Merger, SessionWindows, org.apache.kafka.common.serialization.Serde, String)
 * @see KGroupedStream#aggregate(Initializer, Aggregator, Merger, SessionWindows, org.apache.kafka.common.serialization.Serde, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see Reducer
 */
@InterfaceStability.Unstable
public interface Aggregator<K, V, VA> {

    /**
     * Compute a new aggregate from the key and value of a record and the current aggregate of the same key.
     *
     * @param key       the key of the record
     * @param value     the value of the record
     * @param aggregate the current aggregate value
     * @return the new aggregate value
     */
    VA apply(final K key, final V value, final VA aggregate);
}
