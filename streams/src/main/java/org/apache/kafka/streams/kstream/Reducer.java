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
import org.apache.kafka.streams.KeyValue;
/**
 * The {@code Reducer} interface for combining two values of the same type into a new value.
 * In contrast to {@link Aggregator} the result type must be the same as the input type.
 * <p>
 * The provided values can be either original values from input {@link KeyValue} pair records or be a previously
 * computed result from {@link Reducer#apply(Object, Object)}.
 * <p>
 * {@code Reducer} can be used to implement aggregation functions like sum, min, or max.
 *
 * @param <V> value type
 * @see KGroupedStream#reduce(Reducer, String)
 * @see KGroupedStream#reduce(Reducer, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#reduce(Reducer, Windows, String)
 * @see KGroupedStream#reduce(Reducer, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see KGroupedStream#reduce(Reducer, SessionWindows, String)
 * @see KGroupedStream#reduce(Reducer, SessionWindows, org.apache.kafka.streams.processor.StateStoreSupplier)
 * @see Aggregator
 */
@InterfaceStability.Unstable
public interface Reducer<V> {

    /**
     * Aggregate the two given values into a single one.
     *
     * @param value1 the first value for the aggregation
     * @param value2 the second value for the aggregation
     * @return the aggregated value
     */
    V apply(final V value1, final V value2);
}
