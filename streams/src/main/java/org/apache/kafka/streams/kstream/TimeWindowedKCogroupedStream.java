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
 * {@code TimeWindowedCogroupKStream} is an abstraction of a <i>windowed</i> record stream of {@link org.apache.kafka.streams.KeyValue} pairs.
    * It is an intermediate representation of a {@link KStream} in order to apply a windowed aggregation operation on the original
    * {@link KStream} records.
    * <p>
 * It is an intermediate representation after a grouping and windowing of a {@link KStream} before an aggregation is applied to the
    * new (partitioned) windows resulting in a windowed {@link KTable}
    * (a <emph>windowed</emph> {@code KTable} is a {@link KTable} with key type {@link Windowed Windowed<K>}.
    * <p>
 * The specified {@code windows} define either hopping time windows that can be overlapping or tumbling (c.f.
    * {@link TimeWindows}) or they define landmark windows (c.f. {@link UnlimitedWindows}).
    * The result is written into a local windowed {@link org.apache.kafka.streams.state.KeyValueStore} (which is basically an ever-updating
    * materialized view) that can be queried using the name provided in the {@link Materialized} instance.
    *
    * New events are added to windows until their grace period ends (see {@link TimeWindows#grace(Duration)}).
    *
    * Furthermore, updates to the store are sent downstream into a windowed {@link KTable} changelog stream, where
    * "windowed" implies that the {@link KTable} key is a combined key of the original record key and a window ID.

    * A {@code WindowedKStream} must be obtained from a {@link KGroupedStream} via {@link KGroupedStream#windowedBy(Windows)} .
    *
    * @param <K> Type of keys
    * @param <T> Type of values
    * @see KStream
    * @see KGroupedStream
    */

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;

public interface TimeWindowedKCogroupedStream<K, V> {

    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer,
                                     final Materialized<K, V, WindowStore<Bytes, byte[]>> materialized);

    KTable<Windowed<K>, V> aggregate(final Initializer<V> initializer);

}
