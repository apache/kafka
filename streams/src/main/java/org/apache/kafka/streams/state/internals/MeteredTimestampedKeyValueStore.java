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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * A Metered {@link TimestampedKeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,ValueAndTimestamp&lt;V&gt&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredTimestampedKeyValueStore<K, V>
    extends MeteredKeyValueStore<K, ValueAndTimestamp<V>>
    implements TimestampedKeyValueStore<K, V> {

    MeteredTimestampedKeyValueStore(final KeyValueStore<Bytes, byte[]> inner,
                                    final String metricScope,
                                    final Time time,
                                    final Serde<K> keySerde,
                                    final Serde<ValueAndTimestamp<V>> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
    }

    @SuppressWarnings("unchecked")
    void initStoreSerde(final ProcessorContext context) {
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            valueSerde == null ? new ValueAndTimestampSerde<>((Serde<V>) context.valueSerde()) : valueSerde);
    }
}