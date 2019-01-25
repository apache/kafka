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
import org.apache.kafka.streams.state.ValueAndTimestamp;

/**
 * A Metered {@link KeyValueStore} wrapper that is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own metrics collecting functionality.
 * The inner {@link KeyValueStore} of this class is of type &lt;Bytes,byte[]&gt;, hence we use {@link Serde}s
 * to convert from &lt;K,V&gt; to &lt;Bytes,byte[]&gt;
 * @param <K>
 * @param <V>
 */
public class MeteredKeyValueWithTimestampStore<K, V> extends MeteredKeyValueStore<K, ValueAndTimestamp<V>> {

    private final KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde<V> valueAndTimestampSerde;

    MeteredKeyValueWithTimestampStore(final KeyValueStore<Bytes, byte[]> inner,
                                      final String metricScope,
                                      final Time time,
                                      final Serde<K> keySerde,
                                      final KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde<V> valueSerde) {
        super(inner, metricScope, time, keySerde, valueSerde);
        valueAndTimestampSerde = valueSerde;
    }

    @SuppressWarnings("unchecked")
    @Override
    void initStateSerdes(final ProcessorContext context) {
        final Serde<K> resolvedKeySerde = keySerde == null ? (Serde<K>) context.keySerde() : keySerde;
        if (!valueAndTimestampSerde.initialized()) {
            valueAndTimestampSerde.init((Serde<V>) context.valueSerde());
        }
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            resolvedKeySerde,
            valueAndTimestampSerde);
    }

}