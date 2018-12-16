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
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.ValueAndTimestamp;

public class MeteredSessionWithTimestampStore<K, V> extends MeteredSessionStore<K, ValueAndTimestamp<V>> {

    MeteredSessionWithTimestampStore(final SessionStore<Bytes, byte[]> inner,
                                     final String metricScope,
                                     final Serde<K> keySerde,
                                     final Serde<ValueAndTimestamp<V>> valueSerde,
                                     final Time time) {
        super(inner, metricScope, keySerde, valueSerde, time);
    }

    @SuppressWarnings("unchecked")
    @Override
    void initStateSerdes(final ProcessorContext context) {
        serdes = new StateSerdes<>(
            ProcessorStateManager.storeChangelogTopic(context.applicationId(), name()),
            keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
            ((KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde) valueSerde).initialized() ?
                valueSerde :
                ((KeyValueWithTimestampStoreBuilder.ValueAndTimestampSerde<V>) valueSerde).init((Serde<V>) context.valueSerde()));
    }

}
