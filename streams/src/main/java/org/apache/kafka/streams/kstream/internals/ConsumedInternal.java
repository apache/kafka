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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class ConsumedInternal<K, V> extends Consumed<K, V> {
    public ConsumedInternal(final Consumed<K, V> consumed) {
        super(consumed);
    }


    public ConsumedInternal(final Serde<K> keySerde,
                            final Serde<V> valSerde,
                            final TimestampExtractor timestampExtractor,
                            final Topology.AutoOffsetReset offsetReset) {
        this(Consumed.with(keySerde, valSerde, timestampExtractor, offsetReset));
    }

    public ConsumedInternal() {
        this(Consumed.<K, V>with(null, null));
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Deserializer<K> keyDeserializer() {
        return keySerde == null ? null : keySerde.deserializer();
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public Deserializer<V> valueDeserializer() {
        return valueSerde == null ? null : valueSerde.deserializer();
    }

    public TimestampExtractor timestampExtractor() {
        return timestampExtractor;
    }

    public Topology.AutoOffsetReset offsetResetPolicy() {
        return resetPolicy;
    }
}
