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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Objects;

public class ProducedInternal<K, V> implements Produced<K, V> {
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final StreamPartitioner<? super K, ? super V> partitioner;

    public ProducedInternal(final Serde<K> keySerde,
                            final Serde<V> valueSerde,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.partitioner = partitioner;
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public StreamPartitioner<? super K, ? super V> streamPartitioner() {
        return partitioner;
    }

    @Override
    public Produced<K, V> withStreamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        return new ProducedInternal<>(keySerde, valueSerde, partitioner);
    }

    @Override
    public Produced<K, V> withValueSerde(final Serde<V> valueSerde) {
        return new ProducedInternal<>(keySerde, valueSerde, partitioner);
    }

    @Override
    public Produced<K, V> withKeySerde(final Serde<K> keySerde) {
        return new ProducedInternal<>(keySerde, valueSerde, partitioner);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ProducedInternal<?, ?> that = (ProducedInternal<?, ?>) o;
        return Objects.equals(keySerde, that.keySerde) &&
            Objects.equals(valueSerde, that.valueSerde) &&
            Objects.equals(partitioner, that.partitioner);
    }

    @Override
    public int hashCode() {

        return Objects.hash(keySerde, valueSerde, partitioner);
    }
}
