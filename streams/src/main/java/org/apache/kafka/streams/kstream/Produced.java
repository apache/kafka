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

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner;
import org.apache.kafka.streams.processor.StreamPartitioner;

import java.util.Objects;

/**
 * This class is used to provide the optional parameters when producing to new topics
 * using {@link KStream#to(String, Produced)}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class Produced<K, V> implements NamedOperation<Produced<K, V>> {

    protected Serde<K> keySerde;
    protected Serde<V> valueSerde;
    protected StreamPartitioner<? super K, ? super V> partitioner;
    protected String processorName;

    private Produced(final Serde<K> keySerde,
                     final Serde<V> valueSerde,
                     final StreamPartitioner<? super K, ? super V> partitioner,
                     final String processorName) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.partitioner = partitioner;
        this.processorName = processorName;
    }

    protected Produced(final Produced<K, V> produced) {
        this.keySerde = produced.keySerde;
        this.valueSerde = produced.valueSerde;
        this.partitioner = produced.partitioner;
        this.processorName = produced.processorName;
    }

    /**
     * Create a Produced instance with provided keySerde and valueSerde.
     * @param keySerde      Serde to use for serializing the key
     * @param valueSerde    Serde to use for serializing the value
     * @param <K>           key type
     * @param <V>           value type
     * @return  A new {@link Produced} instance configured with keySerde and valueSerde
     * @see KStream#to(String, Produced)
     */
    public static <K, V> Produced<K, V> with(final Serde<K> keySerde,
                                             final Serde<V> valueSerde) {
        return new Produced<>(keySerde, valueSerde, null, null);
    }

    /**
     * Create a Produced instance with provided keySerde, valueSerde, and partitioner.
     * @param keySerde      Serde to use for serializing the key
     * @param valueSerde    Serde to use for serializing the value
     * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
     *                      if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
     *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner}
     *                      will be used
     * @param <K>           key type
     * @param <V>           value type
     * @return  A new {@link Produced} instance configured with keySerde, valueSerde, and partitioner
     * @see KStream#to(String, Produced)
     */
    public static <K, V> Produced<K, V> with(final Serde<K> keySerde,
                                             final Serde<V> valueSerde,
                                             final StreamPartitioner<? super K, ? super V> partitioner) {
        return new Produced<>(keySerde, valueSerde, partitioner, null);
    }

    /**
     * Create an instance of {@link Produced} with provided processor name.
     *
     * @param processorName the processor name to be used. If {@code null} a default processor name will be generated
     * @param <K>         key type
     * @param <V>         value type
     * @return a new instance of {@link Produced}
     */
    public static <K, V> Produced<K, V> as(final String processorName) {
        return new Produced<>(null, null, null, processorName);
    }

    /**
     * Create a Produced instance with provided keySerde.
     * @param keySerde      Serde to use for serializing the key
     * @param <K>           key type
     * @param <V>           value type
     * @return  A new {@link Produced} instance configured with keySerde
     * @see KStream#to(String, Produced)
     */
    public static <K, V> Produced<K, V> keySerde(final Serde<K> keySerde) {
        return new Produced<>(keySerde, null, null, null);
    }

    /**
     * Create a Produced instance with provided valueSerde.
     * @param valueSerde    Serde to use for serializing the key
     * @param <K>           key type
     * @param <V>           value type
     * @return  A new {@link Produced} instance configured with valueSerde
     * @see KStream#to(String, Produced)
     */
    public static <K, V> Produced<K, V> valueSerde(final Serde<V> valueSerde) {
        return new Produced<>(null, valueSerde, null, null);
    }

    /**
     * Create a Produced instance with provided partitioner.
     * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
     *                      if not specified and the key serde provides a {@link WindowedSerializer} for the key
     *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will be used
     * @param <K>           key type
     * @param <V>           value type
     * @return  A new {@link Produced} instance configured with partitioner
     * @see KStream#to(String, Produced)
     */
    public static <K, V> Produced<K, V> streamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        return new Produced<>(null, null, partitioner, null);
    }

    /**
     * Produce records using the provided partitioner.
     * @param partitioner   the function used to determine how records are distributed among partitions of the topic,
     *                      if not specified and the key serde provides a {@link WindowedSerializer} for the key
     *                      {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} wil be used
     * @return this
     */
    public Produced<K, V> withStreamPartitioner(final StreamPartitioner<? super K, ? super V> partitioner) {
        this.partitioner = partitioner;
        return this;
    }

    /**
     * Produce records using the provided valueSerde.
     * @param valueSerde    Serde to use for serializing the value
     * @return this
     */
    public Produced<K, V> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

    /**
     * Produce records using the provided keySerde.
     * @param keySerde    Serde to use for serializing the key
     * @return this
     */
    public Produced<K, V> withKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Produced<?, ?> produced = (Produced<?, ?>) o;
        return Objects.equals(keySerde, produced.keySerde) &&
               Objects.equals(valueSerde, produced.valueSerde) &&
               Objects.equals(partitioner, produced.partitioner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keySerde, valueSerde, partitioner);
    }

    @Override
    public Produced<K, V> withName(final String name) {
        this.processorName = name;
        return this;
    }
}
