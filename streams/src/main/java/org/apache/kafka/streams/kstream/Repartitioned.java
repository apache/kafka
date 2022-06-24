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

/**
 * This class is used to provide the optional parameters for internal repartition topics.
 *
 * @param <K> key type
 * @param <V> value type
 * @see KStream#repartition()
 * @see KStream#repartition(Repartitioned)
 */
public class Repartitioned<K, V> implements NamedOperation<Repartitioned<K, V>> {

    protected final String name;
    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final Integer numberOfPartitions;
    protected final StreamPartitioner<K, V> partitioner;

    private Repartitioned(final String name,
                          final Serde<K> keySerde,
                          final Serde<V> valueSerde,
                          final Integer numberOfPartitions,
                          final StreamPartitioner<K, V> partitioner) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.numberOfPartitions = numberOfPartitions;
        this.partitioner = partitioner;
    }

    protected Repartitioned(final Repartitioned<K, V> repartitioned) {
        this(
            repartitioned.name,
            repartitioned.keySerde,
            repartitioned.valueSerde,
            repartitioned.numberOfPartitions,
            repartitioned.partitioner
        );
    }

    /**
     * Create a {@code Repartitioned} instance with the provided name used as part of the repartition topic.
     *
     * @param name the name used as a processor named and part of the repartition topic name.
     * @param <K>  key type
     * @param <V>  value type
     * @return A new {@code Repartitioned} instance configured with processor name and repartition topic name
     * @see KStream#repartition(Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> as(final String name) {
        return new Repartitioned<>(name, null, null, null, null);
    }

    /**
     * Create a {@code Repartitioned} instance with provided key serde and value serde.
     *
     * @param keySerde   Serde to use for serializing the key
     * @param valueSerde Serde to use for serializing the value
     * @param <K>        key type
     * @param <V>        value type
     * @return A new {@code Repartitioned} instance configured with key serde and value serde
     * @see KStream#repartition(Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> with(final Serde<K> keySerde,
                                                  final Serde<V> valueSerde) {
        return new Repartitioned<>(null, keySerde, valueSerde, null, null);
    }

    /**
     * Create a {@code Repartitioned} instance with provided partitioner.
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and the key serde provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used—otherwise {@link DefaultPartitioner} will be used
     * @param <K>         key type
     * @param <V>         value type
     * @return A new {@code Repartitioned} instance configured with partitioner
     * @see KStream#repartition(Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> streamPartitioner(final StreamPartitioner<K, V> partitioner) {
        return new Repartitioned<>(null, null, null, null, partitioner);
    }

    /**
     * Create a {@code Repartitioned} instance with provided number of partitions for repartition topic.
     *
     * @param numberOfPartitions number of partitions used when creating repartition topic
     * @param <K>                key type
     * @param <V>                value type
     * @return A new {@code Repartitioned} instance configured number of partitions
     * @see KStream#repartition(Repartitioned)
     */
    public static <K, V> Repartitioned<K, V> numberOfPartitions(final int numberOfPartitions) {
        return new Repartitioned<>(null, null, null, numberOfPartitions, null);
    }

    /**
     * Create a new instance of {@code Repartitioned} with the provided name used as part of repartition topic and processor name.
     *
     * @param name the name used for the processor name and as part of the repartition topic
     * @return a new {@code Repartitioned} instance configured with the name
     */
    @Override
    public Repartitioned<K, V> withName(final String name) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@code Repartitioned} with the provided number of partitions for repartition topic.
     *
     * @param numberOfPartitions the name used for the processor name and as part of the repartition topic name
     * @return a new {@code Repartitioned} instance configured with the number of partitions
     */
    public Repartitioned<K, V> withNumberOfPartitions(final int numberOfPartitions) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@code Repartitioned} with the provided key serde.
     *
     * @param keySerde Serde to use for serializing the key
     * @return a new {@code Repartitioned} instance configured with the key serde
     */
    public Repartitioned<K, V> withKeySerde(final Serde<K> keySerde) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@code Repartitioned} with the provided value serde.
     *
     * @param valueSerde Serde to use for serializing the value
     * @return a new {@code Repartitioned} instance configured with the value serde
     */
    public Repartitioned<K, V> withValueSerde(final Serde<V> valueSerde) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }

    /**
     * Create a new instance of {@code Repartitioned} with the provided partitioner.
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and the key serde provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used—otherwise {@link DefaultPartitioner} wil be used
     * @return a new {@code Repartitioned} instance configured with provided partitioner
     */
    public Repartitioned<K, V> withStreamPartitioner(final StreamPartitioner<K, V> partitioner) {
        return new Repartitioned<>(name, keySerde, valueSerde, numberOfPartitions, partitioner);
    }
}
