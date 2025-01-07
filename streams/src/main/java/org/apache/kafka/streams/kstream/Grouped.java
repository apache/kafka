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

import org.apache.kafka.common.serialization.Serde;

/**
 * The class that is used to capture the key and value {@link Serde}s and set the part of name used for
 * repartition topics when performing {@link KStream#groupBy(KeyValueMapper, Grouped)}, {@link
 * KStream#groupByKey(Grouped)}, or {@link KTable#groupBy(KeyValueMapper, Grouped)} operations.  Note
 * that Kafka Streams does not always create repartition topics for grouping operations.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class Grouped<K, V> implements NamedOperation<Grouped<K, V>> {

    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final String name;

    private Grouped(final String name,
                    final Serde<K> keySerde,
                    final Serde<V> valueSerde) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
    }

    protected Grouped(final Grouped<K, V> grouped) {
        this(grouped.name, grouped.keySerde, grouped.valueSerde);
    }

    /**
     * Create a {@link Grouped} instance with the provided name used as part of the repartition topic if required.
     *
     * @param name
     *        the name used for a repartition topic if required
     *
     * @param <K> the key type
     * @param <V> the value type
     *
     * @return a new {@link Grouped} configured with the name
     *
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K, V> Grouped<K, V> as(final String name) {
        return new Grouped<>(name, null, null);
    }


    /**
     * Create a {@link Grouped} instance with the provided keySerde. If {@code null} the default key serde from config will be used.
     *
     * @param keySerde
     *        the Serde used for serializing the key. If {@code null} the default key serde from config will be used
     *
     * @param <K> the key type
     * @param <V> the value type
     *
     * @return a new {@link Grouped} configured with the keySerde
     *
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K, V> Grouped<K, V> keySerde(final Serde<K> keySerde) {
        return new Grouped<>(null, keySerde, null);
    }


    /**
     * Create a {@link Grouped} instance with the provided valueSerde.  If {@code null} the default value serde from config will be used.
     *
     * @param valueSerde
     *        the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
     *
     * @param <K> the key type
     * @param <V> the value type
     *
     * @return a new {@link Grouped} configured with the valueSerde
     *
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K, V> Grouped<K, V> valueSerde(final Serde<V> valueSerde) {
        return new Grouped<>(null, null, valueSerde);
    }

    /**
     * Create a {@link Grouped} instance with the provided  name, keySerde, and valueSerde. If the keySerde and/or the valueSerde is
     * {@code null} the default value for the respective serde from config will be used.
     *
     * @param name
     *        the name used as part of the repartition topic name if required
     * @param keySerde
     *        the {@link Serde} used for serializing the key. If {@code null} the default key serde from config will be used
     * @param valueSerde
     *        the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
     *
     * @param <K> the key type
     * @param <V> the value type
     *
     * @return a new {@link Grouped} configured with the name, keySerde, and valueSerde
     *
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K, V> Grouped<K, V> with(final String name,
                                            final Serde<K> keySerde,
                                            final Serde<V> valueSerde) {
        return new Grouped<>(name, keySerde, valueSerde);
    }


    /**
     * Create a {@link Grouped} instance with the provided keySerde and valueSerde.  If the keySerde and/or the valueSerde is
     * {@code null} the default value for the respective serde from config will be used.
     *
     * @param keySerde
     *         the {@link Serde} used for serializing the key. If {@code null} the default key serde from config will be used
     * @param valueSerde
     *        the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
     *
     * @param <K> the key type
     * @param <V> the value type
     *
     * @return a new {@link Grouped} configured with the keySerde, and valueSerde
     *
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K, V> Grouped<K, V> with(final Serde<K> keySerde,
                                            final Serde<V> valueSerde) {
        return new Grouped<>(null, keySerde, valueSerde);
    }

    /**
     * Perform the grouping operation with the name for a repartition topic if required.  Note
     * that Kafka Streams does not always create repartition topics for grouping operations.
     *
     * @param name
     *        the name used for the processor name and as part of the repartition topic name if required
     *
     * @return a new {@link Grouped} instance configured with the name
     * */
    @Override
    public Grouped<K, V> withName(final String name) {
        return new Grouped<>(name, keySerde, valueSerde);
    }

    /**
     * Perform the grouping operation using the provided keySerde for serializing the key.
     *
     * @param keySerde
     *        {@link Serde} to use for serializing the key. If {@code null} the default key serde from config will be used
     *
     * @return a new {@link Grouped} instance configured with the keySerde
     */
    public Grouped<K, V> withKeySerde(final Serde<K> keySerde) {
        return new Grouped<>(name, keySerde, valueSerde);
    }

    /**
     * Perform the grouping operation using the provided valueSerde for serializing the value.
     *
     * @param valueSerde
     *        {@link Serde} to use for serializing the value. If {@code null} the default value serde from config will be used
     *
     * @return a new {@link Grouped} instance configured with the valueSerde
     */
    public Grouped<K, V> withValueSerde(final Serde<V> valueSerde) {
        return new Grouped<>(name, keySerde, valueSerde);
    }

}
