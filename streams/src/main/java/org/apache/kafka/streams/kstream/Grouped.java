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
 * The class that is used to capture the key and value {@link Serde}s and set the name used for
 * repartition topics when performing {@link KStream#groupBy(KeyValueMapper, Grouped)}, {@link
 * KStream#groupByKey(Grouped)} {@link KTable#groupBy(KeyValueMapper, Grouped)}operations.
 *
 * @param <K> the key type
 * @param <V> the value type
 */
public class Grouped<K, V> {

    protected Serde<K> keySerde;
    protected Serde<V> valueSerde;
    protected String name;


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
     * Create a Grouped instance with the provided name used for a repartition topic required for
     * performing the grouping operation.
     *
     * @param name the name used for a repartition topic if required
     * @return a new {@link Grouped} configured with the name
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static Grouped named(final String name) {
        return new Grouped<>(name, null, null);
    }


    /**
     * Create a Grouped instance with the provided keySerde
     *
     * @param keySerde the Serde used for serializing the key
     * @return a new {@link Grouped} configured with the keySerde
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K> Grouped keySerde(final Serde<K> keySerde) {
        return new Grouped<>(null, keySerde, null);
    }


    /**
     * Create a Grouped instance with the provided valueSerde
     *
     * @param valueSerde the Serde used for serializing the value
     * @return a new {@link Grouped} configured with the valueSerde
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <V> Grouped valueSerde(final Serde<V> valueSerde) {
        return new Grouped<>(null, null, valueSerde);
    }

    /**
     * Create a Grouped instance with the provided name, keySerde and valueSerde
     *
     * @param name       the name used for a repartition topic if required
     * @param keySerde   the Serde used for serializing the key
     * @param valueSerde the Serde used for serializing the value
     * @return a new {@link Grouped} configured with the name, keySerde, and valueSerde
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
     * Create a Grouped instance with the provided keySerde and valueSerde
     *
     * @param keySerde   the Serde used for serializing the key
     * @param valueSerde the Serde used for serializing the value
     * @return a new {@link Grouped} configured with the keySerde, and valueSerde
     * @see KStream#groupByKey(Grouped)
     * @see KStream#groupBy(KeyValueMapper, Grouped)
     * @see KTable#groupBy(KeyValueMapper, Grouped)
     */
    public static <K, V> Grouped<K, V> with(final Serde<K> keySerde,
                                            final Serde<V> valueSerde) {
        return new Grouped<>(null, keySerde, valueSerde);
    }

    /**
     * Perform the grouping operation with the name for a repartition topic if required
     *
     * @param name the name used for a repartition topic if required
     * @return this
     */
    public Grouped withName(final String name) {
        this.name = name;
        return this;
    }

    /**
     * Perform the grouping operation using the provided keySerde for serializing the key
     *
     * @param keySerde Serde to use for serializing the key
     * @return this
     */
    public Grouped<K, V> withKeySerde(final Serde<K> keySerde) {
        this.keySerde = keySerde;
        return this;
    }

    /**
     * Perform the grouping operation using the provided valueSerde for serializing the value
     *
     * @param valueSerde Serde to use for serializing the value
     * @return this
     */
    public Grouped<K, V> withValueSerde(final Serde<V> valueSerde) {
        this.valueSerde = valueSerde;
        return this;
    }

}
