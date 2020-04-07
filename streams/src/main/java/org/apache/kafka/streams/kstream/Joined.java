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
 * The {@code Joined} class represents optional params that can be passed to
 * {@link KStream#join(KTable, ValueJoiner, Joined) KStream#join(KTable,...)} and
 * {@link KStream#leftJoin(KTable, ValueJoiner) KStream#leftJoin(KTable,...)} operations.
 */
public class Joined<K, V, VO> implements NamedOperation<Joined<K, V, VO>> {

    protected final Serde<K> keySerde;
    protected final Serde<V> valueSerde;
    protected final Serde<VO> otherValueSerde;
    protected final String name;

    private Joined(final Serde<K> keySerde,
                   final Serde<V> valueSerde,
                   final Serde<VO> otherValueSerde,
                   final String name) {
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.otherValueSerde = otherValueSerde;
        this.name = name;
    }

    protected Joined(final Joined<K, V, VO> joined) {
        this(joined.keySerde, joined.valueSerde, joined.otherValueSerde, joined.name);
    }

    /**
     * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
     * {@code null} values are accepted and will be replaced by the default serdes as defined in config.
     *
     * @param keySerde        the key serde to use. If {@code null} the default key serde from config will be used
     * @param valueSerde      the value serde to use. If {@code null} the default value serde from config will be used
     * @param otherValueSerde the otherValue serde to use. If {@code null} the default value serde from config will be used
     * @param <K>             key type
     * @param <V>             value type
     * @param <VO>            other value type
     * @return new {@code Joined} instance with the provided serdes
     */
    public static <K, V, VO> Joined<K, V, VO> with(final Serde<K> keySerde,
                                                   final Serde<V> valueSerde,
                                                   final Serde<VO> otherValueSerde) {
        return new Joined<>(keySerde, valueSerde, otherValueSerde, null);
    }

    /**
     * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
     * {@code null} values are accepted and will be replaced by the default serdes as defined in
     * config.
     *
     * @param keySerde the key serde to use. If {@code null} the default key serde from config will be
     * used
     * @param valueSerde the value serde to use. If {@code null} the default value serde from config
     * will be used
     * @param otherValueSerde the otherValue serde to use. If {@code null} the default value serde
     * from config will be used
     * @param name the name used as the base for naming components of the join including any
     * repartition topics
     * @param <K> key type
     * @param <V> value type
     * @param <VO> other value type
     * @return new {@code Joined} instance with the provided serdes
     */
    public static <K, V, VO> Joined<K, V, VO> with(final Serde<K> keySerde,
                                                   final Serde<V> valueSerde,
                                                   final Serde<VO> otherValueSerde,
                                                   final String name) {
        return new Joined<>(keySerde, valueSerde, otherValueSerde, name);
    }

    /**
     * Create an instance of {@code Joined} with  a key {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
     *
     * @param keySerde the key serde to use. If {@code null} the default key serde from config will be used
     * @param <K>      key type
     * @param <V>      value type
     * @param <VO>     other value type
     * @return new {@code Joined} instance configured with the keySerde
     */
    public static <K, V, VO> Joined<K, V, VO> keySerde(final Serde<K> keySerde) {
        return new Joined<>(keySerde, null, null, null);
    }

    /**
     * Create an instance of {@code Joined} with a value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
     *
     * @param valueSerde the value serde to use. If {@code null} the default value serde from config will be used
     * @param <K>        key type
     * @param <V>        value type
     * @param <VO>       other value type
     * @return new {@code Joined} instance configured with the valueSerde
     */
    public static <K, V, VO> Joined<K, V, VO> valueSerde(final Serde<V> valueSerde) {
        return new Joined<>(null, valueSerde, null, null);
    }

    /**
     * Create an instance of {@code Joined} with an other value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
     *
     * @param otherValueSerde the otherValue serde to use. If {@code null} the default value serde from config will be used
     * @param <K>             key type
     * @param <V>             value type
     * @param <VO>            other value type
     * @return new {@code Joined} instance configured with the otherValueSerde
     */
    public static <K, V, VO> Joined<K, V, VO> otherValueSerde(final Serde<VO> otherValueSerde) {
        return new Joined<>(null, null, otherValueSerde, null);
    }

    /**
     * Create an instance of {@code Joined} with base name for all components of the join, this may
     * include any repartition topics created to complete the join.
     *
     * @param name the name used as the base for naming components of the join including any
     * repartition topics
     * @param <K> key type
     * @param <V> value type
     * @param <VO> other value type
     * @return new {@code Joined} instance configured with the name
     *
     * @deprecated use {@link #as(String)} instead
     */
    @Deprecated
    public static <K, V, VO> Joined<K, V, VO> named(final String name) {
        return new Joined<>(null, null, null, name);
    }

    /**
     * Create an instance of {@code Joined} with base name for all components of the join, this may
     * include any repartition topics created to complete the join.
     *
     * @param name the name used as the base for naming components of the join including any
     * repartition topics
     * @param <K> key type
     * @param <V> value type
     * @param <VO> other value type
     * @return new {@code Joined} instance configured with the name
     *
     */
    public static <K, V, VO> Joined<K, V, VO> as(final String name) {
        return new Joined<>(null, null, null, name);
    }


    /**
     * Set the key {@link Serde} to be used. Null values are accepted and will be replaced by the default
     * key serde as defined in config
     *
     * @param keySerde the key serde to use. If null the default key serde from config will be used
     * @return new {@code Joined} instance configured with the {@code name}
     */
    public Joined<K, V, VO> withKeySerde(final Serde<K> keySerde) {
        return new Joined<>(keySerde, valueSerde, otherValueSerde, name);
    }

    /**
     * Set the value {@link Serde} to be used. Null values are accepted and will be replaced by the default
     * value serde as defined in config
     *
     * @param valueSerde the value serde to use. If null the default value serde from config will be used
     * @return new {@code Joined} instance configured with the {@code valueSerde}
     */
    public Joined<K, V, VO> withValueSerde(final Serde<V> valueSerde) {
        return new Joined<>(keySerde, valueSerde, otherValueSerde, name);
    }

    /**
     * Set the otherValue {@link Serde} to be used. Null values are accepted and will be replaced by the default
     * value serde as defined in config
     *
     * @param otherValueSerde the otherValue serde to use. If null the default value serde from config will be used
     * @return new {@code Joined} instance configured with the {@code valueSerde}
     */
    public Joined<K, V, VO> withOtherValueSerde(final Serde<VO> otherValueSerde) {
        return new Joined<>(keySerde, valueSerde, otherValueSerde, name);
    }

    /**
     * Set the base name used for all components of the join, this may include any repartition topics
     * created to complete the join.
     *
     * @param name the name used as the base for naming components of the join including any
     * repartition topics
     * @return new {@code Joined} instance configured with the {@code name}
     */
    @Override
    public Joined<K, V, VO> withName(final String name) {
        return new Joined<>(keySerde, valueSerde, otherValueSerde, name);
    }

    public Serde<K> keySerde() {
        return keySerde;
    }

    public Serde<V> valueSerde() {
        return valueSerde;
    }

    public Serde<VO> otherValueSerde() {
        return otherValueSerde;
    }

    /**
     * @deprecated this method will be removed in a in a future release
     */
    @Deprecated
    public String name() {
        return name;
    }

}
