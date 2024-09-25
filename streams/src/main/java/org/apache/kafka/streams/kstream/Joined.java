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

import java.time.Duration;

/**
 * The {@code Joined} class represents optional params that can be passed to
 * {@link KStream#join(KTable, ValueJoiner, Joined) KStream#join(KTable,...)} and
 * {@link KStream#leftJoin(KTable, ValueJoiner) KStream#leftJoin(KTable,...)} operations.
 *
 * @param <K> type of record key
 * @param <VLeft> type of left record value
 * @param <VRight> type of right record value
 */
public class Joined<K, VLeft, VRight> implements NamedOperation<Joined<K, VLeft, VRight>> {

    protected final Serde<K> keySerde;
    protected final Serde<VLeft> leftValueSerde;
    protected final Serde<VRight> rightValueSerde;
    protected final String name;
    protected final Duration gracePeriod;

    private Joined(final Serde<K> keySerde,
                   final Serde<VLeft> leftValueSerde,
                   final Serde<VRight> rightValueSerde,
                   final String name,
                   final Duration gracePeriod) {
        this.keySerde = keySerde;
        this.leftValueSerde = leftValueSerde;
        this.rightValueSerde = rightValueSerde;
        this.name = name;
        this.gracePeriod = gracePeriod;
    }

    protected Joined(final Joined<K, VLeft, VRight> joined) {
        this(joined.keySerde, joined.leftValueSerde, joined.rightValueSerde, joined.name, joined.gracePeriod);
    }

    /**
     * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
     * {@code null} values are accepted and will be replaced by the default serdes as defined in config.
     *
     * @param keySerde
     *        the key serde to use. If {@code null} the default key serde from config will be used
     * @param leftValueSerde
     *        the value serde to use. If {@code null} the default value serde from config will be used
     * @param rightValueSerde
     *        the otherValue serde to use. If {@code null} the default value serde from config will be used
     *
     * @param <K> key type
     * @param <VLeft> left value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance with the provided serdes
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> with(final Serde<K> keySerde,
                                                                   final Serde<VLeft> leftValueSerde,
                                                                   final Serde<VRight> rightValueSerde) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, null, null);
    }

    /**
     * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
     * {@code null} values are accepted and will be replaced by the default serdes as defined in
     * config.
     *
     * @param keySerde
     *        the key serde to use. If {@code null} the default key serde from config will be used
     * @param leftValueSerde
     *        the left value serde to use. If {@code null} the default value serde from config will be used
     * @param rightValueSerde
     *        the right value serde to use. If {@code null} the default value serde from config will be used
     * @param name
     *        the name used as the base for naming components of the join including any repartition topics
     *
     * @param <K> key type
     * @param <VLeft> left value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance with the provided serdes
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> with(final Serde<K> keySerde,
                                                                   final Serde<VLeft> leftValueSerde,
                                                                   final Serde<VRight> rightValueSerde,
                                                                   final String name) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, null);
    }

    /**
     * Create an instance of {@code Joined} with key, value, and otherValue {@link Serde} instances.
     * {@code null} values are accepted and will be replaced by the default serdes as defined in
     * config.
     *
     * @param keySerde
     *        the key serde to use. If {@code null} the default key serde from config will be used
     * @param leftValueSerde
     *        the left value serde to use. If {@code null} the default value serde from config will be used
     * @param rightValueSerde
     *        the right value serde to use. If {@code null} the default value serde from config will be used
     * @param name
     *        the name used as the base for naming components of the join including any repartition topics
     * @param gracePeriod
     *        stream buffer time
     *
     * @param <K> key type
     * @param <VLeft> value value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance with the provided serdes
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> with(final Serde<K> keySerde,
                                                                   final Serde<VLeft> leftValueSerde,
                                                                   final Serde<VRight> rightValueSerde,
                                                                   final String name,
                                                                   final Duration gracePeriod) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, gracePeriod);
    }

    /**
     * Create an instance of {@code Joined} with  a key {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default key serde as defined in config.
     *
     * @param keySerde
     *        the key serde to use. If {@code null} the default key serde from config will be used
     *
     * @param <K> key type
     * @param <VLeft> value value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance configured with the keySerde
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> keySerde(final Serde<K> keySerde) {
        return new Joined<>(keySerde, null, null, null, null);
    }

    /**
     * Create an instance of {@code Joined} with a value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
     *
     * @param leftValueSerde
     *        the left value serde to use. If {@code null} the default value serde from config will be used
     *
     * @param <K> key type
     * @param <VLeft> left value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance configured with the valueSerde
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> valueSerde(final Serde<VLeft> leftValueSerde) {
        return new Joined<>(null, leftValueSerde, null, null, null);
    }


    /**
     * Create an instance of {@code Joined} with aother value {@link Serde}.
     * {@code null} values are accepted and will be replaced by the default value serde as defined in config.
     *
     * @param rightValueSerde
     *        the right value serde to use. If {@code null} the default value serde from config will be used
     *
     * @param <K> key type
     * @param <VLeft> value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance configured with the otherValueSerde
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> otherValueSerde(final Serde<VRight> rightValueSerde) {
        return new Joined<>(null, null, rightValueSerde, null, null);
    }

    /**
     * Create an instance of {@code Joined} with base name for all components of the join, this may
     * include any repartition topics created to complete the join.
     *
     * @param name
     *        the name used as the base for naming components of the join including any repartition topics
     *
     * @param <K> key type
     * @param <VLeft> left value type
     * @param <VRight> right value type
     *
     * @return new {@code Joined} instance configured with the name
     */
    public static <K, VLeft, VRight> Joined<K, VLeft, VRight> as(final String name) {
        return new Joined<>(null, null, null, name, null);
    }

    /**
     * Set the key {@link Serde} to be used. Null values are accepted and will be replaced by the default
     * key serde as defined in config
     *
     * @param keySerde
     *        the key serde to use. If null the default key serde from config will be used
     *
     * @return new {@code Joined} instance configured with the {@code name}
     */
    public Joined<K, VLeft, VRight> withKeySerde(final Serde<K> keySerde) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, gracePeriod);
    }

    /**
     * Set the value {@link Serde} to be used. Null values are accepted and will be replaced by the default
     * value serde as defined in config
     *
     * @param leftValueSerde
     *        the left value serde to use. If null the default value serde from config will be used
     *
     * @return new {@code Joined} instance configured with the {@code valueSerde}
     */
    public Joined<K, VLeft, VRight> withValueSerde(final Serde<VLeft> leftValueSerde) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, gracePeriod);
    }

    /**
     * Set the otherValue {@link Serde} to be used. Null values are accepted and will be replaced by the default
     * value serde as defined in config
     *
     * @param rightValueSerde
     *        the right value serde to use. If null the default value serde from config will be used
     *
     * @return new {@code Joined} instance configured with the {@code valueSerde}
     */
    public Joined<K, VLeft, VRight> withOtherValueSerde(final Serde<VRight> rightValueSerde) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, gracePeriod);
    }

    /**
     * Set the base name used for all components of the join, this may include any repartition topics
     * created to complete the join.
     *
     * @param name
     *        the name used as the base for naming components of the join including any repartition topics
     *
     * @return new {@code Joined} instance configured with the {@code name}
     */
    @Override
    public Joined<K, VLeft, VRight> withName(final String name) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, gracePeriod);
    }

    /**
     * Set the grace period on the stream side of the join. Records will enter a buffer before being processed.
     * Out of order records in the grace period will be processed in timestamp order. Late records, out of the
     * grace period, will be executed right as they come in, if it is past the table history retention this could
     * result in a null join. Long gaps in stream side arriving records will cause
     * records to be delayed in processing.
     *
     * @param gracePeriod
     *        the duration of the grace period. Must be less than the joining table's history retention.
     *
     * @return new {@code Joined} instance configured with the gracePeriod
     */
    public Joined<K, VLeft, VRight> withGracePeriod(final Duration gracePeriod) {
        return new Joined<>(keySerde, leftValueSerde, rightValueSerde, name, gracePeriod);
    }


    /**
     * @deprecated since 4.0 and should not be used any longer.
     */
    @Deprecated
    public Duration gracePeriod() {
        return gracePeriod;
    }

    /**
     * @deprecated since 4.0 and should not be used any longer.
     */
    @Deprecated
    public Serde<K> keySerde() {
        return keySerde;
    }

    /**
     * @deprecated since 4.0 and should not be used any longer.
     */
    @Deprecated
    public Serde<VLeft> valueSerde() {
        return leftValueSerde;
    }

    /**
     * @deprecated since 4.0 and should not be used any longer.
     */
    @Deprecated
    public Serde<VRight> otherValueSerde() {
        return rightValueSerde;
    }
}
