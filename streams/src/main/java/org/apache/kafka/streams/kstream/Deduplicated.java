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
 * the internal processor and state store when performing
 * {@link KStream#deduplicateByKey(java.time.Duration, Deduplicated)} or
 * {@link KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)} operations.
 *
 * @param <K>  the record key type
 * @param <KR> the computed deduplication ID type (same as {@code K} for {@code deduplicateByKey})
 * @param <V>  the value type
 */
public class Deduplicated<K, KR, V> implements NamedOperation<Deduplicated<K, KR, V>> {

    protected final String name;
    protected final String storeName;
    protected final Serde<K> keySerde;
    protected final Serde<KR> idSerde;
    protected final Serde<V> valueSerde;

    private Deduplicated(final String name,
                         final String storeName,
                         final Serde<K> keySerde,
                         final Serde<KR> idSerde,
                         final Serde<V> valueSerde) {
        this.name = name;
        this.storeName = storeName;
        this.keySerde = keySerde;
        this.idSerde = idSerde;
        this.valueSerde = valueSerde;
    }

    protected Deduplicated(final Deduplicated<K, KR, V> deduplicated) {
        this(deduplicated.name, deduplicated.storeName, deduplicated.keySerde, deduplicated.idSerde, deduplicated.valueSerde);
    }

    /**
     * Create a {@link Deduplicated} instance with the provided name used as the processor name
     * and as part of the internal store name.
     *
     * @param name
     *        the name used for the processor and state store
     * @param <K>   the record key type
     * @param <KR>  the computed deduplication ID type
     * @param <V>   the value type
     *
     * @return a new {@link Deduplicated} configured with the name
     *
     * @see KStream#deduplicateByKey(java.time.Duration, Deduplicated)
     * @see KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)
     */
    public static <K, KR, V> Deduplicated<K, KR, V> as(final String name) {
        return new Deduplicated<>(name, null, null, null, null);
    }

    /**
     * Create a {@link Deduplicated} instance with the provided keySerde.  If {@code null} the default key serde from config will be used.
     *
     * @param keySerde
     *        the Serde used for serializing the record key. If {@code null} the default key serde from config will be used
     * @param <K>   the record key type
     * @param <KR>  the computed deduplication ID type
     * @param <V>   the value type
     *
     * @return a new {@link Deduplicated} configured with the keySerde
     *
     * @see KStream#deduplicateByKey(java.time.Duration, Deduplicated)
     * @see KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)
     */
    public static <K, KR, V> Deduplicated<K, KR, V> keySerde(final Serde<K> keySerde) {
        return new Deduplicated<>(null, null, keySerde, null, null);
    }

    /**
     * Create a {@link Deduplicated} instance with the provided idSerde.  Required for
     * {@link KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)}.
     *
     * @param idSerde
     *        the {@link Serde} used for serializing the computed deduplication ID
     * @param <K>   the record key type
     * @param <KR>  the computed deduplication ID type
     * @param <V>   the value type
     *
     * @return a new {@link Deduplicated} configured with the idSerde
     *
     * @see KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)
     */
    public static <K, KR, V> Deduplicated<K, KR, V> idSerde(final Serde<KR> idSerde) {
        return new Deduplicated<>(null, null, null, idSerde, null);
    }

    /**
     * Create a {@link Deduplicated} instance with the provided valueSerde.  If {@code null} the default value serde from config will be used.
     *
     * @param valueSerde
     *        the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
     * @param <K>   the record key type
     * @param <KR>  the computed deduplication ID type
     * @param <V>   the value type
     *
     * @return a new {@link Deduplicated} configured with the valueSerde
     *
     * @see KStream#deduplicateByKey(java.time.Duration, Deduplicated)
     * @see KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)
     */
    public static <K, KR, V> Deduplicated<K, KR, V> valueSerde(final Serde<V> valueSerde) {
        return new Deduplicated<>(null, null, null, null, valueSerde);
    }

    /**
     * Create a {@link Deduplicated} instance with the provided keySerde and valueSerde.  If the keySerde and/or the valueSerde is
     * {@code null} the default value for the respective serde from config will be used.
     *
     * @param keySerde
     *        the {@link Serde} used for serializing the record key. If {@code null} the default key serde from config will be used
     * @param valueSerde
     *        the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
     * @param <K>   the record key type
     * @param <KR>  the computed deduplication ID type
     * @param <V>   the value type
     *
     * @return a new {@link Deduplicated} configured with the keySerde and valueSerde
     *
     * @see KStream#deduplicateByKey(java.time.Duration, Deduplicated)
     * @see KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)
     */
    public static <K, KR, V> Deduplicated<K, KR, V> with(final Serde<K> keySerde,
                                                          final Serde<V> valueSerde) {
        return new Deduplicated<>(null, null, keySerde, null, valueSerde);
    }

    /**
     * Create a {@link Deduplicated} instance with the provided keySerde, idSerde, and valueSerde.  If the keySerde and/or the valueSerde is
     * {@code null} the default value for the respective serde from config will be used.
     *
     * @param keySerde
     *        the {@link Serde} used for serializing the record key. If {@code null} the default key serde from config will be used
     * @param idSerde
     *        the {@link Serde} used for serializing the computed deduplication ID
     * @param valueSerde
     *        the {@link Serde} used for serializing the value. If {@code null} the default value serde from config will be used
     * @param <K>   the record key type
     * @param <KR>  the computed deduplication ID type
     * @param <V>   the value type
     *
     * @return a new {@link Deduplicated} configured with the keySerde, idSerde, and valueSerde
     *
     * @see KStream#deduplicateByKeyValue(KeyValueMapper, java.time.Duration, Deduplicated)
     */
    public static <K, KR, V> Deduplicated<K, KR, V> with(final Serde<K> keySerde,
                                                          final Serde<KR> idSerde,
                                                          final Serde<V> valueSerde) {
        return new Deduplicated<>(null, null, keySerde, idSerde, valueSerde);
    }

    /**
     * Set the name to be used for the deduplication processor and state store.
     *
     * @param name
     *        the name used for the processor and as part of the state store name
     *
     * @return a new {@link Deduplicated} instance configured with the name
     */
    @Override
    public Deduplicated<K, KR, V> withName(final String name) {
        return new Deduplicated<>(name, storeName, keySerde, idSerde, valueSerde);
    }

    /**
     * Set the name to be used for the internal deduplication state store.
     *
     * @param storeName
     *        the name to use for the state store
     *
     * @return a new {@link Deduplicated} instance configured with the store name
     */
    public Deduplicated<K, KR, V> withStoreName(final String storeName) {
        return new Deduplicated<>(name, storeName, keySerde, idSerde, valueSerde);
    }

    /**
     * Set the keySerde to be used for serializing the record key.
     *
     * @param keySerde
     *        {@link Serde} to use for serializing the record key. If {@code null} the default key serde from config will be used
     *
     * @return a new {@link Deduplicated} instance configured with the keySerde
     */
    public Deduplicated<K, KR, V> withKeySerde(final Serde<K> keySerde) {
        return new Deduplicated<>(name, storeName, keySerde, idSerde, valueSerde);
    }

    /**
     * Set the idSerde to be used for serializing the computed deduplication ID.
     *
     * @param idSerde
     *        {@link Serde} to use for serializing the computed deduplication ID
     *
     * @return a new {@link Deduplicated} instance configured with the idSerde
     */
    public Deduplicated<K, KR, V> withIdSerde(final Serde<KR> idSerde) {
        return new Deduplicated<>(name, storeName, keySerde, idSerde, valueSerde);
    }

    /**
     * Set the valueSerde to be used for serializing the value.
     *
     * @param valueSerde
     *        {@link Serde} to use for serializing the value. If {@code null} the default value serde from config will be used
     *
     * @return a new {@link Deduplicated} instance configured with the valueSerde
     */
    public Deduplicated<K, KR, V> withValueSerde(final Serde<V> valueSerde) {
        return new Deduplicated<>(name, storeName, keySerde, idSerde, valueSerde);
    }
}
