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
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * {@code KTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 * Each record in this changelog stream is an update on the primary-keyed table with the record key as the primary key.
 * <p>
 * A {@code KTable} is either {@link StreamsBuilder#table(String) defined from a single Kafka topic} that is
 * consumed message by message or the result of a {@code KTable} transformation.
 * An aggregation of a {@link KStream} also yields a {@code KTable}.
 * <p>
 * A {@code KTable} can be transformed record by record, joined with another {@code KTable} or {@link KStream}, or
 * can be re-partitioned and aggregated into a new {@code KTable}.
 * <p>
 * Some {@code KTable}s have an internal state (a {@link ReadOnlyKeyValueStore}) and are therefore queryable via the
 * interactive queries API.
 * For example:
 * <pre>{@code
 *     final KTable table = ...
 *     ...
 *     final KafkaStreams streams = ...;
 *     streams.start()
 *     ...
 *     final String queryableStoreName = table.queryableStoreName(); // returns null if KTable is not queryable
 *     ReadOnlyKeyValueStore view = streams.store(queryableStoreName, QueryableStoreTypes.keyValueStore());
 *     view.get(key);
 *}</pre>
 *<p>
 * Records from the source topic that have null keys are dropped.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 * @see KStream
 * @see KGroupedTable
 * @see GlobalKTable
 * @see StreamsBuilder#table(String)
 */
@InterfaceStability.Evolving
public interface KTable<K, V> {

    /**
     * Create a new {@code KTable} that consists of all records of this {@code KTable} which satisfy the given
     * predicate.
     * All records that do not satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filter} for a <i>changelog stream</i> works different to {@link KStream#filter(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., dot not satisfy the given predicate) a tombstone record
     * is forwarded.
     *
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @return a {@code KTable} that contains only those records that satisfy the given predicate
     * @see #filterNot(Predicate)
     */
    KTable<K, V> filter(final Predicate<? super K, ? super V> predicate);

    /**
     * Create a new {@code KTable} that consists of all records of this {@code KTable} which satisfy the given
     * predicate.
     * All records that do not satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filter} for a <i>changelog stream</i> works different to {@link KStream#filter(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., dot not satisfy the given predicate) a tombstone record
     * is forwarded.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // filtering words
     * ReadOnlyKeyValueStore<K,V> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, V>keyValueStore());
     * K key = "some-word";
     * V valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * The store name to query with is specified by {@link Materialized#as(String)} or {@link Materialized#as(KeyValueBytesStoreSupplier)}.
     * <p>
     *
     * @param predicate     a filter {@link Predicate} that is applied to each record
     * @param materialized  a {@link Materialized} that describes how the {@link StateStore} for the resulting {@code KTable}
     *                      should be materialized. Cannot be {@code null}
     * @return a {@code KTable} that contains only those records that satisfy the given predicate
     * @see #filterNot(Predicate, Materialized)
     */
    KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                        final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Create a new {@code KTable} that consists of all records of this {@code KTable} which satisfy the given
     * predicate.
     * All records that do not satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filter} for a <i>changelog stream</i> works different to {@link KStream#filter(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., dot not satisfy the given predicate) a tombstone record
     * is forwarded.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // filtering words
     * ReadOnlyKeyValueStore<K,V> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, V>keyValueStore());
     * K key = "some-word";
     * V valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     *
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @param queryableStoreName a user-provided name of the underlying {@link KTable} that can be
     *                          used to subsequently query the operation results; valid characters are ASCII
     *                          alphanumerics, '.', '_' and '-'. If {@code null} then the results cannot be queried
     *                          (i.e., that would be equivalent to calling {@link KTable#filter(Predicate)}.
     * @return a {@code KTable} that contains only those records that satisfy the given predicate
     * @see #filterNot(Predicate, Materialized)
     * @deprecated use {@link #filter(Predicate, Materialized) filter(predicate, Materialized.as(queryableStoreName))}
     */
    @Deprecated
    KTable<K, V> filter(final Predicate<? super K, ? super V> predicate, final String queryableStoreName);

    /**
     * Create a new {@code KTable} that consists of all records of this {@code KTable} which satisfy the given
     * predicate.
     * All records that do not satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filter} for a <i>changelog stream</i> works different to {@link KStream#filter(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., dot not satisfy the given predicate) a tombstone record
     * is forwarded.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // filtering words
     * ReadOnlyKeyValueStore<K,V> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, V>keyValueStore());
     * K key = "some-word";
     * V valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     *
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains only those records that satisfy the given predicate
     * @see #filterNot(Predicate, Materialized)
     * @deprecated use {@link #filter(Predicate, Materialized) filter(predicate, Materialized.as(KeyValueByteStoreSupplier))}
     */
    @Deprecated
    KTable<K, V> filter(final Predicate<? super K, ? super V> predicate,
                        final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Create a new {@code KTable} that consists all records of this {@code KTable} which do <em>not</em> satisfy the
     * given predicate.
     * All records that <em>do</em> satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filterNot} for a <i>changelog stream</i> works different to {@link KStream#filterNot(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
     * forwarded.
     *
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
     * @see #filter(Predicate)
     */
    KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate);

    /**
     * Create a new {@code KTable} that consists all records of this {@code KTable} which do <em>not</em> satisfy the
     * given predicate.
     * All records that <em>do</em> satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filterNot} for a <i>changelog stream</i> works different to {@link KStream#filterNot(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
     * forwarded.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // filtering words
     * ReadOnlyKeyValueStore<K,V> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, V>keyValueStore());
     * K key = "some-word";
     * V valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * The store name to query with is specified by {@link Materialized#as(String)} or {@link Materialized#as(KeyValueBytesStoreSupplier)}.
     * <p>
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @param materialized  a {@link Materialized} that describes how the {@link StateStore} for the resulting {@code KTable}
     *                      should be materialized. Cannot be {@code null}
     * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
     * @see #filter(Predicate, Materialized)
     */
    KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                           final Materialized<K, V, KeyValueStore<Bytes, byte[]>> materialized);
    /**
     * Create a new {@code KTable} that consists all records of this {@code KTable} which do <em>not</em> satisfy the
     * given predicate.
     * All records that <em>do</em> satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filterNot} for a <i>changelog stream</i> works different to {@link KStream#filterNot(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
     * forwarded.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // filtering words
     * ReadOnlyKeyValueStore<K,V> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, V>keyValueStore());
     * K key = "some-word";
     * V valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
     * @see #filter(Predicate, Materialized)
     * @deprecated use {@link #filterNot(Predicate, Materialized) filterNot(predicate, Materialized.as(KeyValueByteStoreSupplier))}
     */
    @Deprecated
    KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate,
                           final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Create a new {@code KTable} that consists all records of this {@code KTable} which do <em>not</em> satisfy the
     * given predicate.
     * All records that <em>do</em> satisfy the predicate are dropped.
     * For each {@code KTable} update the filter is evaluated on the update record to produce an update record for the
     * result {@code KTable}.
     * This is a stateless record-by-record operation.
     * <p>
     * Note that {@code filterNot} for a <i>changelog stream</i> works different to {@link KStream#filterNot(Predicate)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided filter predicate is not evaluated but the tombstone record is forwarded
     * directly if required (i.e., if there is anything to be deleted).
     * Furthermore, for each record that gets dropped (i.e., does satisfy the given predicate) a tombstone record is
     * forwarded.
     * <p>
     * To query the local {@link KeyValueStore} it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * <pre>{@code
     * KafkaStreams streams = ... // filtering words
     * ReadOnlyKeyValueStore<K,V> localStore = streams.store(queryableStoreName, QueryableStoreTypes.<K, V>keyValueStore());
     * K key = "some-word";
     * V valueForKey = localStore.get(key); // key must be local (application state is shared over all running Kafka Streams instances)
     * }</pre>
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @param queryableStoreName a user-provided name of the underlying {@link KTable} that can be
     * used to subsequently query the operation results; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'. If {@code null} then the results cannot be queried
     * (i.e., that would be equivalent to calling {@link KTable#filterNot(Predicate)}.
     * @return a {@code KTable} that contains only those records that do <em>not</em> satisfy the given predicate
     * @see #filter(Predicate, Materialized)
     * @deprecated use {@link #filter(Predicate, Materialized) filterNot(predicate, Materialized.as(queryableStoreName))}
     */
    @Deprecated
    KTable<K, V> filterNot(final Predicate<? super K, ? super V> predicate, final String queryableStoreName);


    /**
     * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
     * (with possible new type)in the new {@code KTable}.
     * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the update record and
     * computes a new value for it, resulting in an update record for the result {@code KTable}.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * This is a stateless record-by-record operation.
     * <p>
     * The example below counts the number of token of the value string.
     * <pre>{@code
     * KTable<String, String> inputTable = builder.table("topic");
     * KTable<String, Integer> outputTable = inputTable.mapValue(new ValueMapper<String, Integer> {
     *     Integer apply(String value) {
     *         return value.split(" ").length;
     *     }
     * });
     * }</pre>
     * <p>
     * This operation preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
     * the result {@code KTable}.
     * <p>
     * Note that {@code mapValues} for a <i>changelog stream</i> works different to {@link KStream#mapValues(ValueMapper)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
     * delete the corresponding record in the result {@code KTable}.
     *
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param <VR>   the value type of the result {@code KTable}
     * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
     */
    <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper);

    /**
     * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
     * (with possible new type)in the new {@code KTable}.
     * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the update record and
     * computes a new value for it, resulting in an update record for the result {@code KTable}.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * This is a stateless record-by-record operation.
     * <p>
     * The example below counts the number of token of the value string.
     * <pre>{@code
     * KTable<String, String> inputTable = builder.table("topic");
     * KTable<String, Integer> outputTable = inputTable.mapValue(new ValueMapper<String, Integer> {
     *     Integer apply(String value) {
     *         return value.split(" ").length;
     *     }
     * });
     * }</pre>
     * <p>
     * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * The store name to query with is specified by {@link Materialized#as(String)} or {@link Materialized#as(KeyValueBytesStoreSupplier)}.
     * <p>
     * This operation preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
     * the result {@code KTable}.
     * <p>
     * Note that {@code mapValues} for a <i>changelog stream</i> works different to {@link KStream#mapValues(ValueMapper)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
     * delete the corresponding record in the result {@code KTable}.
     *
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param materialized  a {@link Materialized} that describes how the {@link StateStore} for the resulting {@code KTable}
     *                      should be materialized. Cannot be {@code null}
     * @param <VR>   the value type of the result {@code KTable}
     *
     * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
     */
    <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                 final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
     * (with possible new type)in the new {@code KTable}.
     * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the update record and
     * computes a new value for it, resulting in an update record for the result {@code KTable}.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * This is a stateless record-by-record operation.
     * <p>
     * The example below counts the number of token of the value string.
     * <pre>{@code
     * KTable<String, String> inputTable = builder.table("topic");
     * KTable<String, Integer> outputTable = inputTable.mapValue(new ValueMapper<String, Integer> {
     *     Integer apply(String value) {
     *         return value.split(" ").length;
     *     }
     * });
     * }</pre>
     * <p>
     * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * <p>
     * This operation preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
     * the result {@code KTable}.
     * <p>
     * Note that {@code mapValues} for a <i>changelog stream</i> works different to {@link KStream#mapValues(ValueMapper)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
     * delete the corresponding record in the result {@code KTable}.
     *
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param queryableStoreName a user-provided name of the underlying {@link KTable} that can be
     * used to subsequently query the operation results; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'. If {@code null} then the results cannot be queried
     * (i.e., that would be equivalent to calling {@link KTable#mapValues(ValueMapper)}.
     * @param valueSerde serializer for new value type
     * @param <VR>   the value type of the result {@code KTable}
     *
     * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
     * @deprecated use {@link #mapValues(ValueMapper, Materialized) mapValues(mapper, Materialized.as(queryableStoreName).withValueSerde(valueSerde))}
     */
    @Deprecated
    <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper, final Serde<VR> valueSerde, final String queryableStoreName);

    /**
     * Create a new {@code KTable} by transforming the value of each record in this {@code KTable} into a new value
     * (with possible new type)in the new {@code KTable}.
     * For each {@code KTable} update the provided {@link ValueMapper} is applied to the value of the update record and
     * computes a new value for it, resulting in an update record for the result {@code KTable}.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * This is a stateless record-by-record operation.
     * <p>
     * The example below counts the number of token of the value string.
     * <pre>{@code
     * KTable<String, String> inputTable = builder.table("topic");
     * KTable<String, Integer> outputTable = inputTable.mapValue(new ValueMapper<String, Integer> {
     *     Integer apply(String value) {
     *         return value.split(" ").length;
     *     }
     * });
     * }</pre>
     * <p>
     * To query the local {@link KeyValueStore} representing outputTable above it must be obtained via
     * {@link KafkaStreams#store(String, QueryableStoreType) KafkaStreams#store(...)}:
     * For non-local keys, a custom RPC mechanism must be implemented using {@link KafkaStreams#allMetadata()} to
     * query the value of the key on a parallel running instance of your Kafka Streams application.
     * <p>
     * <p>
     * This operation preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like a join) is applied to
     * the result {@code KTable}.
     * <p>
     * Note that {@code mapValues} for a <i>changelog stream</i> works different to {@link KStream#mapValues(ValueMapper)
     * record stream filters}, because {@link KeyValue records} with {@code null} values (so-called tombstone records)
     * have delete semantics.
     * Thus, for tombstones the provided value-mapper is not evaluated but the tombstone record is forwarded directly to
     * delete the corresponding record in the result {@code KTable}.
     *
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param valueSerde serializer for new value type
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @param <VR>   the value type of the result {@code KTable}
     * @return a {@code KTable} that contains records with unmodified keys and new values (possibly of different type)
     * @deprecated use {@link #mapValues(ValueMapper, Materialized) mapValues(mapper, Materialized.as(KeyValueByteStoreSupplier).withValueSerde(valueSerde))}
     */
    @Deprecated
    <VR> KTable<K, VR> mapValues(final ValueMapper<? super V, ? extends VR> mapper,
                                 final Serde<VR> valueSerde,
                                 final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);


    /**
     * Print the update records of this {@code KTable} to {@code System.out}.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the console.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code print()} is not applied to the internal state store and only called for each new {@code KTable}
     * update record.
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toSysOut())} on the result.
     */
    @Deprecated
    void print();

    /**
     * Print the update records of this {@code KTable} to {@code System.out}.
     * This function will use the given name to label the key/value pairs printed to the console.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code print()} is not applied to the internal state store and only called for each new {@code KTable}
     * update record.
     *
     * @param label the name used to label the key/value pairs printed to the console
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toSysOut().withLabel(lable))} on the result.
     */
    @Deprecated
    void print(final String label);

    /**
     * Print the update records of this {@code KTable} to {@code System.out}.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the console.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code print()} is not applied to the internal state store and only called for each new {@code KTable}
     * update record.
     *
     * @param keySerde key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde value serde used to deserialize value if type is {@code byte[]}
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toSysOut().withKeyValueMapper(...)} on the result.
     */
    @Deprecated
    void print(final Serde<K> keySerde,
               final Serde<V> valSerde);

    /**
     * Print the update records of this {@code KTable} to {@code System.out}.
     * This function will use the given name to label the key/value pairs printed to the console.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code print()} is not applied to the internal state store and only called for each new {@code KTable}
     * update record.
     *
     * @param keySerde   key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde   value serde used to deserialize value if type is {@code byte[]},
     * @param label the name used to label the key/value pairs printed to the console
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toSysOut().withLabel(label).withKeyValueMapper(...)} on the result.
     */
    @Deprecated
    void print(final Serde<K> keySerde,
               final Serde<V> valSerde,
               final String label);

    /**
     * Write the update records of this {@code KTable} to a file at the given path.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the file.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code writeAsText()} is not applied to the internal state store and only called for each new
     * {@code KTable} update record.
     *
     * @param filePath name of file to write to
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toFile(filePath)} on the result.
     */
    @Deprecated
    void writeAsText(final String filePath);

    /**
     * Write the update records of this {@code KTable} to a file at the given path.
     * This function will use the given name to label the key/value printed to the file.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code writeAsText()} is not applied to the internal state store and only called for each new
     * {@code KTable} update record.
     *
     * @param filePath   name of file to write to
     * @param label the name used to label the key/value pairs printed out to the console
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toFile(filePath).withLabel(label)} on the result.
     */
    @Deprecated
    void writeAsText(final String filePath,
                     final String label);

    /**
     * Write the update records of this {@code KTable} to a file at the given path.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the file.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code writeAsText()} is not applied to the internal state store and only called for each new
     * {@code KTable} update record.
     *
     * @param filePath name of file to write to
     * @param keySerde key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde value serde used to deserialize value if type is {@code byte[]}
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toFile(filePath).withKeyValueMapper(...)} on the result.
     */
    @Deprecated
    void  writeAsText(final String filePath,
                      final Serde<K> keySerde,
                      final Serde<V> valSerde);

    /**
     * Write the update records of this {@code KTable} to a file at the given path.
     * This function will use the given name to label the key/value printed to the file.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     * <p>
     * Note that {@code writeAsText()} is not applied to the internal state store and only called for each new
     * {@code KTable} update record.
     *
     * @param filePath name of file to write to
     * @param label the name used to label the key/value pairs printed to the console
     * @param keySerde key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde value serde used to deserialize value if type is {@code byte[]}
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#print(Printed) print(Printed.toFile(filePath).withLabel(label).withKeyValueMapper(...)} on the result.
     */
    @Deprecated
    void writeAsText(final String filePath,
                     final String label,
                     final Serde<K> keySerde,
                     final Serde<V> valSerde);

    /**
     * Perform an action on each update record of this {@code KTable}.
     * Note that this is a terminal operation that returns void.
     * <p>
     * Note that {@code foreach()} is not applied to the internal state store and only called for each new
     * {@code KTable} update record.
     *
     * @param action an action to perform on each record
     * @deprecated Use the Interactive Queries APIs (e.g., {@link KafkaStreams#store(String, QueryableStoreType) }
     * followed by {@link ReadOnlyKeyValueStore#all()}) to iterate over the keys of a KTable. Alternatively
     * convert to a {@link KStream} using {@link #toStream()} and then use
     * {@link KStream#foreach(ForeachAction) foreach(action)} on the result.
     */
    @Deprecated
    void foreach(final ForeachAction<? super K, ? super V> action);

    /**
     * Convert this changelog stream to a {@link KStream}.
     * <p>
     * Note that this is a logical operation and only changes the "interpretation" of the stream, i.e., each record of
     * this changelog stream is no longer treated as an update record (cf. {@link KStream} vs {@code KTable}).
     *
     * @return a {@link KStream} that contains the same records as this {@code KTable}
     */
    KStream<K, V> toStream();

    /**
     * Convert this changelog stream to a {@link KStream} using the given {@link KeyValueMapper} to select the new key.
     * <p>
     * For example, you can compute the new key as the length of the value string.
     * <pre>{@code
     * KTable<String, String> table = builder.table("topic");
     * KTable<Integer, String> keyedStream = table.toStream(new KeyValueMapper<String, String, Integer> {
     *     Integer apply(String key, String value) {
     *         return value.length();
     *     }
     * });
     * }</pre>
     * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
     * join) is applied to the result {@link KStream}.
     * <p>
     * This operation is equivalent to calling
     * {@code table.}{@link #toStream() toStream}{@code ().}{@link KStream#selectKey(KeyValueMapper) selectKey(KeyValueMapper)}.
     * <p>
     * Note that {@link #toStream()} is a logical operation and only changes the "interpretation" of the stream, i.e.,
     * each record of this changelog stream is no longer treated as an update record (cf. {@link KStream} vs {@code KTable}).
     *
     * @param mapper a {@link KeyValueMapper} that computes a new key for each record
     * @param <KR> the new key type of the result stream
     * @return a {@link KStream} that contains the same records as this {@code KTable}
     */
    <KR> KStream<KR, V> toStream(final KeyValueMapper<? super K, ? super V, ? extends KR> mapper);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using default
     * serializers and deserializers and producer's {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(String) #to(someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     * The store name must be a valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     *
     * @param topic     the topic name
     * @param queryableStoreName the state store name used for the result {@code KTable}; valid characters are ASCII
     *                  alphanumerics, '.', '_' and '-'. If {@code null} this is the equivalent of {@link KTable#through(String)}
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by {@link KStream#to(String) to(topic)} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(queryableStoreName))}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final String topic,
                         final String queryableStoreName);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using default
     * serializers and deserializers and producer's {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(String) #to(someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     * The store name must be a valid Kafka topic name and cannot contain characters other than ASCII alphanumerics, '.', '_' and '-'.
     *
     * @param topic     the topic name
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by {@link KStream#to(String) to(topic)} and
     * and {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(KeyValueBytesStoreSupplier))}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final String topic,
                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using default
     * serializers and deserializers and producer's {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(String) #to(someTopicName)} and
     * {@link KStreamBuilder#table(String) KStreamBuilder#table(someTopicName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with an internal store name (cf.
     * {@link KStreamBuilder#table(String)})
     *
     * @param topic     the topic name
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by {@link KStream#to(String) to(topic)} and
     * and {@link StreamsBuilder#table(String) StreamsBuilder#table(topic)} to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final String topic);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using default
     * serializers and deserializers and a customizable {@link StreamPartitioner} to determine the distribution of
     * records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(StreamPartitioner, String) #to(partitioner, someTopicName)} and
     * {@link KStreamBuilder#table(String) KStreamBuilder#table(someTopicName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with an internal store name (cf.
     * {@link KStreamBuilder#table(String)})
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic       the topic name
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.streamPartitioner(partitioner))} and
     * {@link StreamsBuilder#table(String) StreamsBuilder#table(topic)} to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                         final String topic);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using default
     * serializers and deserializers and a customizable {@link StreamPartitioner} to determine the distribution of
     * records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(StreamPartitioner, String) #to(partitioner, someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic       the topic name
     * @param queryableStoreName   the state store name used for the result {@code KTable}.
     *                             If {@code null} this is the equivalent of {@link KTable#through(StreamPartitioner, String)}
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.streamPartitioner(partitioner))} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(queryableStoreName))}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                         final String topic,
                         final String queryableStoreName);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using default
     * serializers and deserializers and a customizable {@link StreamPartitioner} to determine the distribution of
     * records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(StreamPartitioner, String) #to(partitioner, someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic       the topic name
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.streamPartitioner(partitioner))} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(KeyValueBytesStoreSupplier)}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                         final String topic,
                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * If {@code keySerde} provides a {@link WindowedSerializer} for the key {@link WindowedStreamPartitioner} is
     * used&mdash;otherwise producer's {@link DefaultPartitioner} is used.
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, String) #to(keySerde, valueSerde, someTopicName)} and
     * {@link KStreamBuilder#table(String, String) StreamsBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name
     * @param queryableStoreName the state store name used for the result {@code KTable}.
     *                           If {@code null} this is the equivalent of {@link KTable#through(Serde, Serde, String)}
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde))} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(queryableStoreName))}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final Serde<K> keySerde,
                         final Serde<V> valSerde,
                         final String topic,
                         final String queryableStoreName);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * If {@code keySerde} provides a {@link WindowedSerializer} for the key {@link WindowedStreamPartitioner} is
     * used&mdash;otherwise producer's {@link DefaultPartitioner} is used.
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, String) #to(keySerde, valueSerde, someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link StreamsBuilder#table(String, Materialized)})
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde))} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(KeyValueBytesStoreSupplier)}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final Serde<K> keySerde,
                         final Serde<V> valSerde,
                         final String topic,
                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * If {@code keySerde} provides a {@link WindowedSerializer} for the key {@link WindowedStreamPartitioner} is
     * used&mdash;otherwise producer's {@link DefaultPartitioner} is used.
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, String) #to(keySerde, valueSerde, someTopicName)} and
     * {@link KStreamBuilder#table(String) KStreamBuilder#table(someTopicName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with an interna; store name (cf.
     * {@link KStreamBuilder#table(String)})
     *
     * @param keySerde  key serde used to send key-value pairs,
     *                  if not specified the default key serde defined in the configuration will be used
     * @param valSerde  value serde used to send key-value pairs,
     *                  if not specified the default value serde defined in the configuration will be used
     * @param topic     the topic name
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde))}
     * and {@link StreamsBuilder#table(String) StreamsBuilder#table(topic)} to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final Serde<K> keySerde,
                         final Serde<V> valSerde,
                         final String topic);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, StreamPartitioner, String)
     * #to(keySerde, valueSerde, partitioner, someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     *
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will
     *                    be used
     * @param topic      the topic name
     * @param queryableStoreName  the state store name used for the result {@code KTable}.
     *                            If {@code null} this is the equivalent of {@link KTable#through(Serde, Serde, StreamPartitioner, String)()}
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde, partitioner))} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(queryableStoreName))}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final Serde<K> keySerde,
                         final Serde<V> valSerde,
                         final StreamPartitioner<? super K, ? super V> partitioner,
                         final String topic,
                         final String queryableStoreName);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, StreamPartitioner, String)
     * #to(keySerde, valueSerde, partitioner, someTopicName)} and
     * {@link KStreamBuilder#table(String, String) KStreamBuilder#table(someTopicName, queryableStoreName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with the given store name (cf.
     * {@link KStreamBuilder#table(String, String)})
     *
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will
     *                    be used
     * @param topic      the topic name
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde, partitioner))} and
     * {@link StreamsBuilder#table(String, Materialized) StreamsBuilder#table(topic, Materialized.as(KeyValueBytesStoreSupplier))}
     * to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final Serde<K> keySerde,
                         final Serde<V> valSerde,
                         final StreamPartitioner<? super K, ? super V> partitioner,
                         final String topic,
                         final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Materialize this changelog stream to a topic and creates a new {@code KTable} from the topic using a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, StreamPartitioner, String)
     * #to(keySerde, valueSerde, partitioner, someTopicName)} and
     * {@link KStreamBuilder#table(String) KStreamBuilder#table(someTopicName)}.
     * <p>
     * The resulting {@code KTable} will be materialized in a local state store with an internal store name (cf.
     * {@link KStreamBuilder#table(String)})
     *
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will
     *                    be used
     * @param topic      the topic name
     * @return a {@code KTable} that contains the exact same (and potentially repartitioned) records as this {@code KTable}
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde, partitioner))} and
     * {@link StreamsBuilder#table(String) StreamsBuilder#table(topic)} to read back as a {@code KTable}
     */
    @Deprecated
    KTable<K, V> through(final Serde<K> keySerde,
                         final Serde<V> valSerde,
                         final StreamPartitioner<? super K, ? super V> partitioner,
                         final String topic);

    /**
     * Materialize this changelog stream to a topic using default serializers and deserializers and producer's
     * {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param topic the topic name
     * @deprecated use {@link #toStream()} followed by {@link KStream#to(String) to(topic)}
     */
    @Deprecated
    void to(final String topic);

    /**
     * Materialize this changelog stream to a topic using default serializers and deserializers and a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic       the topic name
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.withStreamPartitioner(partitioner)}
     */
    @Deprecated
    void to(final StreamPartitioner<? super K, ? super V> partitioner,
            final String topic);

    /**
     * Materialize this changelog stream to a topic.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * If {@code keySerde} provides a {@link WindowedSerializer} for the key {@link WindowedStreamPartitioner} is
     * used&mdash;otherwise producer's {@link DefaultPartitioner} is used.
     *
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default key serde defined in the configuration will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default value serde defined in the configuration will be used
     * @param topic    the topic name
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde)}
     */
    @Deprecated
    void to(final Serde<K> keySerde,
            final Serde<V> valSerde,
            final String topic);

    /**
     * Materialize this changelog stream to a topic using a customizable {@link StreamPartitioner} to determine the
     * distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will
     *                    be used
     * @param topic      the topic name
     * @deprecated use {@link #toStream()} followed by
     * {@link KStream#to(String, Produced) to(topic, Produced.with(keySerde, valSerde, partioner)}
     */
    @Deprecated
    void to(final Serde<K> keySerde,
            final Serde<V> valSerde,
            final StreamPartitioner<? super K, ? super V> partitioner,
            final String topic);

    /**
     * Re-groups the records of this {@code KTable} using the provided {@link KeyValueMapper} and default serializers
     * and deserializers.
     * Each {@link KeyValue} pair of this {@code KTable} is mapped to a new {@link KeyValue} pair by applying the
     * provided {@link KeyValueMapper}.
     * Re-grouping a {@code KTable} is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedTable}).
     * The {@link KeyValueMapper} selects a new key and value (with should both have unmodified type).
     * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedTable}
     * <p>
     * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link  StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
     * an internally generated name, and "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * All data of this {@code KTable} will be redistributed through the repartitioning topic by writing all update
     * records to and rereading all update records from it, such that the resulting {@link KGroupedTable} is partitioned
     * on the new key.
     * <p>
     * If the key or value type is changed, it is recommended to use {@link #groupBy(KeyValueMapper, Serialized)}
     * instead.
     *
     * @param selector a {@link KeyValueMapper} that computes a new grouping key and value to be aggregated
     * @param <KR>     the key type of the result {@link KGroupedTable}
     * @param <VR>     the value type of the result {@link KGroupedTable}
     * @return a {@link KGroupedTable} that contains the re-grouped records of the original {@code KTable}
     */
    <KR, VR> KGroupedTable<KR, VR> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector);

    /**
     * Re-groups the records of this {@code KTable} using the provided {@link KeyValueMapper}
     * and {@link Serde}s as specified by {@link Serialized}.
     * Each {@link KeyValue} pair of this {@code KTable} is mapped to a new {@link KeyValue} pair by applying the
     * provided {@link KeyValueMapper}.
     * Re-grouping a {@code KTable} is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedTable}).
     * The {@link KeyValueMapper} selects a new key and value (with should both have unmodified type).
     * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedTable}
     * <p>
     * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link  StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
     * an internally generated name, and "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * All data of this {@code KTable} will be redistributed through the repartitioning topic by writing all update
     * records to and rereading all update records from it, such that the resulting {@link KGroupedTable} is partitioned
     * on the new key.
     *
     * @param selector      a {@link KeyValueMapper} that computes a new grouping key and value to be aggregated
     * @param serialized    the {@link Serialized} instance used to specify {@link org.apache.kafka.common.serialization.Serdes}
     * @param <KR>          the key type of the result {@link KGroupedTable}
     * @param <VR>          the value type of the result {@link KGroupedTable}
     * @return a {@link KGroupedTable} that contains the re-grouped records of the original {@code KTable}
     */
    <KR, VR> KGroupedTable<KR, VR> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
                                           final Serialized<KR, VR> serialized);

    /**
     * Re-groups the records of this {@code KTable} using the provided {@link KeyValueMapper}.
     * Each {@link KeyValue} pair of this {@code KTable} is mapped to a new {@link KeyValue} pair by applying the
     * provided {@link KeyValueMapper}.
     * Re-grouping a {@code KTable} is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedTable}).
     * The {@link KeyValueMapper} selects a new key and value (both with potentially different type).
     * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedTable}
     * <p>
     * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link  StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
     * an internally generated name, and "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * All data of this {@code KTable} will be redistributed through the repartitioning topic by writing all update
     * records to and rereading all update records from it, such that the resulting {@link KGroupedTable} is partitioned
     * on the new key.
     *
     * @param selector   a {@link KeyValueMapper} that computes a new grouping key and value to be aggregated
     * @param keySerde   key serdes for materializing this stream,
     *                   if not specified the default serdes defined in the configs will be used
     * @param valueSerde value serdes for materializing this stream,
     *                   if not specified the default serdes defined in the configs will be used
     * @param <KR>       the key type of the result {@link KGroupedTable}
     * @param <VR>       the value type of the result {@link KGroupedTable}
     * @return a {@link KGroupedTable} that contains the re-grouped records of the original {@code KTable}
     * @deprecated use {@link #groupBy(KeyValueMapper, Serialized) groupBy(selector, Serialized.with(keySerde, valueSerde)}
     */
    @Deprecated
    <KR, VR> KGroupedTable<KR, VR> groupBy(final KeyValueMapper<? super K, ? super V, KeyValue<KR, VR>> selector,
                                           final Serde<KR> keySerde,
                                           final Serde<VR> valueSerde);

    /**
     * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
     * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key
     * @see #leftJoin(KTable, ValueJoiner)
     * @see #outerJoin(KTable, ValueJoiner)
     */
    <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                final ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    /**
     * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
     * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other         the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
     *                      Cannot be {@code null}
     * @param <VO>          the value type of the other {@code KTable}
     * @param <VR>          the value type of the result {@code KTable}
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key
     * @see #leftJoin(KTable, ValueJoiner, Materialized)
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     */
    <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
    /**
     * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
     * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @param joinSerde serializer for join result value type
     * @param queryableStoreName a user-provided name of the underlying {@link KTable} that can be
     * used to subsequently query the operation results; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'. If {@code null} then the results cannot be queried
     * (i.e., that would be equivalent to calling {@link KTable#join(KTable, ValueJoiner)}.
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key
     * @see #leftJoin(KTable, ValueJoiner, Materialized)
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     * @deprecated use {@link #join(KTable, ValueJoiner, Materialized) join(other, joiner, Materialized.as(queryableStoreName).withValueSerde(joinSerde)}
     */
    @Deprecated
    <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                final Serde<VR> joinSerde,
                                final String queryableStoreName);

    /**
     * Join records of this {@code KTable} with another {@code KTable}'s records using non-windowed inner equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded
     * directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:C&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key
     * @see #leftJoin(KTable, ValueJoiner, Materialized)
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     * @deprecated use {@link #join(KTable, ValueJoiner, Materialized) join(other, joiner, Materialized.as(KeyValueByteStoreSupplier)}
     */
    @Deprecated
    <VO, VR> KTable<K, VR> join(final KTable<K, VO> other,
                                final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);


    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed left equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, all records from left {@code KTable} will produce
     * an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
     * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
     * null} to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
     * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
     * deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * left {@code KTable}
     * @see #join(KTable, ValueJoiner)
     * @see #outerJoin(KTable, ValueJoiner)
     */
    <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                    final ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed left equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, all records from left {@code KTable} will produce
     * an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
     * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
     * null} to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
     * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
     * deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other         the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
     *                      Cannot be {@code null}
     * @param <VO>          the value type of the other {@code KTable}
     * @param <VR>          the value type of the result {@code KTable}
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * left {@code KTable}
     * @see #join(KTable, ValueJoiner, Materialized)
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     */
    <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                    final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                    final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);
    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed left equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, all records from left {@code KTable} will produce
     * an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
     * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
     * null} to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
     * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
     * deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @param joinSerde serializer for join result value type
     * @param queryableStoreName a user-provided name of the underlying {@link KTable} that can be
     * used to subsequently query the operation results; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'. If {@code null} then the results cannot be queried
     * (i.e., that would be equivalent to calling {@link KTable#leftJoin(KTable, ValueJoiner)}.
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * left {@code KTable}
     * @see #join(KTable, ValueJoiner, Materialized)
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     * @deprecated use {@link #leftJoin(KTable, ValueJoiner, Materialized) leftJoin(other, joiner, Materialized.as(queryableStoreName).withValueSerde(joinSerde)}
     */
    @Deprecated
    <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                    final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                    final Serde<VR> joinSerde,
                                    final String queryableStoreName);

    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed left equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, all records from left {@code KTable} will produce
     * an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record of left {@code KTable} that does not find a corresponding record in the
     * right {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code rightValue =
     * null} to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * For example, for left input tombstones the provided value-joiner is not called but a tombstone record is
     * forwarded directly to delete a record in the result {@code KTable} if required (i.e., if there is anything to be
     * deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * left {@code KTable}
     * @see #join(KTable, ValueJoiner, Materialized)
     * @see #outerJoin(KTable, ValueJoiner, Materialized)
     * @deprecated use {@link #leftJoin(KTable, ValueJoiner, Materialized) leftJoin(other, joiner, Materialized.as(KeyValueByteStoreSupplier)}
     */
    @Deprecated
    <VO, VR> KTable<K, VR> leftJoin(final KTable<K, VO> other,
                                    final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                    final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);


    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed outer equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
     * all records from both input {@code KTable}s will produce an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record that does not find a corresponding record in the corresponding other
     * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
     * corresponding other value to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
     * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * both {@code KTable}s
     * @see #join(KTable, ValueJoiner)
     * @see #leftJoin(KTable, ValueJoiner)
     */
    <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                     final ValueJoiner<? super V, ? super VO, ? extends VR> joiner);

    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed outer equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
     * all records from both input {@code KTable}s will produce an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record that does not find a corresponding record in the corresponding other
     * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
     * corresponding other value to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
     * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other         the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner        a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param materialized  an instance of {@link Materialized} used to describe how the state store should be materialized.
     *                      Cannot be {@code null}
     * @param <VO>          the value type of the other {@code KTable}
     * @param <VR>          the value type of the result {@code KTable}
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * both {@code KTable}s
     * @see #join(KTable, ValueJoiner)
     * @see #leftJoin(KTable, ValueJoiner)
     */
    <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                     final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                     final Materialized<K, VR, KeyValueStore<Bytes, byte[]>> materialized);

    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed outer equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
     * all records from both input {@code KTable}s will produce an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record that does not find a corresponding record in the corresponding other
     * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
     * corresponding other value to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
     * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @param joinSerde serializer for join result value type
     * @param queryableStoreName a user-provided name of the underlying {@link KTable} that can be
     * used to subsequently query the operation results; valid characters are ASCII
     * alphanumerics, '.', '_' and '-'. If {@code null} then the results cannot be queried
     * (i.e., that would be equivalent to calling {@link KTable#outerJoin(KTable, ValueJoiner)}.
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * both {@code KTable}s
     * @see #join(KTable, ValueJoiner, Materialized)
     * @see #leftJoin(KTable, ValueJoiner, Materialized)
     * @deprecated use {@link #outerJoin(KTable, ValueJoiner, Materialized) outerJoin(other, joiner, Materialized.as(queryableStoreName).withValueSerde(joinSerde)}
     */
    @Deprecated
    <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                     final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                     final Serde<VR> joinSerde,
                                     final String queryableStoreName);

    /**
     * Join records of this {@code KTable} (left input) with another {@code KTable}'s (right input) records using
     * non-windowed outer equi join.
     * The join is a primary key join with join attribute {@code thisKTable.key == otherKTable.key}.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join} or {@link #leftJoin(KTable, ValueJoiner) left-join},
     * all records from both input {@code KTable}s will produce an output record (cf. below).
     * The result is an ever updating {@code KTable} that represents the <em>current</em> (i.e., processing time) result
     * of the join.
     * <p>
     * The join is computed by (1) updating the internal state of one {@code KTable} and (2) performing a lookup for a
     * matching record in the <em>current</em> (i.e., processing time) internal state of the other {@code KTable}.
     * This happens in a symmetric way, i.e., for each update of either {@code this} or the {@code other} input
     * {@code KTable} the result gets updated.
     * <p>
     * For each {@code KTable} record that finds a corresponding record in the other {@code KTable}'s state the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * Additionally, for each record that does not find a corresponding record in the corresponding other
     * {@code KTable}'s state the provided {@link ValueJoiner} will be called with {@code null} value for the
     * corresponding other value to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * <p>
     * Note that {@link KeyValue records} with {@code null} values (so-called tombstone records) have delete semantics.
     * Thus, for input tombstones the provided value-joiner is not called but a tombstone record is forwarded directly
     * to delete a record in the result {@code KTable} if required (i.e., if there is anything to be deleted).
     * <p>
     * Input records with {@code null} key will be dropped and no join computation is performed.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>thisKTable</th>
     * <th>thisState</th>
     * <th>otherKTable</th>
     * <th>otherState</th>
     * <th>result update record</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:A&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(A,b)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(null,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * <td></td>
     * <td>&lt;K1:null&gt;</td>
     * </tr>
     * </table>
     * Both input streams (or to be more precise, their underlying source topics) need to have the same number of
     * partitions.
     *
     * @param other  the other {@code KTable} to be joined with this {@code KTable}
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VO>   the value type of the other {@code KTable}
     * @param <VR>   the value type of the result {@code KTable}
     * @param storeSupplier user defined state store supplier. Cannot be {@code null}.
     * @return a {@code KTable} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * both {@code KTable}s
     * @see #join(KTable, ValueJoiner)
     * @see #leftJoin(KTable, ValueJoiner)
     * @deprecated use {@link #outerJoin(KTable, ValueJoiner, Materialized) outerJoin(other, joiner, Materialized.as(KeyValueByteStoreSupplier)}
     */
    @Deprecated
    <VO, VR> KTable<K, VR> outerJoin(final KTable<K, VO> other,
                                     final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                     final org.apache.kafka.streams.processor.StateStoreSupplier<KeyValueStore> storeSupplier);

    /**
     * Get the name of the local state store used that can be used to query this {@code KTable}.
     *
     * @return the underlying state store name, or {@code null} if this {@code KTable} cannot be queried.
     */
    String queryableStoreName();
}
