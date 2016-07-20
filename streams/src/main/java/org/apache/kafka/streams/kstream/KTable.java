/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;

/**
 * {@link KTable} is an abstraction of a <i>changelog stream</i> from a primary-keyed table.
 * Each record in this stream is an update on the primary-keyed table with the record key as the primary key.
 * <p>
 * A {@link KTable} is either defined from one or multiple Kafka topics that are consumed message by message or
 * the result of a {@link KTable} transformation. An aggregation of a {@link KStream} also yields a {@link KTable}.
 * <p>
 * A {@link KTable} can be transformed record by record, joined with another {@link KTable} or {@link KStream}, or
 * can be re-partitioned and aggregated into a new {@link KTable}.
 *
 * @param <K> Type of primary keys
 * @param <V> Type of value changes
 *
 * @see KStream
 */
@InterfaceStability.Unstable
public interface KTable<K, V> {

    /**
     * Create a new instance of {@link KTable} that consists of all elements of this stream which satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     *
     * @return a {@link KTable} that contains only those records that satisfy the given predicate
     */
    KTable<K, V> filter(Predicate<K, V> predicate);

    /**
     * Create a new instance of {@link KTable} that consists all elements of this stream which do not satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     *
     * @return a {@link KTable} that contains only those records that do not satisfy the given predicate
     */
    KTable<K, V> filterNot(Predicate<K, V> predicate);

    /**
     * Create a new instance of {@link KTable} by transforming the value of each element in this stream into a new value in the new stream.
     *
     * @param mapper        the instance of {@link ValueMapper}
     * @param <V1>          the value type of the new stream
     *
     * @return a {@link KTable} that contains records with unmodified keys and new values of different type
     */
    <V1> KTable<K, V1> mapValues(ValueMapper<V, V1> mapper);


    /**
     * Print the elements of this stream to {@code System.out}. This function
     * will use the generated name of the parent processor node to label the key/value pairs
     * printed out to the console.
     *
     * Implementors will need to override toString for keys and values that are not of
     * type String, Integer etc to get meaningful information.
     */
    void print();

    /**
     * Print the elements of this stream to {@code System.out}.  This function
     * will use the given name to label the key/value printed out to the console.
     *
     * @param streamName the name used to label the key/value pairs printed out to the console
     *
     * Implementors will need to override toString for keys and values that are not of
     * type String, Integer etc to get meaningful information.
     */
    void print(String streamName);

    /**
     * Print the elements of this stream to {@code System.out}
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     *
     * Implementors will need to override toString for keys and values that are not of
     * type String, Integer etc to get meaningful information.
     */
    void print(Serde<K> keySerde, Serde<V> valSerde);

    /**
     * Print the elements of this stream to System.out
     *
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param streamName the name used to label the key/value pairs printed out to the console
     *
     * Implementors will need to override toString for keys and values that are not of
     * type String, Integer etc to get meaningful information.
     */
    void print(Serde<K> keySerde, Serde<V> valSerde, String streamName);

    /**
     * Write the elements of this stream to a file at the given path using default serializers and deserializers.
     * @param filePath name of file to write to
     *
     * Implementors will need to override {@code toString} for keys and values that are not of
     * type {@link String}, {@link Integer} etc. to get meaningful information.
     */
    void writeAsText(String filePath);

    /**
     * Write the elements of this stream to a file at the given path.
     *
     * @param filePath name of file to write to
     * @param streamName the name used to label the key/value pairs printed out to the console
     *
     * Implementors will need to override {@code toString} for keys and values that are not of
     * type {@link String}, {@link Integer} etc. to get meaningful information.
     */
    void writeAsText(String filePath, String streamName);

    /**
     * Write the elements of this stream to a file at the given path.
     *
     * @param filePath name of file to write to
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     *
     * Implementors will need to override {@code toString} for keys and values that are not of
     * type {@link String}, {@link Integer} etc. to get meaningful information.
     */
    void  writeAsText(String filePath, Serde<K> keySerde, Serde<V> valSerde);

    /**
     * @param filePath name of file to write to
     * @param streamName the name used to label the key/value pairs printed out to the console
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     *
     * Implementors will need to override {@code toString} for keys and values that are not of
     * type {@link String}, {@link Integer} etc. to get meaningful information.
     */

    void writeAsText(String filePath, String streamName, Serde<K> keySerde, Serde<V> valSerde);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KTable} from the topic
     * using default serializers and deserializers and producer's {@link DefaultPartitioner}.
     * This is equivalent to calling {@link #to(String)} and {@link org.apache.kafka.streams.kstream.KStreamBuilder#table(String, String)}.
     * The resulting {@link KTable} will be materialized in a local state
     * store with the given store name. Also a changelog topic named "${applicationId}-${storeName}-changelog"
     * will be automatically created in Kafka for failure recovery, where "applicationID"
     * is specified by the user in {@link org.apache.kafka.streams.StreamsConfig}.
     *
     * @param topic         the topic name
     * @param storeName     the state store name used for this KTable
     * @return a new {@link KTable} that contains the exact same records as this {@link KTable}
     */
    KTable<K, V> through(String topic, String storeName);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KTable} from the topic using default serializers
     * and deserializers and a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     * This is equivalent to calling {@link #to(String)} and {@link org.apache.kafka.streams.kstream.KStreamBuilder#table(String, String)}.
     * The resulting {@link KTable} will be materialized in a local state
     * store with the given store name. Also a changelog topic named "${applicationId}-${storeName}-changelog"
     * will be automatically created in Kafka for failure recovery, where "applicationID"
     * is specified by the user in {@link org.apache.kafka.streams.StreamsConfig}.
     *
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic        the topic name
     * @param storeName    the state store name used for this KTable
     * @return a new {@link KTable} that contains the exact same records as this {@link KTable}
     */
    KTable<K, V> through(StreamPartitioner<K, V> partitioner, String topic, String storeName);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KTable} from the topic.
     * If {@code keySerde} provides a {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer}
     * for the key {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} is used
     * &mdash; otherwise producer's {@link DefaultPartitioner} is used.
     * This is equivalent to calling {@link #to(Serde, Serde, String)} and
     * {@link org.apache.kafka.streams.kstream.KStreamBuilder#table(Serde, Serde, String, String)}.
     * The resulting {@link KTable} will be materialized in a local state
     * store with the given store name. Also a changelog topic named "${applicationId}-${storeName}-changelog"
     * will be automatically created in Kafka for failure recovery, where "applicationID"
     * is specified by the user in {@link org.apache.kafka.streams.StreamsConfig}.
     *
     * @param keySerde     key serde used to send key-value pairs,
     *                     if not specified the default key serde defined in the configuration will be used
     * @param valSerde     value serde used to send key-value pairs,
     *                     if not specified the default value serde defined in the configuration will be used
     * @param topic        the topic name
     * @param storeName    the state store name used for this KTable
     * @return a new {@link KTable} that contains the exact same records as this {@link KTable}
     */
    KTable<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic, String storeName);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KTable} from the topic
     * using a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     * This is equivalent to calling {@link #to(Serde, Serde, StreamPartitioner, String)} and
     * {@link org.apache.kafka.streams.kstream.KStreamBuilder#table(Serde, Serde, String, String)}.
     * The resulting {@link KTable} will be materialized in a local state
     * store with the given store name. Also a changelog topic named "${applicationId}-${storeName}-changelog"
     * will be automatically created in Kafka for failure recovery, where "applicationID"
     * is specified by the user in {@link org.apache.kafka.streams.StreamsConfig}.
     *
     * @param keySerde     key serde used to send key-value pairs,
     *                     if not specified the default key serde defined in the configuration will be used
     * @param valSerde     value serde used to send key-value pairs,
     *                     if not specified the default value serde defined in the configuration will be used
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified and {@code keySerde} provides a {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer} for the key
     *                     {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} will be used
     *                     &mdash; otherwise {@link DefaultPartitioner} will be used
     * @param topic        the topic name
     * @param storeName    the state store name used for this KTable
     * @return a new {@link KTable} that contains the exact same records as this {@link KTable}
     */
    KTable<K, V> through(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic, String storeName);

    /**
     * Materialize this stream to a topic using default serializers specified in the config
     * and producer's {@link DefaultPartitioner}.
     *
     * @param topic         the topic name
     */
    void to(String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config
     * and a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     *
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic        the topic name
     */
    void to(StreamPartitioner<K, V> partitioner, String topic);

    /**
     * Materialize this stream to a topic. If {@code keySerde} provides a
     * {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer} for the key
     * {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} is used
     * &mdash; otherwise producer's {@link DefaultPartitioner} is used.
     *
     * @param keySerde     key serde used to send key-value pairs,
     *                     if not specified the default serde defined in the configs will be used
     * @param valSerde     value serde used to send key-value pairs,
     *                     if not specified the default serde defined in the configs will be used
     * @param topic        the topic name
     */
    void to(Serde<K> keySerde, Serde<V> valSerde, String topic);

    /**
     * Materialize this stream to a topic using a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     *
     * @param keySerde     key serde used to send key-value pairs,
     *                     if not specified the default serde defined in the configs will be used
     * @param valSerde     value serde used to send key-value pairs,
     *                     if not specified the default serde defined in the configs will be used
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified and {@code keySerde} provides a {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer} for the key
     *                     {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} will be used
     *                     &mdash; otherwise {@link DefaultPartitioner} will be used
     * @param topic        the topic name
     */
    void to(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic);

    /**
     * Convert this stream to a new instance of {@link KStream}.
     *
     * @return a {@link KStream} that contains the same records as this {@link KTable};
     *         the records are no longer treated as updates on a primary-keyed table,
     *         but rather as normal key-value pairs in a record stream
     */
    KStream<K, V> toStream();

    /**
     *  Convert this stream to a new instance of {@link KStream} using the given {@link KeyValueMapper} to select
     *  the new key.
     *
     * @param mapper  @param mapper  the instance of {@link KeyValueMapper}
     * @param <K1> the new key type
     *
     * @return a {@link KStream} that contains the transformed records from this {@link KTable};
     *         the records are no longer treated as updates on a primary-keyed table,
     *         but rather as normal key-value pairs in a record stream
     */
    <K1> KStream<K1, V> toStream(KeyValueMapper<K, V, K1> mapper);

    /**
     * Combine values of this stream with another {@link KTable} stream's elements of the same key using Inner Join.
     *
     * @param other         the instance of {@link KTable} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     *
     * @return a {@link KTable} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key
     */
    <V1, R> KTable<K, R> join(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combine values of this stream with another {@link KTable} stream's elements of the same key using Outer Join.
     *
     * @param other         the instance of {@link KTable} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     *
     * @return a {@link KTable} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key
     */
    <V1, R> KTable<K, R> outerJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Combine values of this stream with another {@link KTable} stream's elements of the same key using Left Join.
     *
     * @param other         the instance of {@link KTable} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     *
     * @return a {@link KTable} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key
     */
    <V1, R> KTable<K, R> leftJoin(KTable<K, V1> other, ValueJoiner<V, V1, R> joiner);

    /**
     * Group the records of this {@link KTable} using the provided {@link KeyValueMapper}.
     * 
     * @param selector      select the grouping key and value to be aggregated
     * @param keySerde      key serdes for materializing this stream,
     *                      if not specified the default serdes defined in the configs will be used
     * @param valueSerde    value serdes for materializing this stream,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <K1>          the key type of the {@link KGroupedTable}
     * @param <V1>          the value type of the {@link KGroupedTable}
     *
     * @return a {@link KGroupedTable} that contains the re-partitioned records of this {@link KTable}
     */
    <K1, V1> KGroupedTable<K1, V1> groupBy(KeyValueMapper<K, V, KeyValue<K1, V1>> selector, Serde<K1> keySerde, Serde<V1> valueSerde);

    /**
     * Group the records of this {@link KTable} using the provided {@link KeyValueMapper} and default serializers and deserializers.
     *
     * @param selector      select the grouping key and value to be aggregated
     * @param <K1>          the key type of the {@link KGroupedTable}
     * @param <V1>          the value type of the {@link KGroupedTable}
     *
     * @return a {@link KGroupedTable} that contains the re-partitioned records of this {@link KTable}
     */
    <K1, V1> KGroupedTable<K1, V1> groupBy(KeyValueMapper<K, V, KeyValue<K1, V1>> selector);

    /**
     * Perform an action on each element of {@link KTable}.
     * Note that this is a terminal operation that returns void.
     *
     * @param action an action to perform on each element
     */
    void foreach(ForeachAction<K, V> action);

    /**
     * Get the name of the local state store used for materializing this {@link KTable}
     * @return the underlying state store name, or {@code null} if KTable does not have one
     */
    String getStoreName();
}
