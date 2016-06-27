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
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * {@link KStream} is an abstraction of a <i>record stream</i> of key-value pairs.
 * <p>
 * A {@link KStream} is either defined from one or multiple Kafka topics that are consumed message by message or
 * the result of a {@link KStream} transformation. A {@link KTable} can also be converted into a {@link KStream}.
 * <p>
 * A {@link KStream} can be transformed record by record, joined with another {@link KStream} or {@link KTable}, or
 * can be aggregated into a {@link KTable}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 *
 * @see KTable
 */
@InterfaceStability.Unstable
public interface KStream<K, V> {

    /**
     * Create a new instance of {@link KStream} that consists of all elements of this stream which satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     *
     * @return a {@link KStream} that contains only those records that satisfy the given predicate
     */
    KStream<K, V> filter(Predicate<K, V> predicate);

    /**
     * Create a new instance of {@link KStream} that consists all elements of this stream which do not satisfy a predicate.
     *
     * @param predicate     the instance of {@link Predicate}
     *
     * @return a {@link KStream} that contains only those records that do not satisfy the given predicate
     */
    KStream<K, V> filterNot(Predicate<K, V> predicate);


    /**
     * Create a new key from the current key and value.
     *
     * @param mapper  the instance of {@link KeyValueMapper}
     * @param <K1>   the new key type on the stream
     *
     * @return a {@link KStream} that contains records with different key type and same value type
     */
    <K1> KStream<K1, V> selectKey(KeyValueMapper<K, V, K1> mapper);

    /**
     * Create a new instance of {@link KStream} by transforming each element in this stream into a different element in the new stream.
     *
     * @param mapper        the instance of {@link KeyValueMapper}
     * @param <K1>          the key type of the new stream
     * @param <V1>          the value type of the new stream
     *
     * @return a {@link KStream} that contains records with new key and value type
     */
    <K1, V1> KStream<K1, V1> map(KeyValueMapper<K, V, KeyValue<K1, V1>> mapper);

    /**
     * Create a new instance of {@link KStream} by transforming the value of each element in this stream into a new value in the new stream.
     *
     * @param mapper        the instance of {@link ValueMapper}
     * @param <V1>          the value type of the new stream
     *
     * @return a {@link KStream} that contains records with unmodified keys and new values of different type
     */
    <V1> KStream<K, V1> mapValues(ValueMapper<V, V1> mapper);

    /**
     * Print the elements of this stream to System.out
     *
     * Implementors will need to override toString for keys and values that are not of
     * type String, Integer etc to get meaningful information.
     */
    void print();


    /**
     * Print the elements of this stream to System.out
     *
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     *
     *                 Implementors will need to override toString for keys and values that are not of
     *                 type String, Integer etc to get meaningful information.
     */
    void print(Serde<K> keySerde, Serde<V> valSerde);


    /**
     * Write the elements of this stream to a file at the given path.
     *
     * @param filePath name of file to write to
     *
     *                 Implementors will need to override toString for keys and values that are not of
     *                 type String, Integer etc to get meaningful information.
     */
    void writeAsText(String filePath);

    /**
     * @param filePath name of file to write to
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     *
     *                 Implementors will need to override toString for keys and values that are not of
     *                 type String, Integer etc to get meaningful information.
     */

    void writeAsText(String filePath, Serde<K> keySerde, Serde<V> valSerde);

    /**
     * Create a new instance of {@link KStream} by transforming each element in this stream into zero or more elements in the new stream.
     *
     * @param mapper        the instance of {@link KeyValueMapper}
     * @param <K1>          the key type of the new stream
     * @param <V1>          the value type of the new stream
     *
     * @return a {@link KStream} that contains more or less records with new key and value type
     */
    <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K, V, Iterable<KeyValue<K1, V1>>> mapper);

    /**
     * Create a new instance of {@link KStream} by transforming the value of each element in this stream into zero or more values with the same key in the new stream.
     *
     * @param processor     the instance of {@link ValueMapper}
     * @param <V1>          the value type of the new stream
     *
     * @return a {@link KStream} that contains more or less records with unmodified keys and new values of different type
     */
    <V1> KStream<K, V1> flatMapValues(ValueMapper<V, Iterable<V1>> processor);

    /**
     * Creates an array of {@link KStream} from this stream by branching the elements in the original stream based on the supplied predicates.
     * Each element is evaluated against the supplied predicates, and predicates are evaluated in order. Each stream in the result array
     * corresponds position-wise (index) to the predicate in the supplied predicates. The branching happens on first-match: An element
     * in the original stream is assigned to the corresponding result stream for the first predicate that evaluates to true, and
     * assigned to this stream only. An element will be dropped if none of the predicates evaluate to true.
     *
     * @param predicates    the ordered list of {@link Predicate} instances
     *
     * @return multiple distinct substreams of this {@link KStream}
     */
    KStream<K, V>[] branch(Predicate<K, V>... predicates);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KStream} from the topic
     * using default serializers and deserializers and producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
     * This is equivalent to calling {@link #to(String)} and {@link org.apache.kafka.streams.kstream.KStreamBuilder#stream(String...)}.
     *
     * @param topic     the topic name
     *
     * @return a {@link KStream} that contains the exact same records as this {@link KStream}
     */
    KStream<K, V> through(String topic);

    /**
     * Perform an action on each element of {@link KStream}.
     * Note that this is a terminal operation that returns void.
     *
     * @param action an action to perform on each element
     */
    void foreach(ForeachAction<K, V> action);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KStream} from the topic
     * using default serializers and deserializers and a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     * This is equivalent to calling {@link #to(StreamPartitioner, String)} and {@link org.apache.kafka.streams.kstream.KStreamBuilder#stream(String...)}.
     *
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner} will be used
     * @param topic        the topic name
     *
     * @return a {@link KStream} that contains the exact same records as this {@link KStream}
     */
    KStream<K, V> through(StreamPartitioner<K, V> partitioner, String topic);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KStream} from the topic.
     * If {@code keySerde} provides a {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer}
     * for the key {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} is used
     * &mdash; otherwise producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner} is used.
     * This is equivalent to calling {@link #to(Serde, Serde, String)} and
     * {@link org.apache.kafka.streams.kstream.KStreamBuilder#stream(Serde, Serde, String...)}.
     *
     * @param keySerde     key serde used to send key-value pairs,
     *                     if not specified the default key serde defined in the configuration will be used
     * @param valSerde     value serde used to send key-value pairs,
     *                     if not specified the default value serde defined in the configuration will be used
     * @param topic        the topic name
     *
     * @return a {@link KStream} that contains the exact same records as this {@link KStream}
     */
    KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, String topic);

    /**
     * Materialize this stream to a topic, also creates a new instance of {@link KStream} from the topic
     * using a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     * This is equivalent to calling {@link #to(Serde, Serde, StreamPartitioner, String)} and
     * {@link org.apache.kafka.streams.kstream.KStreamBuilder#stream(Serde, Serde, String...)}.
     *
     * @param keySerde     key serde used to send key-value pairs,
     *                     if not specified the default key serde defined in the configuration will be used
     * @param valSerde     value serde used to send key-value pairs,
     *                     if not specified the default value serde defined in the configuration will be used
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified and {@code keySerde} provides a {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer} for the key
     *                     {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} will be used
     *                     &mdash; otherwise {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner} will be used
     * @param topic        the topic name
     *
     * @return a {@link KStream} that contains the exact same records as this {@link KStream}
     */
    KStream<K, V> through(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config
     * and producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner}.
     *
     * @param topic     the topic name
     */
    void to(String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config and a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     *
     * @param partitioner  the function used to determine how records are distributed among partitions of the topic,
     *                     if not specified producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner} will be used
     * @param topic        the topic name
     */
    void to(StreamPartitioner<K, V> partitioner, String topic);

    /**
     * Materialize this stream to a topic. If {@code keySerde} provides a
     * {@link org.apache.kafka.streams.kstream.internals.WindowedSerializer} for the key
     * {@link org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner} is used
     * &mdash; otherwise producer's {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner} is used.
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
     *                     &mdash; otherwise {@link org.apache.kafka.clients.producer.internals.DefaultPartitioner} will be used
     * @param topic        the topic name
     */
    void to(Serde<K> keySerde, Serde<V> valSerde, StreamPartitioner<K, V> partitioner, String topic);

    /**
     * Create a new {@link KStream} instance by applying a {@link org.apache.kafka.streams.kstream.Transformer} to all elements in this stream, one element at a time.
     *
     * @param transformerSupplier   the instance of {@link TransformerSupplier} that generates {@link org.apache.kafka.streams.kstream.Transformer}
     * @param stateStoreNames       the names of the state store used by the processor
     *
     * @return a new {@link KStream} with transformed key and value types
     */
    <K1, V1> KStream<K1, V1> transform(TransformerSupplier<K, V, KeyValue<K1, V1>> transformerSupplier, String... stateStoreNames);

    /**
     * Create a new {@link KStream} instance by applying a {@link org.apache.kafka.streams.kstream.ValueTransformer} to all values in this stream, one element at a time.
     *
     * @param valueTransformerSupplier  the instance of {@link ValueTransformerSupplier} that generates {@link org.apache.kafka.streams.kstream.ValueTransformer}
     * @param stateStoreNames           the names of the state store used by the processor
     *
     * @return a {@link KStream} that contains records with unmodified keys and transformed values with type {@code R}
     */
    <R> KStream<K, R> transformValues(ValueTransformerSupplier<V, R> valueTransformerSupplier, String... stateStoreNames);

    /**
     * Process all elements in this stream, one element at a time, by applying a {@link org.apache.kafka.streams.processor.Processor}.
     *
     * @param processorSupplier         the supplier of {@link ProcessorSupplier} that generates {@link org.apache.kafka.streams.processor.Processor}
     * @param stateStoreNames           the names of the state store used by the processor
     */
    void process(ProcessorSupplier<K, V> processorSupplier, String... stateStoreNames);

    /**
     * Combine element values of this stream with another {@link KStream}'s elements of the same key using windowed Inner Join.
     *
     * @param otherStream       the instance of {@link KStream} joined with this stream
     * @param joiner            the instance of {@link ValueJoiner}
     * @param windows           the specification of the {@link JoinWindows}
     * @param keySerde          key serdes for materializing both streams,
     *                          if not specified the default serdes defined in the configs will be used
     * @param thisValueSerde    value serdes for materializing this stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde   value serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param <V1>              the value type of the other stream
     * @param <R>               the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, R> KStream<K, R> join(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde);

    /**
     * Combine element values of this stream with another {@link KStream}'s elements of the same key using windowed Inner Join
     * with default serializers and deserializers.
     *
     * @param otherStream   the instance of {@link KStream} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param windows       the specification of the {@link JoinWindows}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, R> KStream<K, R> join(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows);

    /**
     * Combine values of this stream with another {@link KStream}'s elements of the same key using windowed Outer Join.
     *
     * @param otherStream       the instance of {@link KStream} joined with this stream
     * @param joiner            the instance of {@link ValueJoiner}
     * @param windows           the specification of the {@link JoinWindows}
     * @param keySerde          key serdes for materializing both streams,
     *                          if not specified the default serdes defined in the configs will be used
     * @param thisValueSerde    value serdes for materializing this stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde   value serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param <V1>              the value type of the other stream
     * @param <R>               the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V> thisValueSerde,
            Serde<V1> otherValueSerde);

    /**
     * Combine values of this stream with another {@link KStream}'s elements of the same key using windowed Outer Join
     * with default serializers and deserializers.
     *
     * @param otherStream   the instance of {@link KStream} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param windows       the specification of the {@link JoinWindows}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, R> KStream<K, R> outerJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows);

    /**
     * Combine values of this stream with another {@link KStream}'s elements of the same key using windowed Left Join.
     *
     * @param otherStream       the instance of {@link KStream} joined with this stream
     * @param joiner            the instance of {@link ValueJoiner}
     * @param windows           the specification of the {@link JoinWindows}
     * @param keySerde          key serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde   value serdes for materializing the other stream,
     *                          if not specified the default serdes defined in the configs will be used
     * @param <V1>              the value type of the other stream
     * @param <R>               the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows,
            Serde<K> keySerde,
            Serde<V1> otherValueSerde);

    /**
     * Combine values of this stream with another {@link KStream}'s elements of the same key using windowed Left Join
     * with default serializers and deserializers.
     *
     * @param otherStream   the instance of {@link KStream} joined with this stream
     * @param joiner        the instance of {@link ValueJoiner}
     * @param windows       the specification of the {@link JoinWindows}
     * @param <V1>          the value type of the other stream
     * @param <R>           the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, R> KStream<K, R> leftJoin(
            KStream<K, V1> otherStream,
            ValueJoiner<V, V1, R> joiner,
            JoinWindows windows);

    /**
     * Combine values of this stream with {@link KTable}'s elements of the same key using non-windowed Left Join.
     *
     * @param table     the instance of {@link KTable} joined with this stream
     * @param joiner    the instance of {@link ValueJoiner}
     * @param <V1>      the value type of the table
     * @param <V2>      the value type of the new stream
     *
     * @return a {@link KStream} that contains join-records for each key and values computed by the given {@link ValueJoiner},
     *         one for each matched record-pair with the same key and within the joining window intervals
     */
    <V1, V2> KStream<K, V2> leftJoin(KTable<K, V1> table, ValueJoiner<V, V1, V2> joiner);

    /**
     * Combine values of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param reducer           the instance of {@link Reducer}
     * @param windows           the specification of the aggregation {@link Windows}
     * @param keySerde          key serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param valueSerde        value serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer,
                                                          Windows<W> windows,
                                                          Serde<K> keySerde,
                                                          Serde<V> valueSerde);

    /**
     * Combine values of this stream by key on a window basis into a new instance of windowed {@link KTable}
     * with default serializers and deserializers.
     *
     * @param reducer the instance of {@link Reducer}
     * @param windows the specification of the aggregation {@link Windows}
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, V> reduceByKey(Reducer<V> reducer, Windows<W> windows);

    /**
     * Combine values of this stream by key into a new instance of ever-updating {@link KTable}.
     *
     * @param reducer           the instance of {@link Reducer}
     * @param keySerde          key serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param valueSerde        value serdes for materializing the aggregated table,
     *                          if not specified the default serdes defined in the configs will be used
     * @param name              the name of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) aggregate for each key
     */
    KTable<K, V> reduceByKey(Reducer<V> reducer,
                             Serde<K> keySerde,
                             Serde<V> valueSerde,
                             String name);

    /**
     * Combine values of this stream by key into a new instance of ever-updating {@link KTable} with default serializers and deserializers.
     *
     * @param reducer the instance of {@link Reducer}
     * @param name    the name of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) aggregate for each key
     */
    KTable<K, V> reduceByKey(Reducer<V> reducer, String name);

    /**
     * Aggregate values of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param windows       the specification of the aggregation {@link Windows}
     * @param keySerde      key serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param <T>           the value type of the resulted {@link KTable}
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values with type {@code T}
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
                                                                Aggregator<K, V, T> aggregator,
                                                                Windows<W> windows,
                                                                Serde<K> keySerde,
                                                                Serde<T> aggValueSerde);

    /**
     * Aggregate values of this stream by key on a window basis into a new instance of windowed {@link KTable}
     * with default serializers and deserializers.
     *
     * @param initializer   the instance of {@link Initializer}
     * @param aggregator    the instance of {@link Aggregator}
     * @param windows       the specification of the aggregation {@link Windows}
     * @param <T>           the value type of the resulted {@link KTable}
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values with type {@code T}
     *         that represent the latest (rolling) aggregate for each key within that window
     */
    <T, W extends Window> KTable<Windowed<K>, T> aggregateByKey(Initializer<T> initializer,
                                                                Aggregator<K, V, T> aggregator,
                                                                Windows<W> windows);

    /**
     * Aggregate values of this stream by key into a new instance of ever-updating {@link KTable}.
     *
     * @param initializer   the class of {@link Initializer}
     * @param aggregator    the class of {@link Aggregator}
     * @param keySerde      key serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param aggValueSerde aggregate value serdes for materializing the aggregated table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param name          the name of the resulted {@link KTable}
     * @param <T>           the value type of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values (of different type) that represent the latest (rolling) aggregate for each key
     */
    <T> KTable<K, T> aggregateByKey(Initializer<T> initializer,
                                    Aggregator<K, V, T> aggregator,
                                    Serde<K> keySerde,
                                    Serde<T> aggValueSerde,
                                    String name);

    /**
     * Aggregate values of this stream by key into a new instance of ever-updating {@link KTable}
     * with default serializers and deserializers.
     *
     * @param initializer   the class of {@link Initializer}
     * @param aggregator    the class of {@link Aggregator}
     * @param name          the name of the resulted {@link KTable}
     * @param <T>           the value type of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values (of different type) that represent the latest (rolling) aggregate for each key
     */
    <T> KTable<K, T> aggregateByKey(Initializer<T> initializer,
                                    Aggregator<K, V, T> aggregator,
                                    String name);

    /**
     * Count number of records of this stream by key on a window basis into a new instance of windowed {@link KTable}.
     *
     * @param windows       the specification of the aggregation {@link Windows}
     * @param keySerde      key serdes for materializing the counting table,
     *                      if not specified the default serdes defined in the configs will be used
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) count (i.e., number of records) for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows, Serde<K> keySerde);

    /**
     * Count number of records of this stream by key on a window basis into a new instance of windowed {@link KTable}
     * with default serializers and deserializers.
     *
     * @param windows       the specification of the aggregation {@link Windows}
     *
     * @return a windowed {@link KTable} which can be treated as a list of {@code KTable}s
     *         where each table contains records with unmodified keys and values
     *         that represent the latest (rolling) count (i.e., number of records) for each key within that window
     */
    <W extends Window> KTable<Windowed<K>, Long> countByKey(Windows<W> windows);

    /**
     * Count number of records of this stream by key into a new instance of ever-updating {@link KTable}.
     *
     * @param keySerde      key serdes for materializing the counting table,
     *                      if not specified the default serdes defined in the configs will be used
     * @param name          the name of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> countByKey(Serde<K> keySerde, String name);

    /**
     * Count number of records of this stream by key into a new instance of ever-updating {@link KTable}
     * with default serializers and deserializers.
     *
     * @param name          the name of the resulted {@link KTable}
     *
     * @return a {@link KTable} that contains records with unmodified keys and values that represent the latest (rolling) count (i.e., number of records) for each key
     */
    KTable<K, Long> countByKey(String name);

}
