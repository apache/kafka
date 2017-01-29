/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.kstream.internals.WindowedStreamPartitioner;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * {@code KStream} is an abstraction of a <i>record stream</i> of {@link KeyValue} pairs, i.e., each record is an
 * independent entity/event in the real world.
 * For example a user X might buy two items I1 and I2, and thus there might be two records {@code <K:I1>, <K:I2>}
 * in the stream.
 * <p>
 * A {@code KStream} is either {@link KStreamBuilder#stream(String...) defined from one or multiple Kafka topics} that
 * are consumed message by message or the result of a {@code KStream} transformation.
 * A {@link KTable} can also be {@link KTable#toStream() converted} into a {@code KStream}.
 * <p>
 * A {@code KStream} can be transformed record by record, joined with another {@code KStream}, {@link KTable},
 * {@link GlobalKTable}, or can be aggregated into a {@link KTable}.
 * Kafka Streams DSL can be mixed-and-matched with Processor API (PAPI) (c.f. {@link TopologyBuilder}) via
 * {@link #process(ProcessorSupplier, String...) process(...)},
 * {@link #transform(TransformerSupplier, String...) transform(...)}, and
 * {@link #transformValues(ValueTransformerSupplier, String...) transformValues(...)}.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 * @see KTable
 * @see KGroupedStream
 * @see KStreamBuilder#stream(String...)
 */
@SuppressWarnings("unused")
@InterfaceStability.Unstable
public interface KStream<K, V> {

    /**
     * Create a new {@code KStream} that consists of all records of this stream which satisfy the given predicate.
     * All records that do not satisfy the predicate are dropped.
     * This is a stateless record-by-record operation.
     *
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @return a {@code KStream} that contains only those records that satisfy the given predicate
     * @see #filterNot(Predicate)
     */
    KStream<K, V> filter(Predicate<? super K, ? super V> predicate);

    /**
     * Create a new {@code KStream} that consists all records of this stream which do <em>not</em> satisfy the given
     * predicate.
     * All records that <em>do</em> satisfy the predicate are dropped.
     * This is a stateless record-by-record operation.
     *
     * @param predicate a filter {@link Predicate} that is applied to each record
     * @return a {@code KStream} that contains only those records that do <em>not</em> satisfy the given predicate
     * @see #filter(Predicate)
     */
    KStream<K, V> filterNot(Predicate<? super K, ? super V> predicate);

    /**
     * Set a new key (with possibly new type) for each input record.
     * The provided {@link KeyValueMapper} is applied to each input record and computes a new key for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V>}.
     * This is a stateless record-by-record operation.
     * <p>
     * For example, you can use this transformation to set a key for a key-less input record {@code <null,V>} by
     * extracting a key from the value within your {@link KeyValueMapper}. The example below computes the new key as the
     * length of the value string.
     * <pre>{@code
     * KStream<Byte[], String> keyLessStream = builder.stream("key-less-topic");
     * KStream<Integer, String> keyedStream = keyLessStream.selectKey(new KeyValueMapper<Byte[], String, Integer> {
     *     Integer apply(Byte[] key, String value) {
     *         return value.length();
     *     }
     * });
     * }</pre>
     * <p>
     * Setting a new key might result in an internal data redistribution if a key based operator (like an aggregation or
     * join) is applied to the result {@code KStream}.
     *
     * @param mapper a {@link KeyValueMapper} that computes a new key for each record
     * @param <KR>   the new key type of the result stream
     * @return a {@code KStream} that contains records with new key (possibly of different type) and unmodified value
     * @see #map(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #flatMapValues(ValueMapper)
     */
    <KR> KStream<KR, V> selectKey(KeyValueMapper<? super K, ? super V, ? extends KR> mapper);

    /**
     * Transform each record of the input stream into a new record in the output stream (both key and value type can be
     * altered arbitrarily).
     * The provided {@link KeyValueMapper} is applied to each input record and computes a new output record.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K':V'>}.
     * This is a stateless record-by-record operation (cf. {@link #transform(TransformerSupplier, String...)} for
     * stateful record transformation).
     * <p>
     * The example below normalizes the String key to upper-case letters and counts the number of token of the value string.
     * <pre>{@code
     * KStream<String, String> inputStream = builder.stream("topic");
     * KStream<Integer, String> outputStream = inputStream.map(new KeyValueMapper<String, String, KeyValue<String, Integer>> {
     *     KeyValue<String, Integer> apply(String key, String value) {
     *         return new KeyValue<>(key.toUpperCase(), value.split(" ").length);
     *     }
     * });
     * }</pre>
     * <p>
     * The provided {@link KeyValueMapper} must return a {@link KeyValue} type and must not return {@code null}.
     * <p>
     * Mapping records might result in an internal data redistribution if a key based operator (like an aggregation or
     * join) is applied to the result {@code KStream}. (cf. {@link #mapValues(ValueMapper)})
     *
     * @param mapper a {@link KeyValueMapper} that computes a new output record
     * @param <KR>   the key type of the result stream
     * @param <VR>   the value type of the result stream
     * @return a {@code KStream} that contains records with new key and value (possibly both of different type)
     * @see #selectKey(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #flatMapValues(ValueMapper)
     * @see #transform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     */
    <KR, VR> KStream<KR, VR> map(KeyValueMapper<? super K, ? super V, ? extends KeyValue<? extends KR, ? extends VR>> mapper);

    /**
     * Transform the value of each input record into a new value (with possible new type) of the output record.
     * The provided {@link ValueMapper} is applied to each input record value and computes a new value for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * This is a stateless record-by-record operation (cf.
     * {@link #transformValues(ValueTransformerSupplier, String...)} for stateful value transformation).
     * <p>
     * The example below counts the number of token of the value string.
     * <pre>{@code
     * KStream<String, String> inputStream = builder.stream("topic");
     * KStream<String, Integer> outputStream = inputStream.mapValues(new ValueMapper<String, Integer> {
     *     Integer apply(String value) {
     *         return value.split(" ").length;
     *     }
     * });
     * }</pre>
     * <p>
     * Setting a new value preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like an aggregation or join)
     * is applied to the result {@code KStream}. (cf. {@link #map(KeyValueMapper)})
     *
     * @param mapper a {@link ValueMapper} that computes a new output value
     * @param <VR>   the value type of the result stream
     * @return a {@code KStream} that contains records with unmodified key and new values (possibly of different type)
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #flatMapValues(ValueMapper)
     * @see #transform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     */
    <VR> KStream<K, VR> mapValues(ValueMapper<? super V, ? extends VR> mapper);

    /**
     * Transform each record of the input stream into zero or more records in the output stream (both key and value type
     * can be altered arbitrarily).
     * The provided {@link KeyValueMapper} is applied to each input record and computes zero or more output records.
     * Thus, an input record {@code <K,V>} can be transformed into output records {@code <K':V'>, <K'':V''>, ...}.
     * This is a stateless record-by-record operation (cf. {@link #transform(TransformerSupplier, String...)} for
     * stateful record transformation).
     * <p>
     * The example below splits input records {@code <null:String>} containing sentences as values into their words
     * and emit a record {@code <word:1>} for each word.
     * <pre>{@code
     * KStream<byte[], String> inputStream = builder.stream("topic");
     * KStream<Integer, String> outputStream = inputStream.flatMap(new KeyValueMapper<byte[], String, Iterable<KeyValue<String, Integer>>> {
     *     Iterable<KeyValue<String, Integer>> apply(byte[] key, String value) {
     *         String[] tokens = value.split(" ");
     *         List<KeyValue<String, Integer>> result = new ArrayList<>(tokens.length);
     *
     *         for(String token : tokens) {
     *             result.add(new KeyValue<>(token, 1));
     *         }
     *
     *         return result;
     *     }
     * });
     * }</pre>
     * <p>
     * The provided {@link KeyValueMapper} must return an {@link Iterable} (e.g., any {@link java.util.Collection} type)
     * and the return value must not be {@code null}.
     * <p>
     * Flat-mapping records might result in an internal data redistribution if a key based operator (like an aggregation
     * or join) is applied to the result {@code KStream}. (cf. {@link #flatMapValues(ValueMapper)})
     *
     * @param mapper a {@link KeyValueMapper} that computes the new output records
     * @param <KR>   the key type of the result stream
     * @param <VR>   the value type of the result stream
     * @return a {@code KStream} that contains more or less records with new key and value (possibly of different type)
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #flatMapValues(ValueMapper)
     * @see #transform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     */
    <KR, VR> KStream<KR, VR> flatMap(final KeyValueMapper<? super K, ? super V, ? extends Iterable<? extends KeyValue<? extends KR, ? extends VR>>> mapper);

    /**
     * Create a new {@code KStream} by transforming the value of each record in this stream into zero or more values
     * with the same key in the new stream.
     * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
     * stream (value type can be altered arbitrarily).
     * The provided {@link ValueMapper} is applied to each input record and computes zero or more output values.
     * Thus, an input record {@code <K,V>} can be transformed into output records {@code <K:V'>, <K:V''>, ...}.
     * This is a stateless record-by-record operation (cf. {@link #transformValues(ValueTransformerSupplier, String...)}
     * for stateful value transformation).
     * <p>
     * The example below splits input records {@code <null:String>} containing sentences as values into their words.
     * <pre>{@code
     * KStream<byte[], String> inputStream = builder.stream("topic");
     * KStream<Integer, String> outputStream = inputStream.flatMap(new ValueMapper<String, Iterable<String>> {
     *     Iterable<String> apply(String value) {
     *         return Arrays.asList(value.split(" "));
     *     }
     * });
     * }</pre>
     * <p>
     * The provided {@link ValueMapper} must return an {@link Iterable} (e.g., any {@link java.util.Collection} type)
     * and the return value must not be {@code null}.
     * <p>
     * Splitting a record into multiple records with the same key preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like an aggregation or join)
     * is applied to the result {@code KStream}. (cf. {@link #flatMap(KeyValueMapper)})
     *
     * @param processor a {@link ValueMapper} the computes the new output values
     * @param <VR>      the value type of the result stream
     * @return a {@code KStream} that contains more or less records with unmodified keys and new values of different type
     * @see #selectKey(KeyValueMapper)
     * @see #map(KeyValueMapper)
     * @see #flatMap(KeyValueMapper)
     * @see #mapValues(ValueMapper)
     * @see #transform(TransformerSupplier, String...)
     * @see #transformValues(ValueTransformerSupplier, String...)
     */
    <VR> KStream<K, VR> flatMapValues(final ValueMapper<? super V, ? extends Iterable<? extends VR>> processor);

    /**
     * Print the records of this stream to {@code System.out}.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the console.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     */
    void print();

    /**
     * Print the records of this stream to {@code System.out}.
     * This function will use the given name to label the key/value pairs printed to the console.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param streamName the name used to label the key/value pairs printed to the console
     */
    void print(final String streamName);

    /**
     * Print the records of this stream to {@code System.out}.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the console.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param keySerde key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde value serde used to deserialize value if type is {@code byte[]},
     */
    void print(final Serde<K> keySerde,
               final Serde<V> valSerde);

    /**
     * Print the records of this stream to {@code System.out}.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param keySerde   key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde   value serde used to deserialize value if type is {@code byte[]},
     * @param streamName the name used to label the key/value pairs printed to the console
     */
    void print(final Serde<K> keySerde,
               final Serde<V> valSerde,
               final String streamName);

    /**
     * Write the records of this stream to a file at the given path.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the file.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param filePath name of the file to write to
     */
    void writeAsText(final String filePath);

    /**
     * Write the records of this stream to a file at the given path.
     * This function will use the given name to label the key/value printed to the file.
     * <p>
     * The default serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param filePath   name of the file to write to
     * @param streamName the name used to label the key/value pairs written to the file
     */
    void writeAsText(final String filePath,
                     final String streamName);

    /**
     * Write the records of this stream to a file at the given path.
     * This function will use the generated name of the parent processor node to label the key/value pairs printed to
     * the file.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]} before calling
     * {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param filePath name of the file to write to
     * @param keySerde key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde value serde used to deserialize value if type is {@code byte[]},
     */
    void writeAsText(final String filePath,
                     final Serde<K> keySerde,
                     final Serde<V> valSerde);

    /**
     * Write the records of this stream to a file at the given path.
     * This function will use the given name to label the key/value printed to the file.
     * <p>
     * The provided serde will be used to deserialize the key or value in case the type is {@code byte[]}
     * before calling {@code toString()} on the deserialized object.
     * <p>
     * Implementors will need to override {@code toString()} for keys and values that are not of type {@link String},
     * {@link Integer} etc. to get meaningful information.
     *
     * @param filePath   name of the file to write to
     * @param streamName the name used to label the key/value pairs written to the file
     * @param keySerde   key serde used to deserialize key if type is {@code byte[]},
     * @param valSerde   value serde used deserialize value if type is {@code byte[]},
     */
    void writeAsText(final String filePath,
                     final String streamName,
                     final Serde<K> keySerde,
                     final Serde<V> valSerde);

    /**
     * Perform an action on each record of {@code KStream}.
     * This is a stateless record-by-record operation (cf. {@link #process(ProcessorSupplier, String...)}).
     * Note that this is a terminal operation that returns void.
     *
     * @param action an action to perform on each record
     * @see #process(ProcessorSupplier, String...)
     */
    void foreach(final ForeachAction<? super K, ? super V> action);

    /**
     * Creates an array of {@code KStream} from this stream by branching the records in the original stream based on
     * the supplied predicates.
     * Each record is evaluated against the supplied predicates, and predicates are evaluated in order.
     * Each stream in the result array corresponds position-wise (index) to the predicate in the supplied predicates.
     * The branching happens on first-match: A record in the original stream is assigned to the corresponding result
     * stream for the first predicate that evaluates to true, and is assigned to this stream only.
     * A record will be dropped if none of the predicates evaluate to true.
     * This is a stateless record-by-record operation.
     *
     * @param predicates the ordered list of {@link Predicate} instances
     * @return multiple distinct substreams of this {@code KStream}
     */
    @SuppressWarnings("unchecked")
    KStream<K, V>[] branch(final Predicate<? super K, ? super V>... predicates);

    /**
     * Materialize this stream to a topic and creates a new {@code KStream} from the topic using default serializers and
     * deserializers and producer's {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(String) #to(someTopicName)} and {@link KStreamBuilder#stream(String...)
     * KStreamBuilder#stream(someTopicName)}.
     *
     * @param topic the topic name
     * @return a {@code KStream} that contains the exact same (and potentially repartitioned) records as this {@code KStream}
     */
    KStream<K, V> through(final String topic);

    /**
     * Materialize this stream to a topic and creates a new {@code KStream} from the topic using default serializers and
     * deserializers and a customizable {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(StreamPartitioner, String) #to(StreamPartitioner, someTopicName)} and
     * {@link KStreamBuilder#stream(String...) KStreamBuilder#stream(someTopicName)}.
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic       the topic name
     * @return a {@code KStream} that contains the exact same (and potentially repartitioned) records as this {@code KStream}
     */
    KStream<K, V> through(final StreamPartitioner<? super K, ? super V> partitioner,
                          final String topic);

    /**
     * Materialize this stream to a topic, and creates a new {@code KStream} from the topic.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * If {@code keySerde} provides a {@link WindowedSerializer} for the key {@link WindowedStreamPartitioner} is
     * used&mdash;otherwise producer's {@link DefaultPartitioner} is used.
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, String) #to(keySerde, valSerde, someTopicName)} and
     * {@link KStreamBuilder#stream(Serde, Serde, String...) KStreamBuilder#stream(keySerde, valSerde, someTopicName)}.
     *
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default key serde defined in the configuration will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default value serde defined in the configuration will be used
     * @param topic    the topic name
     * @return a {@code KStream} that contains the exact same (and potentially repartitioned) records as this {@code KStream}
     */
    KStream<K, V> through(final Serde<K> keySerde,
                          final Serde<V> valSerde,
                          final String topic);

    /**
     * Materialize this stream to a topic and creates a new {@code KStream} from the topic using a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     * <p>
     * This is equivalent to calling {@link #to(Serde, Serde, StreamPartitioner, String) #to(keySerde, valSerde,
     * StreamPartitioner, someTopicName)} and {@link KStreamBuilder#stream(Serde, Serde, String...)
     * KStreamBuilder#stream(keySerde, valSerde, someTopicName)}.
     *
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default key serde defined in the configuration will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default value serde defined in the configuration will be used
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and {@code keySerde} provides a {@link WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will
     *                    be used
     * @param topic       the topic name
     * @return a {@code KStream} that contains the exact same (and potentially repartitioned) records as this {@code KStream}
     */
    KStream<K, V> through(final Serde<K> keySerde,
                          final Serde<V> valSerde,
                          final StreamPartitioner<? super K, ? super V> partitioner,
                          final String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config and producer's
     * {@link DefaultPartitioner}.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param topic the topic name
     */
    void to(final String topic);

    /**
     * Materialize this stream to a topic using default serializers specified in the config and a customizable
     * {@link StreamPartitioner} to determine the distribution of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified producer's {@link DefaultPartitioner} will be used
     * @param topic       the topic name
     */
    void to(final StreamPartitioner<? super K, ? super V> partitioner,
            final String topic);

    /**
     * Materialize this stream to a topic. If {@code keySerde} provides a {@link WindowedSerializer WindowedSerializer}
     * for the key {@link WindowedStreamPartitioner} is used&mdash;otherwise producer's {@link DefaultPartitioner} is
     * used.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param keySerde key serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param valSerde value serde used to send key-value pairs,
     *                 if not specified the default serde defined in the configs will be used
     * @param topic    the topic name
     */
    void to(final Serde<K> keySerde,
            final Serde<V> valSerde,
            final String topic);

    /**
     * Materialize this stream to a topic using a customizable {@link StreamPartitioner} to determine the distribution
     * of records to partitions.
     * The specified topic should be manually created before it is used (i.e., before the Kafka Streams application is
     * started).
     *
     * @param keySerde    key serde used to send key-value pairs,
     *                    if not specified the default serde defined in the configs will be used
     * @param valSerde    value serde used to send key-value pairs,
     *                    if not specified the default serde defined in the configs will be used
     * @param partitioner the function used to determine how records are distributed among partitions of the topic,
     *                    if not specified and {@code keySerde} provides a {@link  WindowedSerializer} for the key
     *                    {@link WindowedStreamPartitioner} will be used&mdash;otherwise {@link DefaultPartitioner} will
     *                    be used
     * @param topic       the topic name
     */
    void to(final Serde<K> keySerde,
            final Serde<V> valSerde,
            final StreamPartitioner<? super K, ? super V> partitioner,
            final String topic);

    /**
     * Transform each record of the input stream into zero or more records in the output stream (both key and value type
     * can be altered arbitrarily).
     * A {@link Transformer} (provided by the given {@link TransformerSupplier}) is applied to each input record and
     * computes zero or more output records.
     * Thus, an input record {@code <K,V>} can be transformed into output records {@code <K':V'>, <K'':V''>, ...}.
     * This is a stateful record-by-record operation (cf. {@link #flatMap(KeyValueMapper)}).
     * Furthermore, via {@link Transformer#punctuate(long)} the processing progress can be observed and additional
     * periodic actions can be performed.
     * <p>
     * In order to assign a state, the state must be created and registered beforehand:
     * <pre>{@code
     * // create store
     * StateStoreSupplier myStore = Stores.create("myTransformState")
     *     .withKeys(...)
     *     .withValues(...)
     *     .persistent() // optional
     *     .build();
     *
     * // register store
     * builder.addStore(myStore);
     *
     * KStream outputStream = inputStream.transform(new TransformerSupplier() { ... }, "myTransformState");
     * }</pre>
     * <p>
     * Within the {@link Transformer}, the state is obtained via the
     * {@link  ProcessorContext}.
     * To trigger periodic actions via {@link Transformer#punctuate(long) punctuate()}, a schedule must be registered.
     * The {@link Transformer} must return a {@link KeyValue} type in {@link Transformer#transform(Object, Object)
     * transform()} and {@link Transformer#punctuate(long) punctuate()}.
     * <pre>{@code
     * new TransformerSupplier() {
     *     Transformer get() {
     *         return new Transformer() {
     *             private ProcessorContext context;
     *             private StateStore state;
     *
     *             void init(ProcessorContext context) {
     *                 this.context = context;
     *                 this.state = context.getStateStore("myTransformState");
     *                 context.schedule(1000); // call #punctuate() each 1000ms
     *             }
     *
     *             KeyValue transform(K key, V value) {
     *                 // can access this.state
     *                 // can emit as many new KeyValue pairs as required via this.context#forward()
     *                 return new KeyValue(key, value); // can emit a single value via return -- can also be null
     *             }
     *
     *             KeyValue punctuate(long timestamp) {
     *                 // can access this.state
     *                 // can emit as many new KeyValue pairs as required via this.context#forward()
     *                 return null; // don't return result -- can also be "new KeyValue()"
     *             }
     *
     *             void close() {
     *                 // can access this.state
     *                 // can emit as many new KeyValue pairs as required via this.context#forward()
     *             }
     *         }
     *     }
     * }
     * }</pre>
     * <p>
     * Transforming records might result in an internal data redistribution if a key based operator (like an aggregation
     * or join) is applied to the result {@code KStream}.
     * (cf. {@link #transformValues(ValueTransformerSupplier, String...)})
     *
     * @param transformerSupplier a instance of {@link TransformerSupplier} that generates a {@link Transformer}
     * @param stateStoreNames     the names of the state stores used by the processor
     * @param <K1>                the key type of the new stream
     * @param <V1>                the value type of the new stream
     * @return a {@code KStream} that contains more or less records with new key and value (possibly of different type)
     * @see #flatMap(KeyValueMapper)
     * @see #transformValues(ValueTransformerSupplier, String...)
     * @see #process(ProcessorSupplier, String...)
     */
    <K1, V1> KStream<K1, V1> transform(final TransformerSupplier<? super K, ? super V, KeyValue<K1, V1>> transformerSupplier,
                                       final String... stateStoreNames);

    /**
     * Transform the value of each input record into a new value (with possible new type) of the output record.
     * A {@link ValueTransformer} (provided by the given {@link ValueTransformerSupplier}) is applies to each input
     * record value and computes a new value for it.
     * Thus, an input record {@code <K,V>} can be transformed into an output record {@code <K:V'>}.
     * This is a stateful record-by-record operation (cf. {@link #mapValues(ValueMapper)}).
     * Furthermore, via {@link ValueTransformer#punctuate(long)} the processing progress can be observed and additional
     * periodic actions get be performed.
     * <p>
     * In order to assign a state, the state must be created and registered beforehand:
     * <pre>{@code
     * // create store
     * StateStoreSupplier myStore = Stores.create("myValueTransformState")
     *     .withKeys(...)
     *     .withValues(...)
     *     .persistent() // optional
     *     .build();
     *
     * // register store
     * builder.addStore(myStore);
     *
     * KStream outputStream = inputStream.transformValues(new ValueTransformerSupplier() { ... }, "myValueTransformState");
     * }</pre>
     * <p>
     * Within the {@link ValueTransformer}, the state is obtained via the
     * {@link ProcessorContext}.
     * To trigger periodic actions via {@link ValueTransformer#punctuate(long) punctuate()}, a schedule must be
     * registered.
     * In contrast to {@link #transform(TransformerSupplier, String...) transform()}, no additional {@link KeyValue}
     * pairs should be emitted via {@link ProcessorContext#forward(Object, Object)
     * ProcessorContext.forward()}.
     * <pre>{@code
     * new ValueTransformerSupplier() {
     *     ValueTransformer get() {
     *         return new ValueTransformer() {
     *             private StateStore state;
     *
     *             void init(ProcessorContext context) {
     *                 this.state = context.getStateStore("myValueTransformState");
     *                 context.schedule(1000); // call #punctuate() each 1000ms
     *             }
     *
     *             NewValueType transform(V value) {
     *                 // can access this.state
     *                 return new NewValueType(); // or null
     *             }
     *
     *             NewValueType punctuate(long timestamp) {
     *                 // can access this.state
     *                 return null; // don't return result -- can also be "new NewValueType()" (current key will be used to build KeyValue pair)
     *             }
     *
     *             void close() {
     *                 // can access this.state
     *             }
     *         }
     *     }
     * }
     * }</pre>
     * <p>
     * Setting a new value preserves data co-location with respect to the key.
     * Thus, <em>no</em> internal data redistribution is required if a key based operator (like an aggregation or join)
     * is applied to the result {@code KStream}. (cf. {@link #transform(TransformerSupplier, String...)})
     *
     * @param valueTransformerSupplier a instance of {@link ValueTransformerSupplier} that generates a
     *                                 {@link ValueTransformer}
     * @param stateStoreNames          the names of the state stores used by the processor
     * @param <VR>                     the value type of the result stream
     * @return a {@code KStream} that contains records with unmodified key and new values (possibly of different type)
     * @see #mapValues(ValueMapper)
     * @see #transform(TransformerSupplier, String...)
     */
    <VR> KStream<K, VR> transformValues(final ValueTransformerSupplier<? super V, ? extends VR> valueTransformerSupplier,
                                        final String... stateStoreNames);

    /**
     * Process all records in this stream, one record at a time, by applying a {@link Processor} (provided by the given
     * {@link ProcessorSupplier}).
     * This is a stateful record-by-record operation (cf. {@link #foreach(ForeachAction)}).
     * Furthermore, via {@link Processor#punctuate(long)} the processing progress can be observed and additional
     * periodic actions can be performed.
     * Note that this is a terminal operation that returns void.
     * <p>
     * In order to assign a state, the state must be created and registered beforehand:
     * <pre>{@code
     * // create store
     * StateStoreSupplier myStore = Stores.create("myProcessorState")
     *     .withKeys(...)
     *     .withValues(...)
     *     .persistent() // optional
     *     .build();
     *
     * // register store
     * builder.addStore(myStore);
     *
     * inputStream.process(new ProcessorSupplier() { ... }, "myProcessorState");
     * }</pre>
     * <p>
     * Within the {@link Processor}, the state is obtained via the
     * {@link ProcessorContext}.
     * To trigger periodic actions via {@link Processor#punctuate(long) punctuate()},
     * a schedule must be registered.
     * <pre>{@code
     * new ProcessorSupplier() {
     *     Processor get() {
     *         return new Processor() {
     *             private StateStore state;
     *
     *             void init(ProcessorContext context) {
     *                 this.state = context.getStateStore("myProcessorState");
     *                 context.schedule(1000); // call #punctuate() each 1000ms
     *             }
     *
     *             void process(K key, V value) {
     *                 // can access this.state
     *             }
     *
     *             void punctuate(long timestamp) {
     *                 // can access this.state
     *             }
     *
     *             void close() {
     *                 // can access this.state
     *             }
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param processorSupplier a instance of {@link ProcessorSupplier} that generates a {@link Processor}
     * @param stateStoreNames   the names of the state store used by the processor
     * @see #foreach(ForeachAction)
     * @see #transform(TransformerSupplier, String...)
     */
    void process(final ProcessorSupplier<? super K, ? super V> processorSupplier,
                 final String... stateStoreNames);

    /**
     * Group the records by their current key into a {@link KGroupedStream} while preserving the original values
     * and default serializers and deserializers.
     * Grouping a stream on the record key is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedStream}).
     * If a record key is {@code null} the record will not be included in the resulting {@link KGroupedStream}.
     * <p>
     * If a key changing operator was used before this operation (e.g., {@link #selectKey(KeyValueMapper)},
     * {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)}, or
     * {@link #transform(TransformerSupplier, String...)}), and no data redistribution happened afterwards (e.g., via
     * {@link #through(String)}) an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
     * an internally generated name, and "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the resulting {@link KGroupedStream} is partitioned
     * correctly on its key.
     * If the last key changing operator changed the key type, it is recommended to use
     * {@link #groupByKey(Serde, Serde)} instead.
     *
     * @return a {@link KGroupedStream} that contains the grouped records of the original {@code KStream}
     * @see #groupBy(KeyValueMapper)
     */
    KGroupedStream<K, V> groupByKey();

    /**
     * Group the records by their current key into a {@link KGroupedStream} while preserving the original values.
     * Grouping a stream on the record key is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedStream}).
     * If a record key is {@code null} the record will not be included in the resulting {@link KGroupedStream}.
     * <p>
     * If a key changing operator was used before this operation (e.g., {@link #selectKey(KeyValueMapper)},
     * {@link #map(KeyValueMapper)}, {@link #flatMap(KeyValueMapper)}, or
     * {@link #transform(TransformerSupplier, String...)}), and no data redistribution happened afterwards (e.g., via
     * {@link #through(String)}) an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
     * an internally generated name, and "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * For this case, all data of this stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the resulting {@link KGroupedStream} is partitioned
     * correctly on its key.
     *
     * @param keySerde key serdes for materializing this stream,
     *                 if not specified the default serdes defined in the configs will be used
     * @param valSerde value serdes for materializing this stream,
     *                 if not specified the default serdes defined in the configs will be used
     * @return a {@link KGroupedStream} that contains the grouped records of the original {@code KStream}
     */
    KGroupedStream<K, V> groupByKey(final Serde<K> keySerde,
                                    final Serde<V> valSerde);

    /**
     * Group the records of this {@code KStream} on a new key that is selected using the provided {@link KeyValueMapper}
     * and default serializers and deserializers.
     * Grouping a stream on the record key is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedStream}).
     * The {@link KeyValueMapper} selects a new key (with should be of the same type) while preserving the original values.
     * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedStream}
     * <p>
     * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link  StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is
     * an internally generated name, and "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
     * and rereading all records from it, such that the resulting {@link KGroupedStream} is partitioned on the new key.
     * <p>
     * This operation is equivalent to calling {@link #selectKey(KeyValueMapper)} followed by {@link #groupByKey()}.
     * If the key type is changed, it is recommended to use {@link #groupBy(KeyValueMapper, Serde, Serde)} instead.
     *
     * @param selector a {@link KeyValueMapper} that computes a new key for grouping
     * @param <KR>     the key type of the result {@link KGroupedStream}
     * @return a {@link KGroupedStream} that contains the grouped records of the original {@code KStream}
     */
    <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> selector);

    /**
     * Group the records of this {@code KStream} on a new key that is selected using the provided {@link KeyValueMapper}.
     * Grouping a stream on the record key is required before an aggregation operator can be applied to the data
     * (cf. {@link KGroupedStream}).
     * The {@link KeyValueMapper} selects a new key (with potentially different type) while preserving the original values.
     * If the new record key is {@code null} the record will not be included in the resulting {@link KGroupedStream}.
     * <p>
     * Because a new key is selected, an internal repartitioning topic will be created in Kafka.
     * This topic will be named "${applicationId}-XXX-repartition", where "applicationId" is user-specified in
     * {@link StreamsConfig StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * All data of this stream will be redistributed through the repartitioning topic by writing all records to it,
     * and rereading all records from it, such that the resulting {@link KGroupedStream} is partitioned on the new key.
     * <p>
     * This is equivalent to calling {@link #selectKey(KeyValueMapper)} followed by {@link #groupByKey(Serde, Serde)}.
     *
     * @param selector a {@link KeyValueMapper} that computes a new key for grouping
     * @param keySerde key serdes for materializing this stream,
     *                 if not specified the default serdes defined in the configs will be used
     * @param valSerde value serdes for materializing this stream,
     *                 if not specified the default serdes defined in the configs will be used
     * @param <KR>     the key type of the result {@link KGroupedStream}
     * @return a {@link KGroupedStream} that contains the grouped records of the original {@code KStream}
     * @see #groupByKey()
     */
    <KR> KGroupedStream<KR, V> groupBy(final KeyValueMapper<? super K, ? super V, KR> selector,
                                       final Serde<KR> keySerde,
                                       final Serde<V> valSerde);

    /**
     * Join records of this stream with another {@code KStream}'s records using windowed inner equi join with default
     * serializers and deserializers.
     * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     * <p>
     * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
     * a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
     * output record will be added to the resulting {@code KStream}.
     * <p>
     * Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>this</th>
     * <th>other</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key.
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link  StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen for one or both of the joining {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     * <p>
     * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
     * in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is an
     * internally generated name, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param otherStream the {@code KStream} to be joined with this stream
     * @param joiner      a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows     the specification of the {@link JoinWindows}
     * @param <VO>        the value type of the other stream
     * @param <VR>        the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key and within the joining window intervals
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows)
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows)
     */
    <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                 final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                 final JoinWindows windows);

    /**
     * Join records of this stream with another {@code KStream}'s records using windowed inner equi join.
     * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     * <p>
     * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
     * a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
     * output record will be added to the resulting {@code KStream}.
     * <p>
     * Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>this</th>
     * <th>other</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key.
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen for one or both of the joining {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     * <p>
     * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
     * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param otherStream     the {@code KStream} to be joined with this stream
     * @param joiner          a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows         the specification of the {@link JoinWindows}
     * @param keySerde        key serdes for materializing both streams,
     *                        if not specified the default serdes defined in the configs will be used
     * @param thisValueSerde  value serdes for materializing this stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde value serdes for materializing the other stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param <VO>            the value type of the other stream
     * @param <VR>            the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key and within the joining window intervals
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
     */
    <VO, VR> KStream<K, VR> join(final KStream<K, VO> otherStream,
                                 final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                 final JoinWindows windows,
                                 final Serde<K> keySerde,
                                 final Serde<V> thisValueSerde,
                                 final Serde<VO> otherValueSerde);

    /**
     * Join records of this stream with another {@code KStream}'s records using windowed left equi join with default
     * serializers and deserializers.
     * In contrast to {@link #join(KStream, ValueJoiner, JoinWindows) inner-join}, all records from this stream will
     * produce at least one output record (cf. below).
     * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     * <p>
     * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
     * a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * Furthermore, for each input record of this {@code KStream} that does not satisfy the join predicate the provided
     * {@link ValueJoiner} will be called with a {@code null} value for the other stream.
     * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
     * output record will be added to the resulting {@code KStream}.
     * <p>
     * Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>this</th>
     * <th>other</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key.
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen for one or both of the joining {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     * <p>
     * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
     * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param otherStream the {@code KStream} to be joined with this stream
     * @param joiner      a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows     the specification of the {@link JoinWindows}
     * @param <VO>        the value type of the other stream
     * @param <VR>        the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * this {@code KStream} and within the joining window intervals
     * @see #join(KStream, ValueJoiner, JoinWindows)
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows)
     */
    <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
                                     final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                     final JoinWindows windows);

    /**
     * Join records of this stream with another {@code KStream}'s records using windowed left equi join.
     * In contrast to {@link #join(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde) inner-join}, all records from
     * this stream will produce at least one output record (cf. below).
     * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     * <p>
     * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
     * a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * Furthermore, for each input record of this {@code KStream} that does not satisfy the join predicate the provided
     * {@link ValueJoiner} will be called with a {@code null} value for the other stream.
     * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
     * output record will be added to the resulting {@code KStream}.
     * <p>
     * Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>this</th>
     * <th>other</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td></td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key.
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen for one or both of the joining {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     * <p>
     * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
     * in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "storeName" is an
     * internally generated name, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param otherStream     the {@code KStream} to be joined with this stream
     * @param joiner          a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows         the specification of the {@link JoinWindows}
     * @param keySerde        key serdes for materializing the other stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param thisValSerde    value serdes for materializing this stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde value serdes for materializing the other stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param <VO>            the value type of the other stream
     * @param <VR>            the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * this {@code KStream} and within the joining window intervals
     * @see #join(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
     * @see #outerJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
     */
    <VO, VR> KStream<K, VR> leftJoin(final KStream<K, VO> otherStream,
                                     final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                     final JoinWindows windows,
                                     final Serde<K> keySerde,
                                     final Serde<V> thisValSerde,
                                     final Serde<VO> otherValueSerde);

    /**
     * Join records of this stream with another {@code KStream}'s records using windowed left equi join with default
     * serializers and deserializers.
     * In contrast to {@link #join(KStream, ValueJoiner, JoinWindows) inner-join} or
     * {@link #leftJoin(KStream, ValueJoiner, JoinWindows) left-join}, all records from both streams will produce at
     * least one output record (cf. below).
     * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     * <p>
     * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
     * a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * Furthermore, for each input record of both {@code KStream}s that does not satisfy the join predicate the provided
     * {@link ValueJoiner} will be called with a {@code null} value for the this/other stream, respectively.
     * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
     * output record will be added to the resulting {@code KStream}.
     * <p>
     * Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>this</th>
     * <th>other</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(null,b)&gt;<br />&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td>&lt;K3:ValueJoiner(null,c)&gt;</td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key.
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen for one or both of the joining {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     * <p>
     * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
     * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param otherStream the {@code KStream} to be joined with this stream
     * @param joiner      a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows     the specification of the {@link JoinWindows}
     * @param <VO>        the value type of the other stream
     * @param <VR>        the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * both {@code KStream} and within the joining window intervals
     * @see #join(KStream, ValueJoiner, JoinWindows)
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows)
     */
    <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
                                      final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                      final JoinWindows windows);

    /**
     * Join records of this stream with another {@code KStream}'s records using windowed left equi join.
     * In contrast to {@link #join(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde) inner-join} or
     * {@link #leftJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde) left-join}, all records from both
     * streams will produce at least one output record (cf. below).
     * The join is computed on the records' key with join attribute {@code thisKStream.key == otherKStream.key}.
     * Furthermore, two records are only joined if their timestamps are close to each other as defined by the given
     * {@link JoinWindows}, i.e., the window defines an additional join predicate on the record timestamps.
     * <p>
     * For each pair of records meeting both join predicates the provided {@link ValueJoiner} will be called to compute
     * a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * Furthermore, for each input record of both {@code KStream}s that does not satisfy the join predicate the provided
     * {@link ValueJoiner} will be called with a {@code null} value for this/other stream, respectively.
     * If an input record key or value is {@code null} the record will not be included in the join operation and thus no
     * output record will be added to the resulting {@code KStream}.
     * <p>
     * Example (assuming all input records belong to the correct windows):
     * <table border='1'>
     * <tr>
     * <th>this</th>
     * <th>other</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td>&lt;K2:B&gt;</td>
     * <td>&lt;K2:b&gt;</td>
     * <td>&lt;K2:ValueJoiner(null,b)&gt;<br />&lt;K2:ValueJoiner(B,b)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K3:c&gt;</td>
     * <td>&lt;K3:ValueJoiner(null,c)&gt;</td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key.
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen for one or both of the joining {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     * <p>
     * Both of the joining {@code KStream}s will be materialized in local state stores with auto-generated store names.
     * For failure and recovery each store will be backed by an internal changelog topic that will be created in Kafka.
     * The changelog topic will be named "${applicationId}-storeName-changelog", where "applicationId" is user-specified
     * in {@link StreamsConfig} via parameter {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG},
     * "storeName" is an internally generated name, and "-changelog" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     *
     * @param otherStream     the {@code KStream} to be joined with this stream
     * @param joiner          a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param windows         the specification of the {@link JoinWindows}
     * @param keySerde        key serdes for materializing both streams,
     *                        if not specified the default serdes defined in the configs will be used
     * @param thisValueSerde  value serdes for materializing this stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param otherValueSerde value serdes for materializing the other stream,
     *                        if not specified the default serdes defined in the configs will be used
     * @param <VO>            the value type of the other stream
     * @param <VR>            the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key plus one for each non-matching record of
     * both {@code KStream}s and within the joining window intervals
     * @see #join(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
     * @see #leftJoin(KStream, ValueJoiner, JoinWindows, Serde, Serde, Serde)
     */
    <VO, VR> KStream<K, VR> outerJoin(final KStream<K, VO> otherStream,
                                      final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                      final JoinWindows windows,
                                      final Serde<K> keySerde,
                                      final Serde<V> thisValueSerde,
                                      final Serde<VO> otherValueSerde);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed inner equi join with default
     * serializers and deserializers.
     * The join is a primary key table lookup join with join attribute {@code stream.key == table.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
     * {@link KTable} state.
     * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
     * will not produce any result records.
     * <p>
     * For each {@code KStream} record that finds a corresponding record in {@link KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
     * operation and thus no output record will be added to the resulting {@code KStream}.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>KTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key (cf.
     * {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner)}).
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen only for this {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     *
     * @param table  the {@link KTable} to be joined with this stream
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VT>   the value type of the table
     * @param <VR>   the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key
     * @see #leftJoin(KTable, ValueJoiner)
     * @see #join(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <VT, VR> KStream<K, VR> join(final KTable<K, VT> table,
                                 final ValueJoiner<? super V, ? super VT, ? extends VR> joiner);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed inner equi join.
     * The join is a primary key table lookup join with join attribute {@code stream.key == table.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
     * {@link KTable} state.
     * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
     * will not produce any result records.
     * <p>
     * For each {@code KStream} record that finds a corresponding record in {@link KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as for both joining input records.
     * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
     * operation and thus no output record will be added to the resulting {@code KStream}.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>KTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td></td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key (cf.
     * {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner)}).
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen only for this {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     *
     * @param table    the {@link KTable} to be joined with this stream
     * @param joiner   a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param keySerde key serdes for materializing this stream.
     *                 If not specified the default serdes defined in the configs will be used
     * @param valSerde value serdes for materializing this stream,
     *                 if not specified the default serdes defined in the configs will be used
     * @param <VT>     the value type of the table
     * @param <VR>     the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one for each matched record-pair with the same key
     * @see #leftJoin(KTable, ValueJoiner, Serde, Serde)
     * @see #join(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <VT, VR> KStream<K, VR> join(final KTable<K, VT> table,
                                 final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
                                 final Serde<K> keySerde,
                                 final Serde<V> valSerde);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed left equi join with default
     * serializers and deserializers.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, all records from this stream will produce an
     * output record (cf. below).
     * The join is a primary key table lookup join with join attribute {@code stream.key == table.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
     * {@link KTable} state.
     * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
     * will not produce any result records.
     * <p>
     * For each {@code KStream} record weather or not it finds a corresponding record in {@link KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * If no {@link KTable} record was found during lookup, a {@code null} value will be provided to {@link ValueJoiner}.
     * The key of the result record is the same as for both joining input records.
     * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
     * operation and thus no output record will be added to the resulting {@code KStream}.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>KTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key (cf.
     * {@link #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)}).
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen only for this {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     *
     * @param table  the {@link KTable} to be joined with this stream
     * @param joiner a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <VT>   the value type of the table
     * @param <VR>   the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one output for each input {@code KStream} record
     * @see #join(KTable, ValueJoiner)
     * @see #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <VT, VR> KStream<K, VR> leftJoin(final KTable<K, VT> table,
                                     final ValueJoiner<? super V, ? super VT, ? extends VR> joiner);

    /**
     * Join records of this stream with {@link KTable}'s records using non-windowed left equi join.
     * In contrast to {@link #join(KTable, ValueJoiner) inner-join}, all records from this stream will produce an
     * output record (cf. below).
     * The join is a primary key table lookup join with join attribute {@code stream.key == table.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> (i.e., processing time) internal
     * {@link KTable} state.
     * In contrast, processing {@link KTable} input records will only update the internal {@link KTable} state and
     * will not produce any result records.
     * <p>
     * For each {@code KStream} record weather or not it finds a corresponding record in {@link KTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * If no {@link KTable} record was found during lookup, a {@code null} value will be provided to {@link ValueJoiner}.
     * The key of the result record is the same as for both joining input records.
     * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
     * operation and thus no output record will be added to the resulting {@code KStream}.
     * <p>
     * Example:
     * <table border='1'>
     * <tr>
     * <th>KStream</th>
     * <th>KTable</th>
     * <th>state</th>
     * <th>result</th>
     * </tr>
     * <tr>
     * <td>&lt;K1:A&gt;</td>
     * <td></td>
     * <td></td>
     * <td>&lt;K1:ValueJoiner(A,null)&gt;</td>
     * </tr>
     * <tr>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:b&gt;</td>
     * <td></td>
     * </tr>
     * <tr>
     * <td>&lt;K1:C&gt;</td>
     * <td></td>
     * <td>&lt;K1:b&gt;</td>
     * <td>&lt;K1:ValueJoiner(C,b)&gt;</td>
     * </tr>
     * </table>
     * Both input streams need to be co-partitioned on the join key (cf.
     * {@link #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)}).
     * If this requirement is not met, Kafka Streams will automatically repartition the data, i.e., it will create an
     * internal repartitioning topic in Kafka and write and re-read the data via this topic before the actual join.
     * The repartitioning topic will be named "${applicationId}-XXX-repartition", where "applicationId" is
     * user-specified in {@link StreamsConfig} via parameter
     * {@link StreamsConfig#APPLICATION_ID_CONFIG APPLICATION_ID_CONFIG}, "XXX" is an internally generated name, and
     * "-repartition" is a fixed suffix.
     * You can retrieve all generated internal topic names via {@link KafkaStreams#toString()}.
     * <p>
     * Repartitioning can happen only for this {@code KStream}s.
     * For this case, all data of the stream will be redistributed through the repartitioning topic by writing all
     * records to it, and rereading all records from it, such that the join input {@code KStream} is partitioned
     * correctly on its key.
     *
     * @param table    the {@link KTable} to be joined with this stream
     * @param joiner   a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param keySerde key serdes for materializing this stream.
     *                 If not specified the default serdes defined in the configs will be used
     * @param valSerde value serdes for materializing this stream,
     *                 if not specified the default serdes defined in the configs will be used
     * @param <VT>     the value type of the table
     * @param <VR>     the value type of the result stream
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one output for each input {@code KStream} record
     * @see #join(KTable, ValueJoiner, Serde, Serde)
     * @see #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <VT, VR> KStream<K, VR> leftJoin(final KTable<K, VT> table,
                                     final ValueJoiner<? super V, ? super VT, ? extends VR> joiner,
                                     final Serde<K> keySerde,
                                     final Serde<V> valSerde);

    /**
     * Join records of this stream with {@link GlobalKTable}'s records using non-windowed inner equi join.
     * The join is a primary key table lookup join with join attribute
     * {@code keyValueMapper.map(stream.keyValue) == table.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> internal {@link GlobalKTable}
     * state.
     * In contrast, processing {@link GlobalKTable} input records will only update the internal {@link GlobalKTable}
     * state and will not produce any result records.
     * <p>
     * For each {@code KStream} record that finds a corresponding record in {@link GlobalKTable} the provided
     * {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * The key of the result record is the same as the key of this {@code KStream}.
     * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
     * operation and thus no output record will be added to the resulting {@code KStream}.
     *
     * @param globalKTable   the {@link GlobalKTable} to be joined with this stream
     * @param keyValueMapper instance of {@link KeyValueMapper} used to map from the (key, value) of this stream
     *                       to the key of the {@link GlobalKTable}
     * @param joiner         a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <GK>           the key type of {@link GlobalKTable}
     * @param <GV>           the value type of the {@link GlobalKTable}
     * @param <RV>           the value type of the resulting {@code KStream}
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one output for each input {@code KStream} record
     * @see #leftJoin(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <GK, GV, RV> KStream<K, RV> join(final GlobalKTable<GK, GV> globalKTable,
                                     final KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper,
                                     final ValueJoiner<? super V, ? super GV, ? extends RV> joiner);

    /**
     * Join records of this stream with {@link GlobalKTable}'s records using non-windowed left equi join.
     * In contrast to {@link #join(GlobalKTable, KeyValueMapper, ValueJoiner) inner-join}, all records from this stream
     * will produce an output record (cf. below).
     * The join is a primary key table lookup join with join attribute
     * {@code keyValueMapper.map(stream.keyValue) == table.key}.
     * "Table lookup join" means, that results are only computed if {@code KStream} records are processed.
     * This is done by performing a lookup for matching records in the <em>current</em> internal {@link GlobalKTable}
     * state.
     * In contrast, processing {@link GlobalKTable} input records will only update the internal {@link GlobalKTable}
     * state and will not produce any result records.
     * <p>
     * For each {@code KStream} record whether or not it finds a corresponding record in {@link GlobalKTable} the
     * provided {@link ValueJoiner} will be called to compute a value (with arbitrary type) for the result record.
     * If no {@link GlobalKTable} record was found during lookup, a {@code null} value will be provided to
     * {@link ValueJoiner}.
     * The key of the result record is the same as this {@code KStream}.
     * If an {@code KStream} input record key or value is {@code null} the record will not be included in the join
     * operation and thus no output record will be added to the resulting {@code KStream}.
     *
     * @param globalKTable   the {@link GlobalKTable} to be joined with this stream
     * @param keyValueMapper instance of {@link KeyValueMapper} used to map from the (key, value) of this stream
     *                       to the key of the {@link GlobalKTable}
     * @param valueJoiner    a {@link ValueJoiner} that computes the join result for a pair of matching records
     * @param <GK>           the key type of {@link GlobalKTable}
     * @param <GV>           the value type of the {@link GlobalKTable}
     * @param <RV>           the value type of the resulting {@code KStream}
     * @return a {@code KStream} that contains join-records for each key and values computed by the given
     * {@link ValueJoiner}, one output for each input {@code KStream} record
     * @see #join(GlobalKTable, KeyValueMapper, ValueJoiner)
     */
    <GK, GV, RV> KStream<K, RV> leftJoin(final GlobalKTable<GK, GV> globalKTable,
                                         final KeyValueMapper<? super K, ? super V, ? extends GK> keyValueMapper,
                                         final ValueJoiner<? super V, ? super GV, ? extends RV> valueJoiner);

}
