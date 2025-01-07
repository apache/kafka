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
package org.apache.kafka.streams.processor;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;

import java.io.File;
import java.time.Duration;
import java.util.Map;

/**
 * Processor context interface.
 */
/* This interface was technically deprecated via KIP-478 (AK 2.7), but we did not mark it as deprecated yet,
 *  as it's used on many other places
 *
 * We need to clean this all up (https://issues.apache.org/jira/browse/KAFKA-17131) and mark the interface
 * deprecated afterward.
 */
@SuppressWarnings("deprecation")
public interface ProcessorContext {

    /**
     * Return the application id.
     *
     * @return the application id
     */
    String applicationId();

    /**
     * Return the task id.
     *
     * @return the task id
     */
    TaskId taskId();

    /**
     * Return the default key serde.
     *
     * @return the key serializer
     */
    Serde<?> keySerde();

    /**
     * Return the default value serde.
     *
     * @return the value serializer
     */
    Serde<?> valueSerde();

    /**
     * Return the state directory for the partition.
     *
     * @return the state directory
     */
    File stateDir();

    /**
     * Return Metrics instance.
     *
     * @return StreamsMetrics
     */
    StreamsMetrics metrics();

    /**
     * Register and possibly restores the specified storage engine.
     *
     * @param store the storage engine
     * @param stateRestoreCallback the restoration callback logic for log-backed state stores upon restart
     *
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void register(final StateStore store,
                  final StateRestoreCallback stateRestoreCallback);

    /**
     * Get the state store given the store name.
     *
     * @param name The store name
     * @param <S> The type or interface of the store to return
     * @return The state store instance
     *
     * @throws ClassCastException if the return type isn't a type or interface of the actual returned store.
     */
    <S extends StateStore> S getStateStore(final String name);

    /**
     * Schedule a periodic operation for processors. A processor may call this method during
     * {@link org.apache.kafka.streams.kstream.ValueTransformer#init(ProcessorContext)  initialization} or
     * {@link org.apache.kafka.streams.kstream.ValueTransformer#transform(Object)  processing} to
     * schedule a periodic callback &mdash; called a punctuation &mdash; to {@link Punctuator#punctuate(long)}.
     * The type parameter controls what notion of time is used for punctuation:
     * <ul>
     *   <li>{@link PunctuationType#STREAM_TIME} &mdash; uses "stream time", which is advanced by the processing of messages
     *   in accordance with the timestamp as extracted by the {@link TimestampExtractor} in use.
     *   The first punctuation will be triggered by the first record that is processed.
     *   <b>NOTE:</b> Only advanced if messages arrive</li>
     *   <li>{@link PunctuationType#WALL_CLOCK_TIME} &mdash; uses system time (the wall-clock time),
     *   which is advanced independent of whether new messages arrive.
     *   The first punctuation will be triggered after interval has elapsed.
     *   <b>NOTE:</b> This is best effort only as its granularity is limited by how long an iteration of the
     *   processing loop takes to complete</li>
     * </ul>
     *
     * <b>Skipping punctuations:</b> Punctuations will not be triggered more than once at any given timestamp.
     * This means that "missed" punctuation will be skipped.
     * It's possible to "miss" a punctuation if:
     * <ul>
     *   <li>with {@link PunctuationType#STREAM_TIME}, when stream time advances more than interval</li>
     *   <li>with {@link PunctuationType#WALL_CLOCK_TIME}, on GC pause, too short interval, ...</li>
     * </ul>
     *
     * @param interval the time interval between punctuations (supported minimum is 1 millisecond)
     * @param type one of: {@link PunctuationType#STREAM_TIME}, {@link PunctuationType#WALL_CLOCK_TIME}
     * @param callback a function consuming timestamps representing the current stream or system time
     * @return a handle allowing cancellation of the punctuation schedule established by this method
     * @throws IllegalArgumentException if the interval is not representable in milliseconds
     */
    Cancellable schedule(final Duration interval,
                         final PunctuationType type,
                         final Punctuator callback);

    /**
     * Forward a key/value pair to all downstream processors.
     * Used the input record's timestamp as timestamp for the output record.
     *
     * <p> If this method is called with {@link Punctuator#punctuate(long)} the record that
     * is sent downstream won't have any associated record metadata like topic, partition, or offset.
     *
     * @param key key
     * @param value value
     */
    <K, V> void forward(final K key, final V value);

    /**
     * Forward a key/value pair to the specified downstream processors.
     * Can be used to set the timestamp of the output record.
     *
     * <p> If this method is called with {@link Punctuator#punctuate(long)} the record that
     * is sent downstream won't have any associated record metadata like topic, partition, or offset.
     *
     * @param key key
     * @param value value
     * @param to the options to use when forwarding
     */
    <K, V> void forward(final K key, final V value, final To to);

    /**
     * Request a commit.
     */
    void commit();

    /**
     * Return the topic name of the current input record; could be {@code null} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated topic.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid topic name, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the topic name
     */
    String topic();

    /**
     * Return the partition id of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated partition id.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid partition id, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the partition id
     */
    int partition();

    /**
     * Return the offset of the current input record; could be {@code -1} if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record won't have an associated offset.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide a valid offset, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the offset
     */
    long offset();

    /**
     * Return the headers of the current input record; could be an empty header if it is not
     * available.
     *
     * <p> For example, if this method is invoked within a {@link Punctuator#punctuate(long)
     * punctuation callback}, or while processing a record that was forwarded by a punctuation
     * callback, the record might not have any associated headers.
     * Another example is
     * {@link org.apache.kafka.streams.kstream.KTable#transformValues(ValueTransformerWithKeySupplier, String...)}
     * (and siblings), that do not always guarantee to provide valid headers, as they might be
     * executed "out-of-band" due to some internal optimizations applied by the Kafka Streams DSL.
     *
     * @return the headers
     */
    Headers headers();

    /**
     * Return the current timestamp.
     *
     * <p> If it is triggered while processing a record streamed from the source processor,
     * timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
     * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} by {@link TimestampExtractor}.
     * Note, that an upstream {@link org.apache.kafka.streams.processor.api.Processor} might have set a new timestamp by calling
     * {@link org.apache.kafka.streams.processor.api.ProcessorContext#forward(org.apache.kafka.streams.processor.api.Record)}.
     * In particular, some Kafka Streams DSL operators set result record timestamps explicitly,
     * to guarantee deterministic results.
     *
     * <p> If it is triggered while processing a record generated not from the source processor (for example,
     * if this method is invoked from the punctuate call), timestamp is defined as the current
     * task's stream time, which is defined as the largest timestamp of any record processed by the task.
     *
     * @return the timestamp
     */
    long timestamp();

    /**
     * Return all the application config properties as key/value pairs.
     *
     * <p> The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
     * object and associated to the ProcessorContext.
     *
     * <p> The type of the values is dependent on the {@link org.apache.kafka.common.config.ConfigDef.Type type} of the property
     * (e.g. the value of {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG DEFAULT_KEY_SERDE_CLASS_CONFIG}
     * will be of type {@link Class}, even if it was specified as a String to
     * {@link org.apache.kafka.streams.StreamsConfig#StreamsConfig(Map) StreamsConfig(Map)}).
     *
     * @return all the key/values from the StreamsConfig properties
     */
    Map<String, Object> appConfigs();

    /**
     * Return all the application config properties with the given key prefix, as key/value pairs
     * stripping the prefix.
     *
     * <p> The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
     * object and associated to the ProcessorContext.
     *
     * @param prefix the properties prefix
     * @return the key/values matching the given prefix from the StreamsConfig properties.
     */
    Map<String, Object> appConfigsWithPrefix(final String prefix);

    /**
     * Return the current system timestamp (also called wall-clock time) in milliseconds.
     *
     * <p> Note: this method returns the internally cached system timestamp from the Kafka Stream runtime.
     * Thus, it may return a different value compared to {@code System.currentTimeMillis()}.
     *
     * @return the current system timestamp in milliseconds
     */
    long currentSystemTimeMs();

    /**
     * Return the current stream-time in milliseconds.
     *
     * <p> Stream-time is the maximum observed {@link TimestampExtractor record timestamp} so far
     * (including the currently processed record), i.e., it can be considered a high-watermark.
     * Stream-time is tracked on a per-task basis and is preserved across restarts and during task migration.
     *
     * <p> Note: this method is not supported for global processors (cf.
     * {@link Topology#addGlobalStore(StoreBuilder, String, Deserializer, Deserializer, String, String, ProcessorSupplier)  Topology.addGlobalStore(...)}
     * and {@link StreamsBuilder#addGlobalStore(StoreBuilder, String, Consumed, ProcessorSupplier) StreamsBuilder.addGlobalStore(...)}),
     * because there is no concept of stream-time for this case.
     * Calling this method in a global processor will result in an {@link UnsupportedOperationException}.
     *
     * @return the current stream-time in milliseconds
     */
    long currentStreamTimeMs();
}
