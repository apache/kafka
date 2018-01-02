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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;

import java.io.File;
import java.util.Map;

/**
 * Processor context interface.
 */
@InterfaceStability.Evolving
public interface ProcessorContext {

    /**
     * Returns the application id
     *
     * @return the application id
     */
    String applicationId();

    /**
     * Returns the task id
     *
     * @return the task id
     */
    TaskId taskId();

    /**
     * Returns the default key serde
     *
     * @return the key serializer
     */
    Serde<?> keySerde();

    /**
     * Returns the default value serde
     *
     * @return the value serializer
     */
    Serde<?> valueSerde();

    /**
     * Returns the state directory for the partition.
     *
     * @return the state directory
     */
    File stateDir();

    /**
     * Returns Metrics instance
     *
     * @return StreamsMetrics
     */
    StreamsMetrics metrics();

    /**
     * Registers and possibly restores the specified storage engine.
     *
     * @param store the storage engine
     * @param loggingEnabledIsDeprecatedAndIgnored deprecated parameter {@code loggingEnabled} is ignored:
     *                                             if you want to enable logging on a state stores call
     *                                             {@link org.apache.kafka.streams.state.StoreBuilder#withLoggingEnabled(Map)}
     *                                             when creating the store
     * @throws IllegalStateException If store gets registered after initialized is already finished
     * @throws StreamsException if the store's change log does not contain the partition
     */
    void register(StateStore store, boolean loggingEnabledIsDeprecatedAndIgnored, StateRestoreCallback stateRestoreCallback);

    /**
     * Get the state store given the store name.
     *
     * @param name The store name
     * @return The state store instance
     */
    StateStore getStateStore(String name);

    /**
     * Schedules a periodic operation for processors. A processor may call this method during
     * {@link Processor#init(ProcessorContext) initialization} or
     * {@link Processor#process(Object, Object) processing} to
     * schedule a periodic callback - called a punctuation - to {@link Punctuator#punctuate(long)}.
     * The type parameter controls what notion of time is used for punctuation:
     * <ul>
     *   <li>{@link PunctuationType#STREAM_TIME} - uses "stream time", which is advanced by the processing of messages
     *   in accordance with the timestamp as extracted by the {@link TimestampExtractor} in use.
     *   <b>NOTE:</b> Only advanced if messages arrive</li>
     *   <li>{@link PunctuationType#WALL_CLOCK_TIME} - uses system time (the wall-clock time),
     *   which is advanced at the polling interval ({@link org.apache.kafka.streams.StreamsConfig#POLL_MS_CONFIG})
     *   independent of whether new messages arrive. <b>NOTE:</b> This is best effort only as its granularity is limited
     *   by how long an iteration of the processing loop takes to complete</li>
     * </ul>
     *
     * @param interval the time interval between punctuations
     * @param type one of: {@link PunctuationType#STREAM_TIME}, {@link PunctuationType#WALL_CLOCK_TIME}
     * @param callback a function consuming timestamps representing the current stream or system time
     * @return a handle allowing cancellation of the punctuation schedule established by this method
     */
    Cancellable schedule(long interval, PunctuationType type, Punctuator callback);

    /**
     * Schedules a periodic operation for processors. A processor may call this method during
     * {@link Processor#init(ProcessorContext) initialization} to
     * schedule a periodic call - called a punctuation - to {@link Processor#punctuate(long)}.
     *
     * @deprecated Please use {@link #schedule(long, PunctuationType, Punctuator)} instead.
     *
     * @param interval the time interval between punctuations
     */
    @Deprecated
    void schedule(long interval);

    /**
     * Forwards a key/value pair to the downstream processors
     * @param key key
     * @param value value
     */
    <K, V> void forward(K key, V value);

    /**
     * Forwards a key/value pair to one of the downstream processors designated by childIndex
     * @param key key
     * @param value value
     * @param childIndex index in list of children of this node
     */
    <K, V> void forward(K key, V value, int childIndex);

    /**
     * Forwards a key/value pair to one of the downstream processors designated by the downstream processor name
     * @param key key
     * @param value value
     * @param childName name of downstream processor
     */
    <K, V> void forward(K key, V value, String childName);

    /**
     * Requests a commit
     */
    void commit();

    /**
     * Returns the topic name of the current input record; could be null if it is not
     * available (for example, if this method is invoked from the punctuate call)
     *
     * @return the topic name
     */
    String topic();

    /**
     * Returns the partition id of the current input record; could be -1 if it is not
     * available (for example, if this method is invoked from the punctuate call)
     *
     * @return the partition id
     */
    int partition();

    /**
     * Returns the offset of the current input record; could be -1 if it is not
     * available (for example, if this method is invoked from the punctuate call)
     *
     * @return the offset
     */
    long offset();

    /**
     * Returns the current timestamp.
     *
     * If it is triggered while processing a record streamed from the source processor, timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
     * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} by {@link TimestampExtractor}.
     *
     * If it is triggered while processing a record generated not from the source processor (for example,
     * if this method is invoked from the punctuate call), timestamp is defined as the current
     * task's stream time, which is defined as the smallest among all its input stream partition timestamps.
     *
     * @return the timestamp
     */
    long timestamp();

    /**
     * Returns all the application config properties as key/value pairs.
     *
     * The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
     * object and associated to the ProcessorContext.
     * <p>
     * The type of the values is dependent on the {@link org.apache.kafka.common.config.ConfigDef.Type type} of the property
     * (e.g. the value of {@link org.apache.kafka.streams.StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG DEFAULT_KEY_SERDE_CLASS_CONFIG}
     * will be of type {@link Class}, even if it was specified as a String to
     * {@link org.apache.kafka.streams.StreamsConfig#StreamsConfig(Map) StreamsConfig(Map)}).
     *
     * @return all the key/values from the StreamsConfig properties
     */
    Map<String, Object> appConfigs();

    /**
     * Returns all the application config properties with the given key prefix, as key/value pairs
     * stripping the prefix.
     *
     * The config properties are defined in the {@link org.apache.kafka.streams.StreamsConfig}
     * object and associated to the ProcessorContext.
     *
     * @param prefix the properties prefix
     * @return the key/values matching the given prefix from the StreamsConfig properties.
     *
     */
    Map<String, Object> appConfigsWithPrefix(String prefix);

}
