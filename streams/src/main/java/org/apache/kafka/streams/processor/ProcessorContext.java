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

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;

import java.io.File;

/**
 * Processor context interface.
 */
@InterfaceStability.Unstable
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
     */
    void register(StateStore store, boolean loggingEnabled, StateRestoreCallback stateRestoreCallback);

    /**
     * Get the state store given the store name.
     *
     * @param name The store name
     * @return The state store instance
     */
    StateStore getStateStore(String name);

    /**
     * Schedules a periodic operation for processors. A processor may call this method during
     * {@link Processor#init(ProcessorContext) initialization} to
     * schedule a periodic call called a punctuation to {@link Processor#punctuate(long)}.
     *
     * @param interval the time interval between punctuations
     */
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
}
