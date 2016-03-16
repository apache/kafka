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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsMetrics;

import java.io.File;

/**
 * Processor context interface.
 */
public interface ProcessorContext {

    /**
     * Returns the job id
     *
     * @return the job id
     */
    String jobId();

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
     */
    <K, V> void forward(K key, V value, int childIndex);

    /**
     * Requests a commit
     */
    void commit();

    /**
     * Returns the topic name of the current input record
     *
     * @return the topic name
     */
    String topic();

    /**
     * Returns the partition id of the current input record
     *
     * @return the partition id
     */
    int partition();

    /**
     * Returns the offset of the current input record
     *
     * @return the offset
     */
    long offset();

    /**
     * Returns the timestamp of the current input record. The timestamp is extracted from
     * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} by {@link TimestampExtractor}.
     *
     * @return the timestamp
     */
    long timestamp();
}
