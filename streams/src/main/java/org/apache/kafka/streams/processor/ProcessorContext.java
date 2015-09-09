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

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;

public interface ProcessorContext {

    /**
     * Returns the partition group id
     *
     * @return partition group id
     */
    int id();

    /**
     * Returns the key serializer
     *
     * @return the key serializer
     */
    Serializer<?> keySerializer();

    /**
     * Returns the value serializer
     *
     * @return the value serializer
     */
    Serializer<?> valueSerializer();

    /**
     * Returns the key deserializer
     *
     * @return the key deserializer
     */
    Deserializer<?> keyDeserializer();

    /**
     * Returns the value deserializer
     *
     * @return the value deserializer
     */
    Deserializer<?> valueDeserializer();

    /**
     * Returns the state directory for the partition.
     *
     * @return the state directory
     */
    File stateDir();

    /**
     * Returns Metrics instance
     *
     * @return Metrics
     */
    Metrics metrics();

    /**
     * Check if this process's incoming streams are joinable
     */
    boolean joinable();

    /**
     * Registers and possibly restores the specified storage engine.
     *
     * @param store the storage engine
     */
    void register(StateStore store, RestoreFunc restoreFunc);

    StateStore getStateStore(String name);

    void schedule(Processor processor, long interval);

    <K, V> void forward(K key, V value);

    <K, V> void forward(K key, V value, int childIndex);

    void commit();

    String topic();

    int partition();

    long offset();

    long timestamp();
}
