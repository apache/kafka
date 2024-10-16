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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;

import java.io.Closeable;

/**
 * Partitioner Interface
 */
public interface Partitioner extends Configurable, Closeable {

    /**
     * Indicate if the given topic is handled.  Returning {@code false} will cause the Producer to fallback to default partitioning.
     *
     * @param topic The topic name
     */
    default boolean partitionsTopic(String topic) {
        return true;
    }

    /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes The serialized key to partition on( or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);

    /**
     * This is called when partitioner is closed.
     */
    void close();

    /**
     * Note this method is only implemented in DefaultPartitioner and {@link UniformStickyPartitioner} which
     * are now deprecated. See <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-794%3A+Strictly+Uniform+Sticky+Partitioner">KIP-794</a> for more info.
     * <p>
     * Notifies the partitioner a new batch is about to be created. When using the sticky partitioner,
     * this method can change the chosen sticky partition for the new batch.
     * @param topic The topic name
     * @param cluster The current cluster metadata
     * @param prevPartition The partition previously selected for the record that triggered a new batch
     * @deprecated Since 3.3.0
     */
    @Deprecated
    default void onNewBatch(String topic, Cluster cluster, int prevPartition) {
    }
}
