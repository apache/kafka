/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
*/
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.TopicPartition;

/**
 * A key/value pair to be received from Kafka. This consists of a topic name and a partition number, from which the 
 * record is being received and an offset that points to the record in a Kafka partition. 
 */
public final class ConsumerRecord<K,V> {
    private final TopicPartition partition; 
    private final K key;
    private final V value;
    private final long offset;
    private volatile Exception error;
    
    /**
     * Creates a record to be received from a specified topic and partition
     * 
     * @param topic     The topic this record is received from
     * @param partitionId The partition of the topic this record is received from
     * @param key       The key of the record, if one exists
     * @param value     The record contents
     * @param offset    The offset of this record in the corresponding Kafka partition
     */
    public ConsumerRecord(String topic, int partitionId, K key, V value, long offset) {
        this(topic, partitionId, key, value, offset, null);
    }

    /**
     * Create a record with no key
     * 
     * @param topic The topic this record is received from
     * @param partitionId The partition of the topic this record is received from
     * @param value The record contents
     * @param offset The offset of this record in the corresponding Kafka partition
     */
    public ConsumerRecord(String topic, int partitionId, V value, long offset) {
        this(topic, partitionId, null, value, offset);
    }

    /**
     * Creates a record with an error code
     * @param topic     The topic this record is received from
     * @param partitionId The partition of the topic this record is received from
     * @param error     The exception corresponding to the error code returned by the server for this topic partition
     */
    public ConsumerRecord(String topic, int partitionId, Exception error) {
        this(topic, partitionId, null, null, -1L, error);
    }
    
    private ConsumerRecord(String topic, int partitionId, K key, V value, long offset, Exception error) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.partition = new TopicPartition(topic, partitionId);
        this.key = key;
        this.value = value;
        this.offset = offset;  
        this.error = error;
    }
    
    /**
     * The topic this record is received from
     */
    public String topic() {
        return partition.topic();
    }

    /**
     * The partition from which this record is received 
     */
    public int partition() {
        return partition.partition();
    }
    
    /**
     * The TopicPartition object containing the topic and partition
     */
    public TopicPartition topicAndPartition() {
        return partition;
    }
    
    /**
     * The key (or null if no key is specified)
     * @throws Exception The exception thrown while fetching this record.
     */
    public K key() throws Exception {
        if (this.error != null)
            throw this.error;
        return key;
    }

    /**
     * The value
     * @throws Exception The exception thrown while fetching this record.
     */
    public V value() throws Exception {
        if (this.error != null)
            throw this.error;
        return value;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     * @throws Exception The exception thrown while fetching this record.
     */
    public long offset() throws Exception {
        if (this.error != null)
            throw this.error;
        return offset;
    }

    public Exception error() {
        return this.error;
    }
}
