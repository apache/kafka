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

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.MetricName;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K,V> extends Closeable {

    /**
     * Incrementally subscribe to the given list of topics. This API is mutually exclusive to 
     * {@link #subscribe(TopicPartition...) subscribe(partitions)} 
     * @param topics A variable list of topics that the consumer subscribes to
     */ 
    public void subscribe(String...topics);

    /**
     * Incrementally subscribes to a specific topic and partition. This API is mutually exclusive to 
     * {@link #subscribe(String...) subscribe(topics)}
     * @param partitions Partitions to subscribe to
     */ 
    public void subscribe(TopicPartition... partitions);

    /**
     * Unsubscribe from the specific topics. Messages for this topic will not be returned from the next {@link #poll(long) poll()}
     * onwards. This should be used in conjunction with {@link #subscribe(String...) subscribe(topics)}. It is an error to
     * unsubscribe from a topic that was never subscribed to using {@link #subscribe(String...) subscribe(topics)} 
     * @param topics Topics to unsubscribe from
     */
    public void unsubscribe(String... topics);

    /**
     * Unsubscribe from the specific topic partitions. Messages for these partitions will not be returned from the next 
     * {@link #poll(long) poll()} onwards. This should be used in conjunction with 
     * {@link #subscribe(TopicPartition...) subscribe(topic, partitions)}. It is an error to
     * unsubscribe from a partition that was never subscribed to using {@link #subscribe(TopicPartition...) subscribe(partitions)}
     * @param partitions Partitions to unsubscribe from
     */
    public void unsubscribe(TopicPartition... partitions);
    
    /**
     * Fetches data for the subscribed list of topics and partitions
     * @param timeout  The time, in milliseconds, spent waiting in poll if data is not available. If 0, waits indefinitely. Must not be negative
     * @return Map of topic to records for the subscribed topics and partitions as soon as data is available for a topic partition. Availability
     *         of data is controlled by {@link ConsumerConfig#FETCH_MIN_BYTES_CONFIG} and {@link ConsumerConfig#FETCH_MAX_WAIT_MS_CONFIG}.
     *         If no data is available for timeout ms, returns an empty list
     */
    public Map<String, ConsumerRecords<K,V>> poll(long timeout);

    /**
     * Commits offsets returned on the last {@link #poll(long) poll()} for the subscribed list of topics and partitions.
     * @param sync If true, the commit should block until the consumer receives an acknowledgment 
     * @return An {@link OffsetMetadata} object that contains the partition, offset and a corresponding error code. Returns null
     * if the sync flag is set to false 
     */
    public OffsetMetadata commit(boolean sync);

    /**
     * Commits the specified offsets for the specified list of topics and partitions to Kafka.
     * @param offsets The map of offsets to commit for the given topic partitions
     * @param sync If true, commit will block until the consumer receives an acknowledgment 
     * @return An {@link OffsetMetadata} object that contains the partition, offset and a corresponding error code. Returns null
     * if the sync flag is set to false. 
     */
    public OffsetMetadata commit(Map<TopicPartition, Long> offsets, boolean sync);
    
    /**
     * Overrides the fetch positions that the consumer will use on the next fetch request. If the consumer subscribes to a list of topics
     * using {@link #subscribe(String...) subscribe(topics)}, an exception will be thrown if the specified topic partition is not owned by
     * the consumer.  
     * @param offsets The map of fetch positions per topic and partition
     */
    public void seek(Map<TopicPartition, Long> offsets);

    /**
     * Returns the fetch position of the <i>next message</i> for the specified topic partition to be used on the next {@link #poll(long) poll()}
     * @param partitions Partitions for which the fetch position will be returned
     * @return The position from which data will be fetched for the specified partition on the next {@link #poll(long) poll()}
     */
    public Map<TopicPartition, Long> position(Collection<TopicPartition> partitions);
    
    /**
     * Fetches the last committed offsets for the input list of partitions 
     * @param partitions The list of partitions to return the last committed offset for
     * @return  The list of offsets for the specified list of partitions
     */
    public Map<TopicPartition, Long> committed(Collection<TopicPartition> partitions);
    
    /**
     * Fetches offsets before a certain timestamp
     * @param timestamp The unix timestamp. Value -1 indicates earliest available timestamp. Value -2 indicates latest available timestamp. 
     * @param partitions The list of partitions for which the offsets are returned
     * @return The offsets for messages that were written to the server before the specified timestamp.
     */
    public Map<TopicPartition, Long> offsetsBeforeTime(long timestamp, Collection<TopicPartition> partitions);

    /**
     * Return a map of metrics maintained by the consumer
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * Close this consumer
     */
    public void close();

}
