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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.MetricName;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K, V> extends Closeable {
    
    /**
     * @see KafkaConsumer#subscriptions()
     */
    public Set<TopicPartition> subscriptions();

    /**
     * @see KafkaConsumer#subscribe(String...)
     */
    public void subscribe(String... topics);

    /**
     * @see KafkaConsumer#subscribe(TopicPartition...)
     */
    public void subscribe(TopicPartition... partitions);

    /**
     * @see KafkaConsumer#unsubscribe(String...)
     */
    public void unsubscribe(String... topics);

    /**
     * @see KafkaConsumer#unsubscribe(TopicPartition...)
     */
    public void unsubscribe(TopicPartition... partitions);

    /**
     * @see KafkaConsumer#poll(long)
     */
    public ConsumerRecords<K, V> poll(long timeout);

    /**
     * @see KafkaConsumer#commit(CommitType)
     */
    public void commit(CommitType commitType);

    /**
     * @see KafkaConsumer#commit(CommitType, ConsumerCommitCallback)
     */
    public void commit(CommitType commitType, ConsumerCommitCallback callback);

    /**
     * @see KafkaConsumer#commit(Map, CommitType)
     */
    public void commit(Map<TopicPartition, Long> offsets, CommitType commitType);

    /**
     * @see KafkaConsumer#commit(Map, CommitType, ConsumerCommitCallback)
     */
    public void commit(Map<TopicPartition, Long> offsets, CommitType commitType, ConsumerCommitCallback callback);

    /**
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    public void seek(TopicPartition partition, long offset);

    /**
     * @see KafkaConsumer#seekToBeginning(TopicPartition...)
     */
    public void seekToBeginning(TopicPartition... partitions);

    /**
     * @see KafkaConsumer#seekToEnd(TopicPartition...)
     */
    public void seekToEnd(TopicPartition... partitions);

    /**
     * @see KafkaConsumer#position(TopicPartition)
     */
    public long position(TopicPartition partition);

    /**
     * @see KafkaConsumer#committed(TopicPartition)
     */
    public long committed(TopicPartition partition);

    /**
     * @see KafkaConsumer#metrics()
     */
    public Map<MetricName, ? extends Metric> metrics();

    /**
     * @see KafkaConsumer#partitionsFor(String)
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see KafkaConsumer#listTopics()
     */
    public Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see KafkaConsumer#close()
     */
    public void close();

    /**
     * @see KafkaConsumer#wakeup()
     */
    public void wakeup();

}
