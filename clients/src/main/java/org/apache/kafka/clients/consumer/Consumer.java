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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * @see KafkaConsumer
 * @see MockConsumer
 */
public interface Consumer<K, V> extends Closeable {

    /**
     * @see KafkaConsumer#assignment()
     */
    Set<TopicPartition> assignment();

    /**
     * @see KafkaConsumer#subscription()
     */
    Set<String> subscription();

    /**
     * @see KafkaConsumer#subscribe(Collection)
     */
    void subscribe(Collection<String> topics);

    /**
     * @see KafkaConsumer#subscribe(Collection, ConsumerRebalanceListener)
     */
    void subscribe(Collection<String> topics, ConsumerRebalanceListener callback);

    /**
     * @see KafkaConsumer#assign(Collection)
     */
    void assign(Collection<TopicPartition> partitions);

    /**
    * @see KafkaConsumer#subscribe(Pattern, ConsumerRebalanceListener)
    */
    void subscribe(Pattern pattern, ConsumerRebalanceListener callback);

    /**
    * @see KafkaConsumer#subscribe(Pattern)
    */
    void subscribe(Pattern pattern);

    /**
     * @see KafkaConsumer#unsubscribe()
     */
    void unsubscribe();

    /**
     * @see KafkaConsumer#poll(long)
     */
    @Deprecated
    ConsumerRecords<K, V> poll(long timeout);

    /**
     * @see KafkaConsumer#poll(Duration)
     */
    ConsumerRecords<K, V> poll(Duration timeout);

    /**
     * @see KafkaConsumer#commitSync()
     */
    void commitSync();

    /**
     * @see KafkaConsumer#commitSync(Duration)
     */
    void commitSync(Duration timeout);

    /**
     * @see KafkaConsumer#commitSync(Map)
     */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);

    /**
     * @see KafkaConsumer#commitSync(Map, Duration)
     */
    void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets, final Duration timeout);
    /**
     * @see KafkaConsumer#commitAsync()
     */
    void commitAsync();

    /**
     * @see KafkaConsumer#commitAsync(OffsetCommitCallback)
     */
    void commitAsync(OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback)
     */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback);

    /**
     * @see KafkaConsumer#seek(TopicPartition, long)
     */
    void seek(TopicPartition partition, long offset);

    /**
     * @see KafkaConsumer#seek(TopicPartition, OffsetAndMetadata)
     */
    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);

    /**
     * @see KafkaConsumer#seekToBeginning(Collection)
     */
    void seekToBeginning(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#seekToEnd(Collection)
     */
    void seekToEnd(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#position(TopicPartition)
     */
    long position(TopicPartition partition);
    
    /**
     * @see KafkaConsumer#position(TopicPartition, Duration)
     */
    long position(TopicPartition partition, final Duration timeout);

    /**
     * @see KafkaConsumer#committed(TopicPartition)
     */
    OffsetAndMetadata committed(TopicPartition partition);

    /**
     * @see KafkaConsumer#committed(TopicPartition, Duration)
     */
    OffsetAndMetadata committed(TopicPartition partition, final Duration timeout);

    /**
     * @see KafkaConsumer#metrics()
     */
    Map<MetricName, ? extends Metric> metrics();

    /**
     * @see KafkaConsumer#partitionsFor(String)
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * @see KafkaConsumer#partitionsFor(String, Duration)
     */
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);

    /**
     * @see KafkaConsumer#listTopics()
     */
    Map<String, List<PartitionInfo>> listTopics();

    /**
     * @see KafkaConsumer#listTopics(Duration)
     */
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);

    /**
     * @see KafkaConsumer#paused()
     */
    Set<TopicPartition> paused();

    /**
     * @see KafkaConsumer#pause(Collection)
     */
    void pause(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#resume(Collection)
     */
    void resume(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#offsetsForTimes(Map)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch);

    /**
     * @see KafkaConsumer#offsetsForTimes(Map, Duration)
     */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout);

    /**
     * @see KafkaConsumer#beginningOffsets(Collection)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#beginningOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * @see KafkaConsumer#endOffsets(Collection)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);

    /**
     * @see KafkaConsumer#endOffsets(Collection, Duration)
     */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout);

    /**
     * @see KafkaConsumer#close()
     */
    void close();

    /**
     * @see KafkaConsumer#close(long, TimeUnit)
     */
    @Deprecated
    void close(long timeout, TimeUnit unit);

    /**
     * @see KafkaConsumer#close(Duration)
     */
    void close(Duration timeout);

    /**
     * @see KafkaConsumer#wakeup()
     */
    void wakeup();

}
