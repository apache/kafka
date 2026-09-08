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

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceAudience;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A restricted view of a {@link Consumer} passed to {@link ConsumerRebalanceListener} callback
 * methods during a partition rebalance. This interface provides compile-time enforcement of safe
 * consumer operations during rebalance callbacks, replacing the previous pattern of capturing a
 * {@code Consumer} reference externally (e.g. via constructor injection), which gave callbacks
 * access to dangerous operations like {@code poll()}, {@code close()}, and {@code subscribe()}
 * that could corrupt consumer state mid-rebalance.
 *
 * <h3>Lifecycle</h3>
 *
 * <p>A {@code RebalanceConsumer} instance is created by the consumer for each rebalance callback
 * invocation and is only valid for the duration of that callback. Storing a reference to this
 * object and using it after the callback returns will result in an {@link IllegalStateException}.
 *
 * <h3>Permitted Operations</h3>
 *
 * <p>This interface exposes only operations that are safe and meaningful during partition
 * rebalancing:
 *
 * <ul>
 *   <li><b>Offset management:</b> {@code commitSync}, {@code commitAsync},
 *       {@code committed}, {@code position}</li>
 *   <li><b>Seek:</b> {@code seek}, {@code seekToBeginning},
 *       {@code seekToEnd}</li>
 *   <li><b>Partition state:</b> {@code assignment}, {@code paused},
 *       {@code pause}, {@code resume}</li>
 *   <li><b>Read-only queries:</b> {@code partitionsFor}, {@code listTopics},
 *       {@code offsetsForTimes}, {@code beginningOffsets},
 *       {@code endOffsets}, {@code clientInstanceId}, {@code currentLag},
 *       {@code groupMetadata}</li>
 * </ul>
 *
 * <h3>Excluded Operations</h3>
 *
 * <p>Operations that are dangerous or inapplicable during a rebalance are deliberately omitted
 * from this interface:
 *
 * <ul>
 *   <li>{@code poll()} - would cause re-entrant polling</li>
 *   <li>{@code Consumer.close()} - would terminate the consumer mid-rebalance</li>
 *   <li>{@code subscribe()}, {@code unsubscribe()} - would corrupt
 *       subscription state during rebalance</li>
 *   <li>{@code registerMetricForSubscription()}, {@code unregisterMetricForSubscription()} - metric
 *       registration does not belong during rebalance events</li>
 *   <li>{@code assign()} - conflicts with group-managed assignment</li>
 *   <li>{@code wakeup()} - would interrupt the rebalance</li>
 *   <li>{@code enforceRebalance()} - would trigger re-entrant rebalance</li>
 * </ul>
 *
 * @see ConsumerRebalanceListener
 */
@InterfaceAudience.Public
public interface RebalanceConsumer {

    // --- Offset management ---

    /** @see KafkaConsumer#commitSync() */
    void commitSync();
    /** @see KafkaConsumer#commitSync(Duration) */
    void commitSync(Duration timeout);
    /** @see KafkaConsumer#commitSync(Map) */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets);
    /** @see KafkaConsumer#commitSync(Map, Duration) */
    void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout);
    /** @see KafkaConsumer#commitAsync() */
    void commitAsync();
    /** @see KafkaConsumer#commitAsync(OffsetCommitCallback) */
    void commitAsync(OffsetCommitCallback callback);
    /** @see KafkaConsumer#commitAsync(Map, OffsetCommitCallback) */
    void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets,
                     OffsetCommitCallback callback);

    /** @see KafkaConsumer#committed(Set) */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions);
    /** @see KafkaConsumer#committed(Set, Duration) */
    Map<TopicPartition, OffsetAndMetadata> committed(Set<TopicPartition> partitions,
                                                     Duration timeout);
    /** @see KafkaConsumer#position(TopicPartition) */
    long position(TopicPartition partition);
    /** @see KafkaConsumer#position(TopicPartition, Duration) */
    long position(TopicPartition partition, Duration timeout);

    // --- Seek ---

    /** @see KafkaConsumer#seek(TopicPartition, long) */
    void seek(TopicPartition partition, long offset);
    /** @see KafkaConsumer#seek(TopicPartition, OffsetAndMetadata) */
    void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata);
    /** @see KafkaConsumer#seekToBeginning(Collection) */
    void seekToBeginning(Collection<TopicPartition> partitions);
    /** @see KafkaConsumer#seekToEnd(Collection) */
    void seekToEnd(Collection<TopicPartition> partitions);

    // --- Partition state ---

    /** @see KafkaConsumer#assignment() */
    Set<TopicPartition> assignment();
    /** @see KafkaConsumer#pause(Collection) */
    void pause(Collection<TopicPartition> partitions);
    /** @see KafkaConsumer#resume(Collection) */
    void resume(Collection<TopicPartition> partitions);
    /** @see KafkaConsumer#paused() */
    Set<TopicPartition> paused();

    // --- Read-only queries ---

    /** @see KafkaConsumer#clientInstanceId(Duration) */
    Uuid clientInstanceId(Duration timeout);
    /** @see KafkaConsumer#beginningOffsets(Collection) */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions);
    /** @see KafkaConsumer#beginningOffsets(Collection, Duration) */
    Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions,
                                               Duration timeout);
    /** @see KafkaConsumer#endOffsets(Collection) */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions);
    /** @see KafkaConsumer#endOffsets(Collection, Duration) */
    Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions,
                                         Duration timeout);
    /** @see KafkaConsumer#offsetsForTimes(Map) */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch);
    /** @see KafkaConsumer#offsetsForTimes(Map, Duration) */
    Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
            Map<TopicPartition, Long> timestampsToSearch, Duration timeout);
    /** @see KafkaConsumer#partitionsFor(String) */
    List<PartitionInfo> partitionsFor(String topic);
    /** @see KafkaConsumer#partitionsFor(String, Duration) */
    List<PartitionInfo> partitionsFor(String topic, Duration timeout);
    /** @see KafkaConsumer#listTopics() */
    Map<String, List<PartitionInfo>> listTopics();
    /** @see KafkaConsumer#listTopics(Duration) */
    Map<String, List<PartitionInfo>> listTopics(Duration timeout);
    /** @see KafkaConsumer#currentLag(TopicPartition) */
    OptionalLong currentLag(TopicPartition topicPartition);
    /** @see KafkaConsumer#groupMetadata() */
    ConsumerGroupMetadata groupMetadata();
}
