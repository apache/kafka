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

package org.apache.kafka.clients.admin;


import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;

/**
 * The result of {@link Admin#electPreferredLeaders(Collection, ElectPreferredLeadersOptions)}
 *
 * The API of this class is evolving, see {@link Admin} for details.
 *
 * @deprecated Since 2.4.0. Use {@link Admin#electLeaders(ElectionType, Set, ElectLeadersOptions)}.
 */
@InterfaceStability.Evolving
@Deprecated
public class ElectPreferredLeadersResult {
    private final ElectLeadersResult electionResult;

    ElectPreferredLeadersResult(ElectLeadersResult electionResult) {
        this.electionResult = electionResult;
    }

    /**
     * Get the result of the election for the given {@code partition}.
     * If there was not an election triggered for the given {@code partition}, the
     * returned future will complete with an error.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        electionResult.partitions().whenComplete(
                new KafkaFuture.BiConsumer<Map<TopicPartition, Optional<Throwable>>, Throwable>() {
                    @Override
                    public void accept(Map<TopicPartition, Optional<Throwable>> topicPartitions, Throwable throwable) {
                        if (throwable != null) {
                            result.completeExceptionally(throwable);
                        } else if (!topicPartitions.containsKey(partition)) {
                            result.completeExceptionally(new UnknownTopicOrPartitionException(
                                        "Preferred leader election for partition \"" + partition +
                                        "\" was not attempted"));
                        } else {
                            Optional<Throwable> exception = topicPartitions.get(partition);
                            if (exception.isPresent()) {
                                result.completeExceptionally(exception.get());
                            } else {
                                result.complete(null);
                            }
                        }
                    }
                });

        return result;
    }

    /**
     * <p>Get a future for the topic partitions for which a leader election
     * was attempted. A partition will be present in this result if
     * an election was attempted even if the election was not successful.</p>
     *
     * <p>This method is provided to discover the partitions attempted when
     * {@link Admin#electPreferredLeaders(Collection)} is called
     * with a null {@code partitions} argument.</p>
     */
    public KafkaFuture<Set<TopicPartition>> partitions() {
        final KafkaFutureImpl<Set<TopicPartition>> result = new KafkaFutureImpl<>();

        electionResult.partitions().whenComplete(
                new KafkaFuture.BiConsumer<Map<TopicPartition, Optional<Throwable>>, Throwable>() {
                    @Override
                    public void accept(Map<TopicPartition, Optional<Throwable>> topicPartitions, Throwable throwable) {
                        if (throwable != null) {
                            result.completeExceptionally(throwable);
                        } else {
                            result.complete(topicPartitions.keySet());
                        }
                    }
                });

        return result;
    }

    /**
     * Return a future which succeeds if all the topic elections succeed.
     */
    public KafkaFuture<Void> all() {
        return electionResult.all();
    }
}
