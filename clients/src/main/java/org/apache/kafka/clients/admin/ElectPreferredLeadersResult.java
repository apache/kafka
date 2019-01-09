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

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ApiError;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The result of {@link AdminClient#electPreferredLeaders(Collection, ElectPreferredLeadersOptions)}
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ElectPreferredLeadersResult {

    private final KafkaFutureImpl<Map<TopicPartition, ApiError>> electionFuture;
    private final Set<TopicPartition> partitions;

    ElectPreferredLeadersResult(KafkaFutureImpl<Map<TopicPartition, ApiError>> electionFuture, Set<TopicPartition> partitions) {
        this.electionFuture = electionFuture;
        this.partitions = partitions;
    }

    /**
     * Get the result of the election for the given {@code partition}.
     * If there was not an election triggered for the given {@code partition}, the
     * returned future will complete with an error.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        electionFuture.whenComplete(new KafkaFuture.BiConsumer<Map<TopicPartition, ApiError>, Throwable>() {
            @Override
            public void accept(Map<TopicPartition, ApiError> topicPartitions, Throwable throwable) {
                if (throwable != null) {
                    result.completeExceptionally(throwable);
                } else if (!topicPartitions.containsKey(partition)) {
                    result.completeExceptionally(new UnknownTopicOrPartitionException(
                            "Preferred leader election for partition \"" + partition +
                                    "\" was not attempted"));
                } else {
                    if (partitions == null && topicPartitions.isEmpty()) {
                        result.completeExceptionally(Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
                    }
                    ApiException exception = topicPartitions.get(partition).exception();
                    if (exception == null) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(exception);
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
     * {@link AdminClient#electPreferredLeaders(Collection)} is called
     * with a null {@code partitions} argument.</p>
     */
    public KafkaFuture<Set<TopicPartition>> partitions() {
        if (partitions != null) {
            return KafkaFutureImpl.completedFuture(this.partitions);
        } else {
            final KafkaFutureImpl<Set<TopicPartition>> result = new KafkaFutureImpl<>();
            electionFuture.whenComplete(new KafkaFuture.BiConsumer<Map<TopicPartition, ApiError>, Throwable>() {
                @Override
                public void accept(Map<TopicPartition, ApiError> topicPartitions, Throwable throwable) {
                    if (throwable != null) {
                        result.completeExceptionally(throwable);
                    } else if (topicPartitions.isEmpty()) {
                        result.completeExceptionally(Errors.CLUSTER_AUTHORIZATION_FAILED.exception());
                    } else {
                        for (ApiError apiError : topicPartitions.values()) {
                            if (apiError.isFailure()) {
                                result.completeExceptionally(apiError.exception());
                            }
                        }
                        result.complete(topicPartitions.keySet());
                    }
                }
            });
            return result;
        }
    }

    /**
     * Return a future which succeeds if all the topic elections succeed.
     */
    public KafkaFuture<Void> all() {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();
        electionFuture.thenApply(new KafkaFuture.Function<Map<TopicPartition, ApiError>, Void>() {
            @Override
            public Void apply(Map<TopicPartition, ApiError> topicPartitions) {
                for (ApiError apiError : topicPartitions.values()) {
                    if (apiError.isFailure()) {
                        result.completeExceptionally(apiError.exception());
                        return null;
                    }
                }
                result.complete(null);
                return null;
            }
        });
        return result;
    }
}
