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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;

/**
 * The result of the {@link AdminClient#alterConsumerGroupOffsets(String, Map)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class AlterConsumerGroupOffsetsResult {

    private final KafkaFuture<Map<TopicPartition, Errors>> future;

    AlterConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, Errors>> future) {
        this.future = future;
    }

    /**
     * Return a future which can be used to check the result for a given partition.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        this.future.whenComplete((topicPartitions, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else if (!topicPartitions.containsKey(partition)) {
                result.completeExceptionally(new IllegalArgumentException(
                    "Alter offset for partition \"" + partition + "\" was not attempted"));
            } else {
                final Errors error = topicPartitions.get(partition);
                if (error == Errors.NONE) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(error.exception());
                }
            }
        });

        return result;
    }

    /**
     * Return a future which succeeds if all the alter offsets succeed.
     */
    public KafkaFuture<Void> all() {
        return this.future.thenApply(topicPartitionErrorsMap ->  {
            List<TopicPartition> partitionsFailed = topicPartitionErrorsMap.entrySet()
                .stream()
                .filter(e -> e.getValue() != Errors.NONE)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            for (Errors error : topicPartitionErrorsMap.values()) {
                if (error != Errors.NONE) {
                    throw error.exception(
                        "Failed altering consumer group offsets for the following partitions: " + partitionsFailed);
                }
            }
            return null;
        });
    }
}
