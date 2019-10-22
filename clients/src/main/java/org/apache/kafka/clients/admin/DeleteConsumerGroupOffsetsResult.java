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

import java.util.Set;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Map;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.protocol.Errors;

/**
 * The result of the {@link Admin#deleteConsumerGroupOffsets(String, Set)} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DeleteConsumerGroupOffsetsResult {
    private final KafkaFuture<Map<TopicPartition, Errors>> future;
    private final Set<TopicPartition> partitions;


    DeleteConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, Errors>> future, Set<TopicPartition> partitions) {
        this.future = future;
        this.partitions = partitions;
    }

    /**
     * Return a future which can be used to check the result for a given partition.
     */
    public KafkaFuture<Void> partitionResult(final TopicPartition partition) {
        if (!partitions.contains(partition)) {
            throw new IllegalArgumentException("Partition " + partition + " was not included in the original request");
        }
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        this.future.whenComplete((topicPartitions, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else if (!maybeCompleteExceptionally(topicPartitions, partition, result)) {
                result.complete(null);
            }
        });
        return result;
    }

    /**
     * Return a future which succeeds only if all the deletions succeed.
     * If not, the first partition error shall be returned.
     */
    public KafkaFuture<Void> all() {
        final KafkaFutureImpl<Void> result = new KafkaFutureImpl<>();

        this.future.whenComplete((topicPartitions, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                for (TopicPartition partition : partitions) {
                    if (maybeCompleteExceptionally(topicPartitions, partition, result)) {
                        return;
                    }
                }
                result.complete(null);
            }
        });
        return result;
    }

    private boolean maybeCompleteExceptionally(Map<TopicPartition, Errors> partitionLevelErrors,
                                               TopicPartition partition,
                                               KafkaFutureImpl<Void> result) {
        Throwable exception = KafkaAdminClient.getSubLevelError(partitionLevelErrors, partition,
            "Offset deletion result for partition \"" + partition + "\" was not included in the response");
        if (exception != null) {
            result.completeExceptionally(exception);
            return true;
        } else {
            return false;
        }
    }
}
