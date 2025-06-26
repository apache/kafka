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
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Map;

/**
 * The result of the {@link Admin#alterShareGroupOffsets(String, Map, AlterShareGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class AlterShareGroupOffsetsResult {

    private final KafkaFuture<Map<TopicPartition, ApiException>> future;

    AlterShareGroupOffsetsResult(KafkaFuture<Map<TopicPartition, ApiException>> future) {
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
                final ApiException exception = topicPartitions.get(partition);
                if (exception == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(exception);
                }
            }
        });

        return result;
    }

    /**
     * Return a future which succeeds if all the alter offsets succeed.
     */
    public KafkaFuture<Void> all() {
        return this.future.thenApply(topicPartitionErrorsMap -> {
            for (ApiException exception : topicPartitionErrorsMap.values()) {
                if (exception != null) {
                    throw exception;
                }
            }
            return null;
        });
    }
}
