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
import java.util.Set;

/**
 * The result of the {@link Admin#deleteShareGroupOffsets(String, Set, DeleteShareGroupOffsetsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DeleteShareGroupOffsetsResult {

    private final KafkaFuture<Map<TopicPartition, ApiException>> future;

    DeleteShareGroupOffsetsResult(KafkaFuture<Map<TopicPartition, ApiException>> future) {
        this.future = future;
    }

    /**
     * Return a future which succeeds only if all the deletions succeed.
     */
    public KafkaFuture<Void> all() {
        return this.future.thenApply(topicPartitionErrorsMap ->  {
            for (ApiException error : topicPartitionErrorsMap.values()) {
                if (error != null) {
                    throw error;
                }
            }
            return null;
        });
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
                    "Delete offset for partition \"" + partition + "\" was not attempted"));
            } else {
                final ApiException error = topicPartitions.get(partition);
                if (error == null) {
                    result.complete(null);
                } else {
                    result.completeExceptionally(error);
                }
            }
        });

        return result;
    }
}
