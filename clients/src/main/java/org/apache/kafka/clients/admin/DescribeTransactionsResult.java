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

import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.requests.FindCoordinatorRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@InterfaceStability.Evolving
public class DescribeTransactionsResult {
    private final Map<CoordinatorKey, KafkaFutureImpl<TransactionState>> futures;

    DescribeTransactionsResult(Map<CoordinatorKey, KafkaFutureImpl<TransactionState>> futures) {
        this.futures = futures;
    }

    public KafkaFuture<TransactionState> transactionalIdResult(String transactionalId) {
        CoordinatorKey key = buildKey(transactionalId);
        KafkaFuture<TransactionState> future = futures.get(key);
        if (future == null) {
            throw new IllegalArgumentException("TransactionalId " +
                "`" + transactionalId + "` was not included in the request");
        }
        return future;
    }

    private CoordinatorKey buildKey(String transactionalId) {
        return new CoordinatorKey(transactionalId, FindCoordinatorRequest.CoordinatorType.TRANSACTION);
    }

    public KafkaFuture<Map<String, TransactionState>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(nil -> {
                Map<String, TransactionState> results = new HashMap<>(futures.size());
                for (Map.Entry<CoordinatorKey, KafkaFutureImpl<TransactionState>> entry : futures.entrySet()) {
                    try {
                        results.put(entry.getKey().idValue, entry.getValue().get());
                    } catch (InterruptedException | ExecutionException e) {
                        // This should be unreachable, because allOf ensured that all the futures completed successfully.
                        throw new RuntimeException(e);
                    }
                }
                return results;
            });
    }

    public static class TransactionState {
        private final String state;
        private final long producerId;
        private final int producerEpoch;
        private final long transactionTimeoutMs;
        private final OptionalLong transactionStartTimeMs;
        private final Set<TopicPartition> topicPartitions;

        public TransactionState(
            String state,
            long producerId,
            int producerEpoch,
            long transactionTimeoutMs,
            OptionalLong transactionStartTimeMs,
            Set<TopicPartition> topicPartitions
        ) {
            this.state = state;
            this.producerId = producerId;
            this.producerEpoch = producerEpoch;
            this.transactionTimeoutMs = transactionTimeoutMs;
            this.transactionStartTimeMs = transactionStartTimeMs;
            this.topicPartitions = topicPartitions;
        }

        public String state() {
            return state;
        }

        public long producerId() {
            return producerId;
        }

        public int producerEpoch() {
            return producerEpoch;
        }

        public long transactionTimeoutMs() {
            return transactionTimeoutMs;
        }

        public OptionalLong transactionStartTimeMs() {
            return transactionStartTimeMs;
        }

        public Set<TopicPartition> topicPartitions() {
            return topicPartitions;
        }
    }
}
