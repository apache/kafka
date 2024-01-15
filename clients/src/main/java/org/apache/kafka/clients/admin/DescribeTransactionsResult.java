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
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@InterfaceStability.Evolving
public class DescribeTransactionsResult {
    private final Map<CoordinatorKey, KafkaFuture<TransactionDescription>> futures;

    DescribeTransactionsResult(Map<CoordinatorKey, KafkaFuture<TransactionDescription>> futures) {
        this.futures = futures;
    }

    /**
     * Get the description of a specific transactional ID.
     *
     * @param transactionalId the transactional ID to describe
     * @return a future which completes when the transaction description of a particular
     *         transactional ID is available.
     * @throws IllegalArgumentException if the `transactionalId` was not included in the
     *         respective call to {@link Admin#describeTransactions(Collection, DescribeTransactionsOptions)}.
     */
    public KafkaFuture<TransactionDescription> description(String transactionalId) {
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        KafkaFuture<TransactionDescription> future = futures.get(key);
        if (future == null) {
            throw new IllegalArgumentException("TransactionalId " +
                "`" + transactionalId + "` was not included in the request");
        }
        return future;
    }
    /**
     * Get a future which returns a map of the transaction descriptions requested in the respective
     * call to {@link Admin#describeTransactions(Collection, DescribeTransactionsOptions)}.
     *
     * If the description fails on any of the transactional IDs in the request, then this future
     * will also fail.
     *
     * @return a future which either completes when all transaction descriptions complete or fails
     *         if any of the descriptions cannot be obtained
     */
    public KafkaFuture<Map<String, TransactionDescription>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]))
            .thenApply(nil -> {
                Map<String, TransactionDescription> results = new HashMap<>(futures.size());
                for (Map.Entry<CoordinatorKey, KafkaFuture<TransactionDescription>> entry : futures.entrySet()) {
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

}
