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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link Admin#listTransactions()} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListTransactionsResult {
    private final KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future;

    ListTransactionsResult(KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future) {
        this.future = future;
    }

    public KafkaFuture<Collection<TransactionListing>> all() {
        return allByBrokerId().thenApply(map -> {
            List<TransactionListing> allListings = new ArrayList<>();
            for (Collection<TransactionListing> listings : map.values()) {
                allListings.addAll(listings);
            }
            return allListings;
        });
    }

    public KafkaFuture<Set<Integer>> brokerIds() {
        return future.thenApply(map -> new HashSet<>(map.keySet()));
    }

    public KafkaFuture<Map<Integer, Collection<TransactionListing>>> allByBrokerId() {
        KafkaFutureImpl<Map<Integer, Collection<TransactionListing>>> allFuture = new KafkaFutureImpl<>();
        Map<Integer, Collection<TransactionListing>> allListingsMap = new HashMap<>();

        future.whenComplete((map, topLevelException) -> {
            if (topLevelException != null) {
                allFuture.completeExceptionally(topLevelException);
                return;
            }

            Set<Integer> remainingResponses = new HashSet<>(map.keySet());
            for (Map.Entry<Integer, KafkaFutureImpl<Collection<TransactionListing>>> entry : map.entrySet()) {
                Integer brokerId = entry.getKey();
                KafkaFutureImpl<Collection<TransactionListing>> future = entry.getValue();
                future.whenComplete((listings, brokerException) -> {

                    if (brokerException != null) {
                        allFuture.completeExceptionally(brokerException);
                    } else if (!allFuture.isDone()) {
                        allListingsMap.put(brokerId, listings);
                        remainingResponses.remove(brokerId);

                        if (remainingResponses.isEmpty()) {
                            allFuture.complete(allListingsMap);
                        }
                    }
                });
            }
        });

        return allFuture;
    }

    public KafkaFuture<Collection<TransactionListing>> byBrokerId(Integer brokerId) {
        KafkaFutureImpl<Collection<TransactionListing>> resultFuture = new KafkaFutureImpl<>();
        future.whenComplete((map, exception) -> {
            if (exception != null) {
                resultFuture.completeExceptionally(exception);
            } else {
                KafkaFutureImpl<Collection<TransactionListing>> brokerFuture = map.get(brokerId);
                if (brokerFuture == null) {
                    resultFuture.completeExceptionally(new KafkaException("ListTransactions result " +
                        "did not include listings from broker " + brokerId + ". The included listings " +
                        "were from brokers " + map.keySet()));
                } else {
                    brokerFuture.whenComplete((listings, brokerException) -> {
                        if (brokerException != null) {
                            resultFuture.completeExceptionally(brokerException);
                        } else {
                            resultFuture.complete(listings);
                        }
                    });
                }
            }
        });
        return resultFuture;
    }

}
