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
    private final KafkaFuture<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future;

    ListTransactionsResult(KafkaFuture<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future) {
        this.future = future;
    }

    /**
     * Get all transaction listings. If any of the underlying requests fail, then the future
     * returned from this method will also fail with the first encountered error.
     *
     * @return A future containing the collection of transaction listings. The future completes
     *         when all transaction listings are available and fails after any non-retriable error.
     */
    public KafkaFuture<Collection<TransactionListing>> all() {
        return allByBrokerId().thenApply(map -> {
            List<TransactionListing> allListings = new ArrayList<>();
            for (Collection<TransactionListing> listings : map.values()) {
                allListings.addAll(listings);
            }
            return allListings;
        });
    }

    /**
     * Get a future which returns a map containing the underlying listing future for each broker
     * in the cluster. This is useful, for example, if a partial listing of transactions is
     * sufficient, or if you want more granular error details.
     *
     * @return A future containing a map of futures by broker which complete individually when
     *         their respective transaction listings are available. The top-level future returned
     *         from this method may fail if the admin client is unable to lookup the available
     *         brokers in the cluster.
     */
    public KafkaFuture<Map<Integer, KafkaFuture<Collection<TransactionListing>>>> byBrokerId() {
        KafkaFutureImpl<Map<Integer, KafkaFuture<Collection<TransactionListing>>>> result = new KafkaFutureImpl<>();
        future.whenComplete((brokerFutures, exception) -> {
            if (brokerFutures != null) {
                Map<Integer, KafkaFuture<Collection<TransactionListing>>> brokerFuturesCopy =
                    new HashMap<>(brokerFutures.size());
                brokerFuturesCopy.putAll(brokerFutures);
                result.complete(brokerFuturesCopy);
            } else {
                result.completeExceptionally(exception);
            }
        });
        return result;
    }

    /**
     * Get all transaction listings in a map which is keyed by the ID of respective broker
     * that is currently managing them. If any of the underlying requests fail, then the future
     * returned from this method will also fail with the first encountered error.
     *
     * @return A future containing a map from the broker ID to the transactions hosted by that
     *         broker respectively. This future completes when all transaction listings are
     *         available and fails after any non-retriable error.
     */
    public KafkaFuture<Map<Integer, Collection<TransactionListing>>> allByBrokerId() {
        KafkaFutureImpl<Map<Integer, Collection<TransactionListing>>> allFuture = new KafkaFutureImpl<>();
        Map<Integer, Collection<TransactionListing>> allListingsMap = new HashMap<>();

        future.whenComplete((map, topLevelException) -> {
            if (topLevelException != null) {
                allFuture.completeExceptionally(topLevelException);
                return;
            }

            Set<Integer> remainingResponses = new HashSet<>(map.keySet());
            map.forEach((brokerId, future) -> {
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
            });
        });

        return allFuture;
    }

}
