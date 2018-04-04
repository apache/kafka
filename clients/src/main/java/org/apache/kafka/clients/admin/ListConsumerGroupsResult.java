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
import org.apache.kafka.common.Node;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * The result of the {@link AdminClient#listConsumerGroups()} call.
 * <p>
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsResult {
    final Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>> futureMap;

    ListConsumerGroupsResult(Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>> futureMap) {
        this.futureMap = futureMap;
    }

    /**
     * Return a future which yields a map of consumer groups to ConsumerGroupListing objects.
     */
    public Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>> nodesToListings() {
        return futureMap;
    }

    /**
     * Return a future which yields a collection of ConsumerGroupListing objects.
     */
    public KafkaFuture<Collection<ConsumerGroupListing>> listings() {
        return
            KafkaFuture.allOf(futureMap.values().toArray(new KafkaFuture[0]))
                .thenApply(new KafkaFuture.Function<Void, Collection<ConsumerGroupListing>>() {
                    @Override
                    public Collection<ConsumerGroupListing> apply(Void aVoid) {
                        final Collection<ConsumerGroupListing> all = new HashSet<>();

                        for (KafkaFuture<Collection<ConsumerGroupListing>> future : futureMap.values()) {
                            try {
                                Collection<ConsumerGroupListing> listings = future.get();
                                all.addAll(listings);
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        return all;
                    }
                });
    }

    /**
     * Return a future which yields a collection of consumer groups.
     */
    public Set<Node> nodes() {
        return futureMap.keySet();
    }
}
