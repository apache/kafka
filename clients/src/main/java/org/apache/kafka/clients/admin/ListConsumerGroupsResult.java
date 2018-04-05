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
import java.util.Map;
import java.util.Set;

/**
 * The result of the {@link AdminClient#listConsumerGroups()} call.
 * <p>
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsResult {
    final KafkaFuture<Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>>> futureMap;

    ListConsumerGroupsResult(KafkaFuture<Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>>> futureMap) {
        this.futureMap = futureMap;
    }

    /**
     * Return a future which yields a map of consumer groups to ConsumerGroupListing objects.
     */
    public KafkaFuture<Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>>> nodesToListings() {
        return futureMap;
    }

    /**
     * Return a future which yields a collection of ConsumerGroupListing objects.
     */
    public KafkaFuture<Collection<KafkaFuture<Collection<ConsumerGroupListing>>>> listings() {
        return futureMap.thenApply(
            new KafkaFuture.Function<Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>>, Collection<KafkaFuture<Collection<ConsumerGroupListing>>>>() {

                @Override
                public Collection<KafkaFuture<Collection<ConsumerGroupListing>>> apply(Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>> futureMap) {
                    return futureMap.values();
                }
            }
        );
    }

    /**
     * Return a future which yields a collection of consumer groups.
     */
    public KafkaFuture<Set<Node>> nodes() {
        return futureMap.thenApply(
            new KafkaFuture.Function<Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>>, Set<Node>>() {
                @Override
                public Set<Node> apply(Map<Node, KafkaFuture<Collection<ConsumerGroupListing>>> futureMap) {
                    return futureMap.keySet();
                }
            }
        );
    }
}
