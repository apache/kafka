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
import org.apache.kafka.common.internals.KafkaFutureImpl;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ListTransactionsResultTest {
    private final KafkaFutureImpl<Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>>> future =
        new KafkaFutureImpl<>();
    private final ListTransactionsResult result = new ListTransactionsResult(future);

    @Test
    public void testAllFuturesFailIfLookupFails() {
        future.completeExceptionally(new KafkaException());
        assertFutureThrows(result.all(), KafkaException.class);
        assertFutureThrows(result.allByBrokerId(), KafkaException.class);
        assertFutureThrows(result.byBrokerId(), KafkaException.class);
    }

    @Test
    public void testAllFuturesSucceed() throws Exception {
        KafkaFutureImpl<Collection<TransactionListing>> future1 = new KafkaFutureImpl<>();
        KafkaFutureImpl<Collection<TransactionListing>> future2 = new KafkaFutureImpl<>();

        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures = new HashMap<>();
        brokerFutures.put(1, future1);
        brokerFutures.put(2, future2);

        future.complete(brokerFutures);

        List<TransactionListing> broker1Listings = asList(
            new TransactionListing("foo", 12345L, TransactionState.ONGOING),
            new TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT)
        );
        future1.complete(broker1Listings);

        List<TransactionListing> broker2Listings = singletonList(
            new TransactionListing("baz", 13579L, TransactionState.COMPLETE_COMMIT)
        );
        future2.complete(broker2Listings);

        Map<Integer, KafkaFuture<Collection<TransactionListing>>> resultBrokerFutures =
            result.byBrokerId().get();

        assertEquals(Set.of(1, 2), resultBrokerFutures.keySet());
        assertEquals(broker1Listings, resultBrokerFutures.get(1).get());
        assertEquals(broker2Listings, resultBrokerFutures.get(2).get());
        assertEquals(broker1Listings, result.allByBrokerId().get().get(1));
        assertEquals(broker2Listings, result.allByBrokerId().get().get(2));

        Set<TransactionListing> allExpected = new HashSet<>();
        allExpected.addAll(broker1Listings);
        allExpected.addAll(broker2Listings);

        assertEquals(allExpected, new HashSet<>(result.all().get()));
    }

    @Test
    public void testPartialFailure() throws Exception {
        KafkaFutureImpl<Collection<TransactionListing>> future1 = new KafkaFutureImpl<>();
        KafkaFutureImpl<Collection<TransactionListing>> future2 = new KafkaFutureImpl<>();

        Map<Integer, KafkaFutureImpl<Collection<TransactionListing>>> brokerFutures = new HashMap<>();
        brokerFutures.put(1, future1);
        brokerFutures.put(2, future2);

        future.complete(brokerFutures);

        List<TransactionListing> broker1Listings = asList(
            new TransactionListing("foo", 12345L, TransactionState.ONGOING),
            new TransactionListing("bar", 98765L, TransactionState.PREPARE_ABORT)
        );
        future1.complete(broker1Listings);
        future2.completeExceptionally(new KafkaException());

        Map<Integer, KafkaFuture<Collection<TransactionListing>>> resultBrokerFutures =
            result.byBrokerId().get();

        // Ensure that the future for broker 1 completes successfully
        assertEquals(Set.of(1, 2), resultBrokerFutures.keySet());
        assertEquals(broker1Listings, resultBrokerFutures.get(1).get());

        // Everything else should fail
        assertFutureThrows(result.all(), KafkaException.class);
        assertFutureThrows(result.allByBrokerId(), KafkaException.class);
        assertFutureThrows(resultBrokerFutures.get(2), KafkaException.class);
    }

}
