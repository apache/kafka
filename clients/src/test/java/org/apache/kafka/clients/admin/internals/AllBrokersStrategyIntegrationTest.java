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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AllBrokersStrategyIntegrationTest {
    private static final long TIMEOUT_MS = 5000;
    private static final long RETRY_BACKOFF_MS = 100;

    private final LogContext logContext = new LogContext();
    private final MockTime time = new MockTime();

    private AdminApiDriver<AllBrokersStrategy.BrokerKey, Integer> buildDriver(
        AllBrokersStrategy.AllBrokersFuture<Integer> result
    ) {
        return new AdminApiDriver<>(
            new MockApiHandler(),
            result,
            time.milliseconds() + TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MS,
            logContext
        );
    }

    @Test
    public void testFatalLookupError() {
        AllBrokersStrategy.AllBrokersFuture<Integer> result = new AllBrokersStrategy.AllBrokersFuture<>();
        AdminApiDriver<AllBrokersStrategy.BrokerKey, Integer> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> spec = requestSpecs.get(0);
        assertEquals(AllBrokersStrategy.LOOKUP_KEYS, spec.keys);

        driver.onFailure(time.milliseconds(), spec, new UnknownServerException());
        assertTrue(result.all().isDone());
        TestUtils.assertFutureThrows(result.all(), UnknownServerException.class);
        assertEquals(Collections.emptyList(), driver.poll());
    }

    @Test
    public void testRetryLookupAfterDisconnect() {
        AllBrokersStrategy.AllBrokersFuture<Integer> result = new AllBrokersStrategy.AllBrokersFuture<>();
        AdminApiDriver<AllBrokersStrategy.BrokerKey, Integer> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());

        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> spec = requestSpecs.get(0);
        assertEquals(AllBrokersStrategy.LOOKUP_KEYS, spec.keys);

        driver.onFailure(time.milliseconds(), spec, new DisconnectException());
        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> retrySpecs = driver.poll();
        assertEquals(1, retrySpecs.size());

        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> retrySpec = retrySpecs.get(0);
        assertEquals(AllBrokersStrategy.LOOKUP_KEYS, retrySpec.keys);
        assertEquals(time.milliseconds(), retrySpec.nextAllowedTryMs);
        assertEquals(Collections.emptyList(), driver.poll());
    }

    @Test
    public void testMultiBrokerCompletion() throws Exception {
        AllBrokersStrategy.AllBrokersFuture<Integer> result = new AllBrokersStrategy.AllBrokersFuture<>();
        AdminApiDriver<AllBrokersStrategy.BrokerKey, Integer> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> lookupSpecs = driver.poll();
        assertEquals(1, lookupSpecs.size());
        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> lookupSpec = lookupSpecs.get(0);

        Set<Integer> brokerIds = Utils.mkSet(1, 2);
        driver.onResponse(time.milliseconds(), lookupSpec, responseWithBrokers(brokerIds), Node.noNode());
        assertTrue(result.all().isDone());

        Map<Integer, KafkaFutureImpl<Integer>> brokerFutures = result.all().get();

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> requestSpecs = driver.poll();
        assertEquals(2, requestSpecs.size());

        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> requestSpec1 = requestSpecs.get(0);
        assertTrue(requestSpec1.scope.destinationBrokerId().isPresent());
        int brokerId1 = requestSpec1.scope.destinationBrokerId().getAsInt();
        assertTrue(brokerIds.contains(brokerId1));

        driver.onResponse(time.milliseconds(), requestSpec1, null, Node.noNode());
        KafkaFutureImpl<Integer> future1 = brokerFutures.get(brokerId1);
        assertTrue(future1.isDone());

        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> requestSpec2 = requestSpecs.get(1);
        assertTrue(requestSpec2.scope.destinationBrokerId().isPresent());
        int brokerId2 = requestSpec2.scope.destinationBrokerId().getAsInt();
        assertNotEquals(brokerId1, brokerId2);
        assertTrue(brokerIds.contains(brokerId2));

        driver.onResponse(time.milliseconds(), requestSpec2, null, Node.noNode());
        KafkaFutureImpl<Integer> future2 = brokerFutures.get(brokerId2);
        assertTrue(future2.isDone());
        assertEquals(Collections.emptyList(), driver.poll());
    }

    @Test
    public void testRetryFulfillmentAfterDisconnect() throws Exception {
        AllBrokersStrategy.AllBrokersFuture<Integer> result = new AllBrokersStrategy.AllBrokersFuture<>();
        AdminApiDriver<AllBrokersStrategy.BrokerKey, Integer> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> lookupSpecs = driver.poll();
        assertEquals(1, lookupSpecs.size());
        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> lookupSpec = lookupSpecs.get(0);

        int brokerId = 1;
        driver.onResponse(time.milliseconds(), lookupSpec, responseWithBrokers(Collections.singleton(brokerId)), Node.noNode());
        assertTrue(result.all().isDone());

        Map<Integer, KafkaFutureImpl<Integer>> brokerFutures = result.all().get();
        KafkaFutureImpl<Integer> future = brokerFutures.get(brokerId);
        assertFalse(future.isDone());

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());
        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> requestSpec = requestSpecs.get(0);

        driver.onFailure(time.milliseconds(), requestSpec, new DisconnectException());
        assertFalse(future.isDone());
        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> retrySpecs = driver.poll();
        assertEquals(1, retrySpecs.size());

        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> retrySpec = retrySpecs.get(0);
        assertEquals(time.milliseconds() + RETRY_BACKOFF_MS, retrySpec.nextAllowedTryMs);
        assertEquals(OptionalInt.of(brokerId), retrySpec.scope.destinationBrokerId());

        driver.onResponse(time.milliseconds(), retrySpec, null, new Node(brokerId, "host", 1234));
        assertTrue(future.isDone());
        assertEquals(brokerId, future.get());
        assertEquals(Collections.emptyList(), driver.poll());
    }

    @Test
    public void testFatalFulfillmentError() throws Exception {
        AllBrokersStrategy.AllBrokersFuture<Integer> result = new AllBrokersStrategy.AllBrokersFuture<>();
        AdminApiDriver<AllBrokersStrategy.BrokerKey, Integer> driver = buildDriver(result);

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> lookupSpecs = driver.poll();
        assertEquals(1, lookupSpecs.size());
        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> lookupSpec = lookupSpecs.get(0);

        int brokerId = 1;
        driver.onResponse(time.milliseconds(), lookupSpec, responseWithBrokers(Collections.singleton(brokerId)), Node.noNode());
        assertTrue(result.all().isDone());

        Map<Integer, KafkaFutureImpl<Integer>> brokerFutures = result.all().get();
        KafkaFutureImpl<Integer> future = brokerFutures.get(brokerId);
        assertFalse(future.isDone());

        List<AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey>> requestSpecs = driver.poll();
        assertEquals(1, requestSpecs.size());
        AdminApiDriver.RequestSpec<AllBrokersStrategy.BrokerKey> requestSpec = requestSpecs.get(0);

        driver.onFailure(time.milliseconds(), requestSpec, new UnknownServerException());
        assertTrue(future.isDone());
        TestUtils.assertFutureThrows(future, UnknownServerException.class);
        assertEquals(Collections.emptyList(), driver.poll());
    }

    private MetadataResponse responseWithBrokers(Set<Integer> brokerIds) {
        MetadataResponseData response = new MetadataResponseData();
        for (Integer brokerId : brokerIds) {
            response.brokers().add(new MetadataResponseData.MetadataResponseBroker()
                .setNodeId(brokerId)
                .setHost("host" + brokerId)
                .setPort(9092)
            );
        }
        return new MetadataResponse(response, ApiKeys.METADATA.latestVersion());
    }

    private class MockApiHandler extends AdminApiHandler.Batched<AllBrokersStrategy.BrokerKey, Integer> {
        private final AllBrokersStrategy allBrokersStrategy = new AllBrokersStrategy(logContext);

        @Override
        public String apiName() {
            return "mock-api";
        }

        @Override
        public AbstractRequest.Builder<?> buildBatchedRequest(
            int brokerId,
            Set<AllBrokersStrategy.BrokerKey> keys
        ) {
            return new MetadataRequest.Builder(new MetadataRequestData());
        }

        @Override
        public ApiResult<AllBrokersStrategy.BrokerKey, Integer> handleResponse(
            Node broker,
            Set<AllBrokersStrategy.BrokerKey> keys,
            AbstractResponse response
        ) {
            return ApiResult.completed(keys.iterator().next(), broker.id());
        }

        @Override
        public AdminApiLookupStrategy<AllBrokersStrategy.BrokerKey> lookupStrategy() {
            return allBrokersStrategy;
        }
    }
}
