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

import org.apache.kafka.clients.admin.internals.AdminApiDriver.RequestSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.Keys;
import org.apache.kafka.clients.admin.internals.AdminApiLookupStrategy.LookupResult;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.emptyMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class AdminApiDriverTest {
    private static final int API_TIMEOUT_MS = 30000;
    private static final int RETRY_BACKOFF_MS = 100;

    @Test
    public void testCoalescedLookup() {
        MockRequestScope scope = new MockRequestScope(OptionalInt.empty());
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", scope,
            "bar", scope
        )));

        Map<Set<String>, LookupResult<String>> lookupRequests = map(
            mkSet("foo", "bar"), mapped("foo", 1, "bar", 2)
        );

        ctx.poll(lookupRequests, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), completed("foo", 15L),
            mkSet("bar"), completed("bar", 30L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testCoalescedFulfillment() {
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", new MockRequestScope(OptionalInt.empty()),
            "bar", new MockRequestScope(OptionalInt.of(1))
        )));

        Map<Set<String>, LookupResult<String>> lookupRequests = map(
            mkSet("foo"), mapped("foo", 1),
            mkSet("bar"), mapped("bar", 1)
        );

        ctx.poll(lookupRequests, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo", "bar"), completed("foo", 15L, "bar", 30L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testKeyLookupFailure() {
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", new MockRequestScope(OptionalInt.empty()),
            "bar", new MockRequestScope(OptionalInt.of(1))
        )));

        Map<Set<String>, LookupResult<String>> lookupRequests = map(
            mkSet("foo"), failedLookup("foo", new UnknownServerException()),
            mkSet("bar"), mapped("bar", 1)
        );

        ctx.poll(lookupRequests, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("bar"), completed("bar", 30L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testKeyLookupRetry() {
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", new MockRequestScope(OptionalInt.empty()),
            "bar", new MockRequestScope(OptionalInt.of(1))
        )));

        Map<Set<String>, LookupResult<String>> lookupRequests = map(
            mkSet("foo"), emptyLookup(),
            mkSet("bar"), mapped("bar", 1)
        );

        ctx.poll(lookupRequests, emptyMap());

        Map<Set<String>, LookupResult<String>> fooRetry = map(
            mkSet("foo"), mapped("foo", 1)
        );

        Map<Set<String>, ApiResult<String, Long>> barFulfillment = map(
            mkSet("bar"), completed("bar", 30L)
        );

        ctx.poll(fooRetry, barFulfillment);

        Map<Set<String>, ApiResult<String, Long>> fooFulfillment = map(
            mkSet("foo"), completed("foo", 15L)
        );

        ctx.poll(emptyMap(), fooFulfillment);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testStaticMapping() {
        TestContext ctx = new TestContext(Keys.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        )));

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), completed("foo", 15L),
            mkSet("bar", "baz"), completed("bar", 30L, "baz", 45L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testFulfillmentFailure() {
        TestContext ctx = new TestContext(Keys.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        )));

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), failed("foo", new UnknownServerException()),
            mkSet("bar", "baz"), completed("bar", 30L, "baz", 45L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testFulfillmentRetry() {
        TestContext ctx = new TestContext(Keys.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        )));

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), completed("foo", 15L),
            mkSet("bar", "baz"), completed("bar", 30L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        Map<Set<String>, ApiResult<String, Long>> bazRetry = map(
            mkSet("baz"), completed("baz", 45L)
        );

        ctx.poll(emptyMap(), bazRetry);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testFulfillmentUnmapping() {
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", new MockRequestScope(OptionalInt.empty()),
            "bar", new MockRequestScope(OptionalInt.of(1))
        )));

        Map<Set<String>, LookupResult<String>> lookupRequests = map(
            mkSet("foo"), mapped("foo", 0),
            mkSet("bar"), mapped("bar", 1)
        );

        ctx.poll(lookupRequests, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), completed("foo", 15L),
            mkSet("bar"), unmapped("bar")
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        Map<Set<String>, LookupResult<String>> barLookupRetry = map(
            mkSet("bar"), mapped("bar", 1)
        );

        ctx.poll(barLookupRetry, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> barFulfillRetry = map(
            mkSet("bar"), completed("bar", 30L)
        );

        ctx.poll(emptyMap(), barFulfillRetry);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testRecoalescedLookup() {
        MockRequestScope scope = new MockRequestScope(OptionalInt.empty());
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", scope,
            "bar", scope
        )));

        Map<Set<String>, LookupResult<String>> lookupRequests = map(
            mkSet("foo", "bar"), mapped("foo", 1, "bar", 2)
        );

        ctx.poll(lookupRequests, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> fulfillment = map(
            mkSet("foo"), unmapped("foo"),
            mkSet("bar"), unmapped("bar")
        );

        ctx.poll(emptyMap(), fulfillment);

        Map<Set<String>, LookupResult<String>> retryLookupRequests = map(
            mkSet("foo", "bar"), mapped("foo", 3, "bar", 3)
        );

        ctx.poll(retryLookupRequests, emptyMap());

        Map<Set<String>, ApiResult<String, Long>> retryFulfillment = map(
            mkSet("foo", "bar"), completed("foo", 15L, "bar", 30L)
        );

        ctx.poll(emptyMap(), retryFulfillment);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testRetryLookupAfterDisconnect() {
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", new MockRequestScope(OptionalInt.empty())
        )));

        int initialLeaderId = 1;

        Map<Set<String>, LookupResult<String>> initialLookup = map(
            mkSet("foo"), mapped("foo", initialLeaderId)
        );

        ctx.poll(initialLookup, emptyMap());
        assertMappedKey(ctx.driver, "foo", initialLeaderId);

        ctx.handler.expectRequest(mkSet("foo"), completed("foo", 15L));

        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();
        assertEquals(1, requestSpecs.size());

        RequestSpec<String> requestSpec = requestSpecs.get(0);
        assertEquals(OptionalInt.of(initialLeaderId), requestSpec.scope.destinationBrokerId());

        ctx.driver.onFailure(ctx.time.milliseconds(), requestSpec, new DisconnectException());
        assertUnmappedKey(ctx.driver, "foo");

        int retryLeaderId = 2;

        ctx.lookupStrategy().expectLookup(mkSet("foo"), mapped("foo", retryLeaderId));
        List<RequestSpec<String>> retryLookupSpecs = ctx.driver.poll();
        assertEquals(1, retryLookupSpecs.size());

        RequestSpec<String> retryLookupSpec = retryLookupSpecs.get(0);
        assertEquals(ctx.time.milliseconds() + RETRY_BACKOFF_MS, retryLookupSpec.nextAllowedTryMs);
        assertEquals(1, retryLookupSpec.tries);
    }

    @Test
    public void testCoalescedStaticAndDynamicFulfillment() {
        Map<String, MockRequestScope> dynamicLookupScopes = map(
            "foo", new MockRequestScope(OptionalInt.empty())
        );

        Map<String, Integer> staticMapping = map(
            "bar", 1
        );

        TestContext ctx = new TestContext(new Keys<>(
            staticMapping,
            dynamicLookupScopes.keySet(),
            new MockLookupStrategy<>(dynamicLookupScopes)
        ));

        // Initially we expect a lookup for the dynamic key and a
        // fulfillment request for the static key
        LookupResult<String> lookupResult = mapped("foo", 1);
        ctx.lookupStrategy().expectLookup(
            mkSet("foo"), lookupResult
        );
        ctx.handler.expectRequest(
            mkSet("bar"), completed("bar", 10L)
        );

        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();
        assertEquals(2, requestSpecs.size());

        RequestSpec<String> lookupSpec = requestSpecs.get(0);
        assertEquals(mkSet("foo"), lookupSpec.keys);
        ctx.assertLookupResponse(lookupSpec, lookupResult);

        // Receive a disconnect from the fulfillment request so that
        // we have an opportunity to coalesce the keys.
        RequestSpec<String> fulfillmentSpec = requestSpecs.get(1);
        assertEquals(mkSet("bar"), fulfillmentSpec.keys);
        ctx.driver.onFailure(ctx.time.milliseconds(), fulfillmentSpec, new DisconnectException());

        // Now we should get two fulfillment requests. One of them will
        // the coalesced dynamic and static keys for broker 1. The other
        // should contain the single dynamic key for broker 0.
        ctx.handler.reset();
        ctx.handler.expectRequest(
            mkSet("foo", "bar"), completed("foo", 15L, "bar", 30L)
        );

        List<RequestSpec<String>> coalescedSpecs = ctx.driver.poll();
        assertEquals(1, coalescedSpecs.size());
        RequestSpec<String> coalescedSpec = coalescedSpecs.get(0);
        assertEquals(mkSet("foo", "bar"), coalescedSpec.keys);

        // Disconnect in order to ensure that only the dynamic key is unmapped.
        // Then complete the remaining requests.
        ctx.driver.onFailure(ctx.time.milliseconds(), coalescedSpec, new DisconnectException());

        Map<Set<String>, LookupResult<String>> fooLookupRetry = map(
            mkSet("foo"), mapped("foo", 3)
        );
        Map<Set<String>, ApiResult<String, Long>> barFulfillmentRetry = map(
            mkSet("bar"), completed("bar", 30L)
        );
        ctx.poll(fooLookupRetry, barFulfillmentRetry);

        Map<Set<String>, ApiResult<String, Long>> fooFulfillmentRetry = map(
            mkSet("foo"), completed("foo", 15L)
        );
        ctx.poll(emptyMap(), fooFulfillmentRetry);
        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testLookupRetryBookkeeping() {
        TestContext ctx = new TestContext(dynamicMapped(map(
            "foo", new MockRequestScope(OptionalInt.empty())
        )));

        LookupResult<String> emptyLookup = emptyLookup();
        ctx.lookupStrategy().expectLookup(mkSet("foo"), emptyLookup);

        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();
        assertEquals(1, requestSpecs.size());

        RequestSpec<String> requestSpec = requestSpecs.get(0);
        assertEquals(0, requestSpec.tries);
        assertEquals(0L, requestSpec.nextAllowedTryMs);
        ctx.assertLookupResponse(requestSpec, emptyLookup);

        List<RequestSpec<String>> retrySpecs = ctx.driver.poll();
        assertEquals(1, retrySpecs.size());

        RequestSpec<String> retrySpec = retrySpecs.get(0);
        assertEquals(1, retrySpec.tries);
        assertEquals(ctx.time.milliseconds() + RETRY_BACKOFF_MS, retrySpec.nextAllowedTryMs);
    }

    @Test
    public void testFulfillmentRetryBookkeeping() {
        TestContext ctx = new TestContext(Keys.staticMapped(map("foo", 0)));

        ApiResult<String, Long> emptyFulfillment = emptyFulfillment();
        ctx.handler.expectRequest(mkSet("foo"), emptyFulfillment);

        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();
        assertEquals(1, requestSpecs.size());

        RequestSpec<String> requestSpec = requestSpecs.get(0);
        assertEquals(0, requestSpec.tries);
        assertEquals(0L, requestSpec.nextAllowedTryMs);
        ctx.assertResponse(requestSpec, emptyFulfillment);

        List<RequestSpec<String>> retrySpecs = ctx.driver.poll();
        assertEquals(1, retrySpecs.size());

        RequestSpec<String> retrySpec = retrySpecs.get(0);
        assertEquals(1, retrySpec.tries);
        assertEquals(ctx.time.milliseconds() + RETRY_BACKOFF_MS, retrySpec.nextAllowedTryMs);
    }

    private static void assertMappedKey(
        AdminApiDriver<String, Long> driver,
        String key,
        Integer expectedBrokerId
    )  {
        OptionalInt brokerIdOpt = driver.keyToBrokerId(key);
        assertEquals(OptionalInt.of(expectedBrokerId), brokerIdOpt);
    }

    private static void assertUnmappedKey(
        AdminApiDriver<String, Long> driver,
        String key
    ) {
        OptionalInt brokerIdOpt = driver.keyToBrokerId(key);
        assertEquals(OptionalInt.empty(), brokerIdOpt);
        KafkaFutureImpl<Long> future = driver.futures().get(key);
        assertFalse(future.isDone());
    }

    private static void assertFailedKey(
        AdminApiDriver<String, Long> driver,
        String key,
        Throwable expectedException
    ) {
        KafkaFutureImpl<Long> future = driver.futures().get(key);
        assertTrue(future.isCompletedExceptionally());
        Throwable exception = assertThrows(ExecutionException.class, future::get);
        assertEquals(expectedException, exception.getCause());
    }

    private static void assertCompletedKey(
        AdminApiDriver<String, Long> driver,
        String key,
        Long expected
    ) {
        KafkaFutureImpl<Long> future = driver.futures().get(key);
        assertTrue(future.isDone());
        try {
            assertEquals(expected, future.get());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static class MockRequestScope implements ApiRequestScope {
        private final OptionalInt destinationBrokerId;

        private MockRequestScope(OptionalInt destinationBrokerId) {
            this.destinationBrokerId = destinationBrokerId;
        }

        @Override
        public OptionalInt destinationBrokerId() {
            return destinationBrokerId;
        }
    }

    private static class TestContext {
        private final MockTime time = new MockTime();
        private final Keys<String> keys;
        private final MockAdminApiHandler<String, Long> handler;
        private final AdminApiDriver<String, Long> driver;

        public TestContext(Keys<String> keys) {
            this.keys = keys;
            this.handler = new MockAdminApiHandler<>(keys);
            this.driver = new AdminApiDriver<>(
                handler,
                time.milliseconds() + API_TIMEOUT_MS,
                RETRY_BACKOFF_MS,
                new LogContext()
            );

            keys.staticKeys.forEach((key, brokerId) -> {
                assertMappedKey(driver, key, brokerId);
            });

            keys.dynamicKeys.forEach(key -> {
                assertUnmappedKey(driver, key);
            });
        }

        private void assertLookupResponse(
            RequestSpec<String> requestSpec,
            LookupResult<String> result
        ) {
            requestSpec.keys.forEach(key -> {
                assertUnmappedKey(driver, key);
            });

            // The response is just a placeholder. The result is all we are interested in
            MetadataResponse response = new MetadataResponse(new MetadataResponseData(),
                ApiKeys.METADATA.latestVersion());
            driver.onResponse(time.milliseconds(), requestSpec, response);

            result.mappedKeys.forEach((key, brokerId) -> {
                assertMappedKey(driver, key, brokerId);
            });

            result.failedKeys.forEach((key, exception) -> {
                assertFailedKey(driver, key, exception);
            });
        }

        private void assertResponse(
            RequestSpec<String> requestSpec,
            ApiResult<String, Long> result
        ) {
            int brokerId = requestSpec.scope.destinationBrokerId().orElseThrow(() ->
                new AssertionError("Fulfillment requests must specify a target brokerId"));

            requestSpec.keys.forEach(key -> {
                assertMappedKey(driver, key, brokerId);
            });

            // The response is just a placeholder. The result is all we are interested in
            MetadataResponse response = new MetadataResponse(new MetadataResponseData(),
                ApiKeys.METADATA.latestVersion());

            driver.onResponse(time.milliseconds(), requestSpec, response);

            result.unmappedKeys.forEach(key -> {
                assertUnmappedKey(driver, key);
            });

            result.failedKeys.forEach((key, exception) -> {
                assertFailedKey(driver, key, exception);
            });

            result.completedKeys.forEach((key, value) -> {
                assertCompletedKey(driver, key, value);
            });
        }

        private MockLookupStrategy<String> lookupStrategy() {
            if (keys.dynamicKeys.isEmpty()) {
                throw new IllegalStateException("Unexpected lookup when no dynamic mapping is defined");
            }
            return (MockLookupStrategy<String>) keys.lookupStrategy;
        }

        public void poll(
            Map<Set<String>, LookupResult<String>> expectedLookups,
            Map<Set<String>, ApiResult<String, Long>> expectedRequests
        ) {
            if (!expectedLookups.isEmpty()) {
                MockLookupStrategy<String> lookupStrategy = lookupStrategy();
                lookupStrategy.reset();
                expectedLookups.forEach(lookupStrategy::expectLookup);
            }

            handler.reset();
            expectedRequests.forEach(handler::expectRequest);

            List<RequestSpec<String>> requestSpecs = driver.poll();
            assertEquals(expectedLookups.size() + expectedRequests.size(), requestSpecs.size(),
                "Driver generated an unexpected number of requests");

            for (RequestSpec<String> requestSpec : requestSpecs) {
                Set<String> keys = requestSpec.keys;
                if (expectedLookups.containsKey(keys)) {
                    LookupResult<String> result = expectedLookups.get(keys);
                    assertLookupResponse(requestSpec, result);
                } else if (expectedRequests.containsKey(keys)) {
                    ApiResult<String, Long> result = expectedRequests.get(keys);
                    assertResponse(requestSpec, result);
                } else {
                    fail("Unexpected request for keys " + keys);
                }
            }
        }
    }

    private static class MockLookupStrategy<K> implements AdminApiLookupStrategy<K> {
        private final Map<Set<K>, LookupResult<K>> expectedLookups = new HashMap<>();
        private final Map<K, MockRequestScope> lookupScopes;

        private MockLookupStrategy(Map<K, MockRequestScope> lookupScopes) {
            this.lookupScopes = lookupScopes;
        }

        @Override
        public ApiRequestScope lookupScope(K key) {
            return lookupScopes.get(key);
        }

        public void expectLookup(Set<K> keys, LookupResult<K> result) {
            expectedLookups.put(keys, result);
        }

        @Override
        public AbstractRequest.Builder<?> buildRequest(Set<K> keys) {
            // The request is just a placeholder in these tests
            assertTrue(expectedLookups.containsKey(keys), "Unexpected lookup request for keys " + keys);
            return new MetadataRequest.Builder(Collections.emptyList(), false);
        }

        @Override
        public LookupResult<K> handleResponse(Set<K> keys, AbstractResponse response) {
            return Optional.ofNullable(expectedLookups.get(keys)).orElseThrow(() ->
                new AssertionError("Unexpected fulfillment request for keys " + keys)
            );
        }

        public void reset() {
            expectedLookups.clear();
        }
    }

    private static class MockAdminApiHandler<K, V> implements AdminApiHandler<K, V> {
        private final Keys<K> keyMappings;
        private final Map<Set<K>, ApiResult<K, V>> expectedRequests = new HashMap<>();

        private MockAdminApiHandler(Keys<K> keyMappings) {
            this.keyMappings = keyMappings;
        }

        @Override
        public String apiName() {
            return "mock-api";
        }

        @Override
        public Keys<K> initializeKeys() {
            return keyMappings;
        }

        public void expectRequest(Set<K> keys, ApiResult<K, V> result) {
            expectedRequests.put(keys, result);
        }

        @Override
        public AbstractRequest.Builder<?> buildRequest(Integer brokerId, Set<K> keys) {
            // The request is just a placeholder in these tests
            assertTrue(expectedRequests.containsKey(keys), "Unexpected fulfillment request for keys " + keys);
            return new MetadataRequest.Builder(Collections.emptyList(), false);
        }

        @Override
        public ApiResult<K, V> handleResponse(Integer brokerId, Set<K> keys, AbstractResponse response) {
            return Optional.ofNullable(expectedRequests.get(keys)).orElseThrow(() ->
                new AssertionError("Unexpected fulfillment request for keys " + keys)
            );
        }

        public void reset() {
            expectedRequests.clear();
        }
    }

    private static <K, V> Map<K, V> map(K key, V value) {
        return Collections.singletonMap(key, value);
    }

    private static <K, V> Map<K, V> map(K k1, V v1, K k2, V v2) {
        HashMap<K, V> map = new HashMap<>(2);
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    private static <K, V> Map<K, V> map(K k1, V v1, K k2, V v2, K k3, V v3) {
        HashMap<K, V> map = new HashMap<>(3);
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return map;
    }

    private static Keys<String> dynamicMapped(Map<String, MockRequestScope> lookupScopes) {
        MockLookupStrategy<String> strategy = new MockLookupStrategy<>(lookupScopes);
        return Keys.dynamicMapped(lookupScopes.keySet(), strategy);
    }

    private static ApiResult<String, Long> completed(String key, Long value) {
        return new ApiResult<>(map(key, value), emptyMap(), Collections.emptyList());
    }

    private static ApiResult<String, Long> failed(String key, Throwable exception) {
        return new ApiResult<>(emptyMap(), map(key, exception), Collections.emptyList());
    }

    private static ApiResult<String, Long> unmapped(String... keys) {
        return new ApiResult<>(emptyMap(), emptyMap(), Arrays.asList(keys));
    }

    private static ApiResult<String, Long> completed(String k1, Long v1, String k2, Long v2) {
        return new ApiResult<>(map(k1, v1, k2, v2), emptyMap(), Collections.emptyList());
    }

    private static ApiResult<String, Long> emptyFulfillment() {
        return new ApiResult<>(emptyMap(), emptyMap(), Collections.emptyList());
    }

    private static LookupResult<String> failedLookup(String key, Throwable exception) {
        return new LookupResult<>(map(key, exception), emptyMap());
    }

    private static LookupResult<String> emptyLookup() {
        return new LookupResult<>(emptyMap(), emptyMap());
    }

    private static LookupResult<String> mapped(String key, Integer brokerId) {
        return new LookupResult<>(emptyMap(), map(key, brokerId));
    }

    private static LookupResult<String> mapped(String k1, Integer broker1, String k2, Integer broker2) {
        return new LookupResult<>(emptyMap(), map(k1, broker1, k2, broker2));
    }

}
