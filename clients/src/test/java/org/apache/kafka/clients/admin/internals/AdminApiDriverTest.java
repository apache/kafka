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

import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.internals.AdminApiDriver.RequestSpec;
import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.clients.admin.internals.AdminApiLookupStrategy.LookupResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.NoBatchedFindCoordinatorsException;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
    private static final int RETRY_BACKOFF_MAX_MS = 1000;

    @Test
    public void testCoalescedLookup() {
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1",
            "bar", "c1"
        ));

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
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1",
            "bar", "c2"
        ));

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
        // Ensure that both generic failures and unhandled UnsupportedVersionExceptions (which could be specifically
        // handled in both the lookup and the fulfillment stages) result in the expected lookup failures.
        Exception[] keyLookupExceptions = new Exception[] {
            new UnknownServerException(), new UnsupportedVersionException("")
        };
        for (Exception keyLookupException : keyLookupExceptions) {
            TestContext ctx = TestContext.dynamicMapped(map(
                "foo", "c1",
                "bar", "c2"
            ));

            Map<Set<String>, LookupResult<String>> lookupRequests = map(
                mkSet("foo"), failedLookup("foo", keyLookupException),
                mkSet("bar"), mapped("bar", 1)
            );

            ctx.poll(lookupRequests, emptyMap());

            Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
                mkSet("bar"), completed("bar", 30L)
            );

            ctx.poll(emptyMap(), fulfillmentResults);

            ctx.poll(emptyMap(), emptyMap());
        }
    }

    @Test
    public void testKeyLookupRetry() {
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1",
            "bar", "c2"
        ));

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
        TestContext ctx = TestContext.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        ));

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), completed("foo", 15L),
            mkSet("bar", "baz"), completed("bar", 30L, "baz", 45L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testFulfillmentFailure() {
        TestContext ctx = TestContext.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        ));

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), failed("foo", new UnknownServerException()),
            mkSet("bar", "baz"), completed("bar", 30L, "baz", 45L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);

        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testFulfillmentRetry() {
        TestContext ctx = TestContext.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        ));

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
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1",
            "bar", "c2"
        ));

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
    public void testFulfillmentFailureUnsupportedVersion() {
        TestContext ctx = TestContext.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 1
        ));

        Map<Set<String>, ApiResult<String, Long>> fulfillmentResults = map(
            mkSet("foo"), failed("foo", new UnsupportedVersionException("")),
            mkSet("bar", "baz"), completed("bar", 30L, "baz", 45L)
        );

        ctx.poll(emptyMap(), fulfillmentResults);
        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testFulfillmentRetriableUnsupportedVersion() {
        TestContext ctx = TestContext.staticMapped(map(
            "foo", 0,
            "bar", 1,
            "baz", 2
        ));

        ctx.handler.addRetriableUnsupportedVersionKey("foo");
        // The mapped ApiResults are only used in the onResponse/handleResponse path - anything that needs
        // to be handled in the onFailure path needs to be manually set up later.
        ctx.handler.expectRequest(mkSet("foo"), failed("foo", new UnsupportedVersionException("")));
        ctx.handler.expectRequest(mkSet("bar"), failed("bar", new UnsupportedVersionException("")));
        ctx.handler.expectRequest(mkSet("baz"), completed("baz", 45L));
        // Setting up specific fulfillment stage executions requires polling the driver in order to obtain
        // the request specs needed for the onResponse/onFailure callbacks.
        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();

        requestSpecs.forEach(requestSpec -> {
            if (requestSpec.keys.contains("foo") || requestSpec.keys.contains("bar")) {
                ctx.driver.onFailure(ctx.time.milliseconds(), requestSpec, new UnsupportedVersionException(""));
            } else {
                ctx.driver.onResponse(
                    ctx.time.milliseconds(),
                    requestSpec,
                    new MetadataResponse(new MetadataResponseData(), ApiKeys.METADATA.latestVersion()),
                    Node.noNode());
            }
        });
        // Verify retry for "foo" but not for "bar" or "baz"
        ctx.poll(emptyMap(), map(
            mkSet("foo"), failed("foo", new UnsupportedVersionException(""))
        ));
        ctx.poll(emptyMap(), emptyMap());
    }

    @Test
    public void testRecoalescedLookup() {
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1",
            "bar", "c1"
        ));

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
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1"
        ));

        int initialLeaderId = 1;

        Map<Set<String>, LookupResult<String>> initialLookup = map(
            mkSet("foo"), mapped("foo", initialLeaderId)
        );

        ctx.poll(initialLookup, emptyMap());
        assertMappedKey(ctx, "foo", initialLeaderId);

        ctx.handler.expectRequest(mkSet("foo"), completed("foo", 15L));

        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();
        assertEquals(1, requestSpecs.size());

        RequestSpec<String> requestSpec = requestSpecs.get(0);
        assertEquals(OptionalInt.of(initialLeaderId), requestSpec.scope.destinationBrokerId());

        ctx.driver.onFailure(ctx.time.milliseconds(), requestSpec, new DisconnectException());
        assertUnmappedKey(ctx, "foo");

        int retryLeaderId = 2;

        ctx.lookupStrategy().expectLookup(mkSet("foo"), mapped("foo", retryLeaderId));
        List<RequestSpec<String>> retryLookupSpecs = ctx.driver.poll();
        assertEquals(1, retryLookupSpecs.size());

        RequestSpec<String> retryLookupSpec = retryLookupSpecs.get(0);
        assertEquals(ctx.time.milliseconds(), retryLookupSpec.nextAllowedTryMs);
        assertEquals(1, retryLookupSpec.tries);
    }

    @Test
    public void testRetryLookupAndDisableBatchAfterNoBatchedFindCoordinatorsException() {
        MockTime time = new MockTime();
        LogContext lc = new LogContext();
        Set<String> groupIds = new HashSet<>(Arrays.asList("g1", "g2"));
        DeleteConsumerGroupsHandler handler = new DeleteConsumerGroupsHandler(lc);
        AdminApiFuture<CoordinatorKey, Void> future = AdminApiFuture.forKeys(
                groupIds.stream().map(g -> CoordinatorKey.byGroupId(g)).collect(Collectors.toSet()));

        AdminApiDriver<CoordinatorKey, Void> driver = new AdminApiDriver<>(
            handler,
            future,
            time.milliseconds() + API_TIMEOUT_MS,
            RETRY_BACKOFF_MS,
            RETRY_BACKOFF_MAX_MS,
            new LogContext()
        );

        assertTrue(((CoordinatorStrategy) handler.lookupStrategy()).batch);
        List<RequestSpec<CoordinatorKey>> requestSpecs = driver.poll();
        // Expect CoordinatorStrategy to try resolving all coordinators in a single request
        assertEquals(1, requestSpecs.size());

        RequestSpec<CoordinatorKey> requestSpec = requestSpecs.get(0);
        driver.onFailure(time.milliseconds(), requestSpec, new NoBatchedFindCoordinatorsException("message"));
        assertFalse(((CoordinatorStrategy) handler.lookupStrategy()).batch);

        // Batching is now disabled, so we now have a request per groupId
        List<RequestSpec<CoordinatorKey>> retryLookupSpecs = driver.poll();
        assertEquals(groupIds.size(), retryLookupSpecs.size());
        // These new requests are treated a new requests and not retries
        for (RequestSpec<CoordinatorKey> retryLookupSpec : retryLookupSpecs) {
            assertEquals(0, retryLookupSpec.nextAllowedTryMs);
            assertEquals(0, retryLookupSpec.tries);
        }
    }

    @Test
    public void testCoalescedStaticAndDynamicFulfillment() {
        Map<String, String> dynamicMapping = map(
            "foo", "c1"
        );

        Map<String, Integer> staticMapping = map(
            "bar", 1
        );

        TestContext ctx = new TestContext(
            staticMapping,
            dynamicMapping
        );

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
        TestContext ctx = TestContext.dynamicMapped(map(
            "foo", "c1"
        ));

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
        assertEquals(ctx.time.milliseconds(), retrySpec.nextAllowedTryMs);
    }

    @Test
    public void testFulfillmentRetryBookkeeping() {
        TestContext ctx = TestContext.staticMapped(map("foo", 0));

        ApiResult<String, Long> emptyFulfillment = emptyFulfillment();
        ctx.handler.expectRequest(mkSet("foo"), emptyFulfillment);

        List<RequestSpec<String>> requestSpecs = ctx.driver.poll();
        assertEquals(1, requestSpecs.size());

        RequestSpec<String> requestSpec = requestSpecs.get(0);
        assertEquals(0, requestSpec.tries);
        assertEquals(0L, requestSpec.nextAllowedTryMs);
        ctx.assertResponse(requestSpec, emptyFulfillment, Node.noNode());

        List<RequestSpec<String>> retrySpecs = ctx.driver.poll();
        assertEquals(1, retrySpecs.size());

        RequestSpec<String> retrySpec = retrySpecs.get(0);
        assertEquals(1, retrySpec.tries);
        assertEquals(ctx.time.milliseconds(), retrySpec.nextAllowedTryMs,
                (long) (RETRY_BACKOFF_MS * CommonClientConfigs.RETRY_BACKOFF_JITTER));
    }

    private static void assertMappedKey(
        TestContext context,
        String key,
        Integer expectedBrokerId
    )  {
        OptionalInt brokerIdOpt = context.driver.keyToBrokerId(key);
        assertEquals(OptionalInt.of(expectedBrokerId), brokerIdOpt);
    }

    private static void assertUnmappedKey(
        TestContext context,
        String key
    ) {
        OptionalInt brokerIdOpt = context.driver.keyToBrokerId(key);
        assertEquals(OptionalInt.empty(), brokerIdOpt);
        KafkaFuture<Long> future = context.future.all().get(key);
        assertFalse(future.isDone());
    }

    private static void assertFailedKey(
        TestContext context,
        String key,
        Throwable expectedException
    ) {
        KafkaFuture<Long> future = context.future.all().get(key);
        assertTrue(future.isCompletedExceptionally());
        Throwable exception = assertThrows(ExecutionException.class, future::get);
        assertEquals(expectedException, exception.getCause());
    }

    private static void assertCompletedKey(
        TestContext context,
        String key,
        Long expected
    ) {
        KafkaFuture<Long> future = context.future.all().get(key);
        assertTrue(future.isDone());
        try {
            assertEquals(expected, future.get());
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static class MockRequestScope implements ApiRequestScope {
        private final OptionalInt destinationBrokerId;
        private final String id;

        private MockRequestScope(
            OptionalInt destinationBrokerId,
            String id
        ) {
            this.destinationBrokerId = destinationBrokerId;
            this.id = id;
        }

        @Override
        public OptionalInt destinationBrokerId() {
            return destinationBrokerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MockRequestScope that = (MockRequestScope) o;
            return Objects.equals(destinationBrokerId, that.destinationBrokerId) &&
                Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(destinationBrokerId, id);
        }
    }

    private static class TestContext {
        private final MockTime time = new MockTime();
        private final MockAdminApiHandler<String, Long> handler;
        private final AdminApiDriver<String, Long> driver;
        private final AdminApiFuture.SimpleAdminApiFuture<String, Long> future;

        public TestContext(
            Map<String, Integer> staticKeys,
            Map<String, String> dynamicKeys
        ) {
            Map<String, MockRequestScope> lookupScopes = new HashMap<>();
            staticKeys.forEach((key, brokerId) -> {
                MockRequestScope scope = new MockRequestScope(OptionalInt.of(brokerId), null);
                lookupScopes.put(key, scope);
            });

            dynamicKeys.forEach((key, context) -> {
                MockRequestScope scope = new MockRequestScope(OptionalInt.empty(), context);
                lookupScopes.put(key, scope);
            });

            MockLookupStrategy<String> lookupStrategy = new MockLookupStrategy<>(lookupScopes);
            this.handler = new MockAdminApiHandler<>(lookupStrategy);
            this.future = AdminApiFuture.forKeys(lookupStrategy.lookupScopes.keySet());

            this.driver = new AdminApiDriver<>(
                handler,
                future,
                time.milliseconds() + API_TIMEOUT_MS,
                RETRY_BACKOFF_MS,
                RETRY_BACKOFF_MAX_MS,
                new LogContext()
            );

            staticKeys.forEach((key, brokerId) -> {
                assertMappedKey(this, key, brokerId);
            });

            dynamicKeys.keySet().forEach(key -> {
                assertUnmappedKey(this, key);
            });
        }

        public static TestContext staticMapped(Map<String, Integer> staticKeys) {
            return new TestContext(staticKeys, Collections.emptyMap());
        }

        public static TestContext dynamicMapped(Map<String, String> dynamicKeys) {
            return new TestContext(Collections.emptyMap(), dynamicKeys);
        }

        private void assertLookupResponse(
            RequestSpec<String> requestSpec,
            LookupResult<String> result
        ) {
            requestSpec.keys.forEach(key -> {
                assertUnmappedKey(this, key);
            });

            // The response is just a placeholder. The result is all we are interested in
            MetadataResponse response = new MetadataResponse(new MetadataResponseData(),
                ApiKeys.METADATA.latestVersion());
            driver.onResponse(time.milliseconds(), requestSpec, response, Node.noNode());

            result.mappedKeys.forEach((key, brokerId) -> {
                assertMappedKey(this, key, brokerId);
            });

            result.failedKeys.forEach((key, exception) -> {
                assertFailedKey(this, key, exception);
            });
        }

        private void assertResponse(
            RequestSpec<String> requestSpec,
            ApiResult<String, Long> result,
            Node node
        ) {
            int brokerId = requestSpec.scope.destinationBrokerId().orElseThrow(() ->
                new AssertionError("Fulfillment requests must specify a target brokerId"));

            requestSpec.keys.forEach(key -> {
                assertMappedKey(this, key, brokerId);
            });

            // The response is just a placeholder. The result is all we are interested in
            MetadataResponse response = new MetadataResponse(new MetadataResponseData(),
                ApiKeys.METADATA.latestVersion());

            driver.onResponse(time.milliseconds(), requestSpec, response, node);

            result.unmappedKeys.forEach(key -> {
                assertUnmappedKey(this, key);
            });

            result.failedKeys.forEach((key, exception) -> {
                assertFailedKey(this, key, exception);
            });

            result.completedKeys.forEach((key, value) -> {
                assertCompletedKey(this, key, value);
            });
        }

        private MockLookupStrategy<String> lookupStrategy() {
            return handler.lookupStrategy;
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
                    assertResponse(requestSpec, result, Node.noNode());
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

    private static class MockAdminApiHandler<K, V> extends AdminApiHandler.Batched<K, V> {
        private final Map<Set<K>, ApiResult<K, V>> expectedRequests = new HashMap<>();
        private final MockLookupStrategy<K> lookupStrategy;
        private final Map<K, Boolean> retriableUnsupportedVersionKeys;

        private MockAdminApiHandler(MockLookupStrategy<K> lookupStrategy) {
            this.lookupStrategy = lookupStrategy;
            this.retriableUnsupportedVersionKeys = new ConcurrentHashMap<>();
        }

        @Override
        public String apiName() {
            return "mock-api";
        }

        @Override
        public AdminApiLookupStrategy<K> lookupStrategy() {
            return lookupStrategy;
        }

        public void expectRequest(Set<K> keys, ApiResult<K, V> result) {
            expectedRequests.put(keys, result);
        }

        @Override
        public AbstractRequest.Builder<?> buildBatchedRequest(int brokerId, Set<K> keys) {
            // The request is just a placeholder in these tests
            assertTrue(expectedRequests.containsKey(keys), "Unexpected fulfillment request for keys " + keys);
            return new MetadataRequest.Builder(Collections.emptyList(), false);
        }

        @Override
        public ApiResult<K, V> handleResponse(Node broker, Set<K> keys, AbstractResponse response) {
            return Optional.ofNullable(expectedRequests.get(keys)).orElseThrow(() ->
                new AssertionError("Unexpected fulfillment request for keys " + keys)
            );
        }

        @Override
        public Map<K, Throwable> handleUnsupportedVersionException(
            int brokerId,
            UnsupportedVersionException exception,
            Set<K> keys
        ) {
            return keys
                .stream()
                .filter(k -> !retriableUnsupportedVersionKeys.containsKey(k))
                .collect(Collectors.toMap(k -> k, k -> exception));
        }

        public void reset() {
            expectedRequests.clear();
        }

        public void addRetriableUnsupportedVersionKey(K key) {
            retriableUnsupportedVersionKeys.put(key, Boolean.TRUE);
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
