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

import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.utils.LogContext;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CoordinatorStrategyTest {

    @Test
    public void testBuildOldLookupRequest() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());
        strategy.disableBatch();
        FindCoordinatorRequest.Builder request = strategy.buildRequest(singleton(
            CoordinatorKey.byGroupId("foo")));
        assertEquals("foo", request.data().key());
        assertEquals(CoordinatorType.GROUP, CoordinatorType.forId(request.data().keyType()));
    }

    @Test
    public void testBuildLookupRequest() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());
        FindCoordinatorRequest.Builder request = strategy.buildRequest(new HashSet<>(Arrays.asList(
            CoordinatorKey.byGroupId("foo"),
            CoordinatorKey.byGroupId("bar"))));
        assertEquals("", request.data().key());
        assertEquals(2, request.data().coordinatorKeys().size());
        assertEquals(CoordinatorType.GROUP, CoordinatorType.forId(request.data().keyType()));
    }

    @Test
    public void testBuildLookupRequestNonRepresentable() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());
        FindCoordinatorRequest.Builder request = strategy.buildRequest(new HashSet<>(Arrays.asList(
                CoordinatorKey.byGroupId("foo"),
                null)));
        assertEquals("", request.data().key());
        assertEquals(1, request.data().coordinatorKeys().size());
    }

    @Test
    public void testBuildOldLookupRequestRequiresOneKey() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());
        strategy.disableBatch();
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(Collections.emptySet()));

        CoordinatorKey group1 = CoordinatorKey.byGroupId("foo");
        CoordinatorKey group2 = CoordinatorKey.byGroupId("bar");
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(Set.of(group1, group2)));
    }

    @Test
    public void testBuildOldLookupRequestRequiresAtLeastOneKey() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());
        strategy.disableBatch();

        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(
                new HashSet<>(Collections.singletonList(CoordinatorKey.byTransactionalId("txnid")))));
    }

    @Test
    public void testBuildLookupRequestRequiresAtLeastOneKey() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());

        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(Collections.emptySet()));
    }

    @Test
    public void testBuildLookupRequestRequiresKeySameType() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());

        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(
                new HashSet<>(Arrays.asList(
                        CoordinatorKey.byGroupId("group"),
                        CoordinatorKey.byTransactionalId("txnid")))));
    }

    @Test
    public void testHandleOldResponseRequiresOneKey() {
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData().setErrorCode(Errors.NONE.code());
        FindCoordinatorResponse response = new FindCoordinatorResponse(responseData);

        CoordinatorStrategy strategy = new CoordinatorStrategy(CoordinatorType.GROUP, new LogContext());
        strategy.disableBatch();
        assertThrows(IllegalArgumentException.class, () ->
            strategy.handleResponse(Collections.emptySet(), response));

        CoordinatorKey group1 = CoordinatorKey.byGroupId("foo");
        CoordinatorKey group2 = CoordinatorKey.byGroupId("bar");
        assertThrows(IllegalArgumentException.class, () ->
            strategy.handleResponse(Set.of(group1, group2), response));
    }

    @Test
    public void testSuccessfulOldCoordinatorLookup() {
        CoordinatorKey group = CoordinatorKey.byGroupId("foo");

        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData()
            .setErrorCode(Errors.NONE.code())
            .setHost("localhost")
            .setPort(9092)
            .setNodeId(1);

        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runOldLookup(group, responseData);
        assertEquals(singletonMap(group, 1), result.mappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    @Test
    public void testSuccessfulCoordinatorLookup() {
        CoordinatorKey group1 = CoordinatorKey.byGroupId("foo");
        CoordinatorKey group2 = CoordinatorKey.byGroupId("bar");

        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData()
            .setCoordinators(Arrays.asList(
                    new FindCoordinatorResponseData.Coordinator()
                        .setKey("foo")
                        .setErrorCode(Errors.NONE.code())
                        .setHost("localhost")
                        .setPort(9092)
                        .setNodeId(1),
                    new FindCoordinatorResponseData.Coordinator()
                        .setKey("bar")
                        .setErrorCode(Errors.NONE.code())
                        .setHost("localhost")
                        .setPort(9092)
                        .setNodeId(2)));

        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runLookup(new HashSet<>(Arrays.asList(group1, group2)), responseData);
        Map<CoordinatorKey, Integer> expectedResult = new HashMap<>();
        expectedResult.put(group1, 1);
        expectedResult.put(group2, 2);
        assertEquals(expectedResult, result.mappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    @Test
    public void testRetriableOldCoordinatorLookup() {
        testRetriableOldCoordinatorLookup(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        testRetriableOldCoordinatorLookup(Errors.COORDINATOR_NOT_AVAILABLE);
    }

    private void testRetriableOldCoordinatorLookup(Errors error) {
        CoordinatorKey group = CoordinatorKey.byGroupId("foo");
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData().setErrorCode(error.code());
        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runOldLookup(group, responseData);

        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.mappedKeys);
    }

    @Test
    public void testRetriableCoordinatorLookup() {
        testRetriableCoordinatorLookup(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        testRetriableCoordinatorLookup(Errors.COORDINATOR_NOT_AVAILABLE);
    }

    private void testRetriableCoordinatorLookup(Errors error) {
        CoordinatorKey group1 = CoordinatorKey.byGroupId("foo");
        CoordinatorKey group2 = CoordinatorKey.byGroupId("bar");
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData()
                .setCoordinators(Arrays.asList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setKey("foo")
                            .setErrorCode(error.code()),
                        new FindCoordinatorResponseData.Coordinator()
                            .setKey("bar")
                            .setErrorCode(Errors.NONE.code())
                            .setHost("localhost")
                            .setPort(9092)
                            .setNodeId(2)));
        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runLookup(new HashSet<>(Arrays.asList(group1, group2)), responseData);

        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(singletonMap(group2, 2), result.mappedKeys);
    }

    @Test
    public void testFatalErrorOldLookupResponses() {
        CoordinatorKey group = CoordinatorKey.byTransactionalId("foo");
        assertFatalOldLookup(group, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        assertFatalOldLookup(group, Errors.UNKNOWN_SERVER_ERROR);

        Throwable throwable = assertFatalOldLookup(group, Errors.GROUP_AUTHORIZATION_FAILED);
        assertInstanceOf(GroupAuthorizationException.class, throwable);
        GroupAuthorizationException exception = (GroupAuthorizationException) throwable;
        assertEquals("foo", exception.groupId());
    }

    public Throwable assertFatalOldLookup(
        CoordinatorKey key,
        Errors error
    ) {
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData().setErrorCode(error.code());
        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runOldLookup(key, responseData);

        assertEquals(emptyMap(), result.mappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());

        Throwable throwable = result.failedKeys.get(key);
        assertInstanceOf(error.exception().getClass(), throwable);
        return throwable;
    }

    @Test
    public void testFatalErrorLookupResponses() {
        CoordinatorKey group = CoordinatorKey.byTransactionalId("foo");
        assertFatalLookup(group, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        assertFatalLookup(group, Errors.UNKNOWN_SERVER_ERROR);

        Throwable throwable = assertFatalLookup(group, Errors.GROUP_AUTHORIZATION_FAILED);
        assertInstanceOf(GroupAuthorizationException.class, throwable);
        GroupAuthorizationException exception = (GroupAuthorizationException) throwable;
        assertEquals("foo", exception.groupId());
    }

    public Throwable assertFatalLookup(
        CoordinatorKey key,
        Errors error
    ) {
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData()
                .setCoordinators(Collections.singletonList(
                        new FindCoordinatorResponseData.Coordinator()
                            .setKey(key.idValue)
                            .setErrorCode(error.code())));
        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runLookup(singleton(key), responseData);

        assertEquals(emptyMap(), result.mappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());

        Throwable throwable = result.failedKeys.get(key);
        assertInstanceOf(error.exception().getClass(), throwable);
        return throwable;
    }

    private AdminApiLookupStrategy.LookupResult<CoordinatorKey> runOldLookup(
        CoordinatorKey key,
        FindCoordinatorResponseData responseData
    ) {
        CoordinatorStrategy strategy = new CoordinatorStrategy(key.type, new LogContext());
        strategy.disableBatch();
        FindCoordinatorResponse response = new FindCoordinatorResponse(responseData);
        return strategy.handleResponse(singleton(key), response);
    }

    private AdminApiLookupStrategy.LookupResult<CoordinatorKey> runLookup(
        Set<CoordinatorKey> keys,
        FindCoordinatorResponseData responseData
    ) {
        CoordinatorStrategy strategy = new CoordinatorStrategy(keys.iterator().next().type, new LogContext());
        strategy.buildRequest(keys);
        FindCoordinatorResponse response = new FindCoordinatorResponse(responseData);
        return strategy.handleResponse(keys, response);
    }

}
