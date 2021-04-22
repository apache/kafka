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

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CoordinatorStrategyTest {

    @Test
    public void testBuildLookupRequest() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(new LogContext());
        FindCoordinatorRequest.Builder request = strategy.buildRequest(singleton(
            CoordinatorKey.byGroupId("foo")));
        assertEquals("foo", request.data().key());
        assertEquals(CoordinatorType.GROUP, CoordinatorType.forId(request.data().keyType()));
    }

    @Test
    public void testBuildLookupRequestRequiresOneKey() {
        CoordinatorStrategy strategy = new CoordinatorStrategy(new LogContext());
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(Collections.emptySet()));

        CoordinatorKey group1 = CoordinatorKey.byGroupId("foo");
        CoordinatorKey group2 = CoordinatorKey.byGroupId("bar");
        assertThrows(IllegalArgumentException.class, () -> strategy.buildRequest(mkSet(group1, group2)));
    }

    @Test
    public void testHandleResponseRequiresOneKey() {
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData().setErrorCode(Errors.NONE.code());
        FindCoordinatorResponse response = new FindCoordinatorResponse(responseData);

        CoordinatorStrategy strategy = new CoordinatorStrategy(new LogContext());
        assertThrows(IllegalArgumentException.class, () ->
            strategy.handleResponse(Collections.emptySet(), response));

        CoordinatorKey group1 = CoordinatorKey.byGroupId("foo");
        CoordinatorKey group2 = CoordinatorKey.byGroupId("bar");
        assertThrows(IllegalArgumentException.class, () ->
            strategy.handleResponse(mkSet(group1, group2), response));
    }

    @Test
    public void testSuccessfulCoordinatorLookup() {
        CoordinatorKey group = CoordinatorKey.byGroupId("foo");

        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData()
            .setErrorCode(Errors.NONE.code())
            .setHost("localhost")
            .setPort(9092)
            .setNodeId(1);

        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runLookup(group, responseData);
        assertEquals(singletonMap(group, 1), result.mappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    @Test
    public void testRetriableCoordinatorLookup() {
        testRetriableCoordinatorLookup(Errors.COORDINATOR_LOAD_IN_PROGRESS);
        testRetriableCoordinatorLookup(Errors.COORDINATOR_NOT_AVAILABLE);
    }

    private void testRetriableCoordinatorLookup(Errors error) {
        CoordinatorKey group = CoordinatorKey.byGroupId("foo");
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData().setErrorCode(error.code());
        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runLookup(group, responseData);

        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(emptyMap(), result.mappedKeys);
    }

    @Test
    public void testFatalErrorLookupResponses() {
        CoordinatorKey group = CoordinatorKey.byTransactionalId("foo");
        assertFatalLookup(group, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        assertFatalLookup(group, Errors.UNKNOWN_SERVER_ERROR);

        Throwable throwable = assertFatalLookup(group, Errors.GROUP_AUTHORIZATION_FAILED);
        assertTrue(throwable instanceof GroupAuthorizationException);
        GroupAuthorizationException exception = (GroupAuthorizationException) throwable;
        assertEquals("foo", exception.groupId());
    }

    public Throwable assertFatalLookup(
        CoordinatorKey key,
        Errors error
    ) {
        FindCoordinatorResponseData responseData = new FindCoordinatorResponseData().setErrorCode(error.code());
        AdminApiLookupStrategy.LookupResult<CoordinatorKey> result = runLookup(key, responseData);

        assertEquals(emptyMap(), result.mappedKeys);
        assertEquals(singleton(key), result.failedKeys.keySet());

        Throwable throwable = result.failedKeys.get(key);
        assertTrue(error.exception().getClass().isInstance(throwable));
        return throwable;
    }

    private AdminApiLookupStrategy.LookupResult<CoordinatorKey> runLookup(
        CoordinatorKey key,
        FindCoordinatorResponseData responseData
    ) {
        CoordinatorStrategy strategy = new CoordinatorStrategy(new LogContext());
        FindCoordinatorResponse response = new FindCoordinatorResponse(responseData);
        return strategy.handleResponse(singleton(key), response);
    }

}
