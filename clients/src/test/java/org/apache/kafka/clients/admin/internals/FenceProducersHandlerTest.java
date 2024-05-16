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

import org.apache.kafka.clients.admin.internals.AdminApiHandler.ApiResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.InitProducerIdRequest;
import org.apache.kafka.common.requests.InitProducerIdResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class FenceProducersHandlerTest {
    private final LogContext logContext = new LogContext();
    private final Node node = new Node(1, "host", 1234);

    @Test
    public void testBuildRequest() {
        FenceProducersHandler handler = new FenceProducersHandler(logContext);
        mkSet("foo", "bar", "baz").forEach(transactionalId -> assertLookup(handler, transactionalId));
    }

    @Test
    public void testHandleSuccessfulResponse() {
        String transactionalId = "foo";
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);

        FenceProducersHandler handler = new FenceProducersHandler(logContext);

        short epoch = 57;
        long producerId = 7;
        InitProducerIdResponse response = new InitProducerIdResponse(new InitProducerIdResponseData()
            .setProducerEpoch(epoch)
            .setProducerId(producerId));

        ApiResult<CoordinatorKey, ProducerIdAndEpoch> result = handler.handleSingleResponse(
            node, key, response);

        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(singleton(key), result.completedKeys.keySet());

        ProducerIdAndEpoch expected = new ProducerIdAndEpoch(producerId, epoch);
        assertEquals(expected, result.completedKeys.get(key));
    }

    @Test
    public void testHandleErrorResponse() {
        String transactionalId = "foo";
        FenceProducersHandler handler = new FenceProducersHandler(logContext);
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
        assertFatalError(handler, transactionalId, Errors.CLUSTER_AUTHORIZATION_FAILED);
        assertFatalError(handler, transactionalId, Errors.UNKNOWN_SERVER_ERROR);
        assertFatalError(handler, transactionalId, Errors.PRODUCER_FENCED);
        assertFatalError(handler, transactionalId, Errors.TRANSACTIONAL_ID_NOT_FOUND);
        assertFatalError(handler, transactionalId, Errors.INVALID_PRODUCER_EPOCH);
        assertRetriableError(handler, transactionalId, Errors.COORDINATOR_LOAD_IN_PROGRESS);
        assertUnmappedKey(handler, transactionalId, Errors.NOT_COORDINATOR);
        assertUnmappedKey(handler, transactionalId, Errors.COORDINATOR_NOT_AVAILABLE);
    }

    private void assertFatalError(
        FenceProducersHandler handler,
        String transactionalId,
        Errors error
    ) {
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        ApiResult<CoordinatorKey, ProducerIdAndEpoch> result = handleResponseError(handler, transactionalId, error);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(mkSet(key), result.failedKeys.keySet());

        Throwable throwable = result.failedKeys.get(key);
        assertInstanceOf(error.exception().getClass(), throwable);
    }

    private void assertRetriableError(
        FenceProducersHandler handler,
        String transactionalId,
        Errors error
    ) {
        ApiResult<CoordinatorKey, ProducerIdAndEpoch> result = handleResponseError(handler, transactionalId, error);
        assertEquals(emptyList(), result.unmappedKeys);
        assertEquals(emptyMap(), result.failedKeys);
    }

    private void assertUnmappedKey(
        FenceProducersHandler handler,
        String transactionalId,
        Errors error
    ) {
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        ApiResult<CoordinatorKey, ProducerIdAndEpoch> result = handleResponseError(handler, transactionalId, error);
        assertEquals(emptyMap(), result.failedKeys);
        assertEquals(singletonList(key), result.unmappedKeys);
    }

    private ApiResult<CoordinatorKey, ProducerIdAndEpoch> handleResponseError(
        FenceProducersHandler handler,
        String transactionalId,
        Errors error
    ) {
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        Set<CoordinatorKey> keys = mkSet(key);

        InitProducerIdResponse response = new InitProducerIdResponse(new InitProducerIdResponseData()
            .setErrorCode(error.code()));

        ApiResult<CoordinatorKey, ProducerIdAndEpoch> result = handler.handleResponse(node, keys, response);
        assertEquals(emptyMap(), result.completedKeys);
        return result;
    }

    private void assertLookup(FenceProducersHandler handler, String transactionalId) {
        CoordinatorKey key = CoordinatorKey.byTransactionalId(transactionalId);
        InitProducerIdRequest.Builder request = handler.buildSingleRequest(1, key);
        assertEquals(transactionalId, request.data.transactionalId());
        assertEquals(1, request.data.transactionTimeoutMs());
    }
}
