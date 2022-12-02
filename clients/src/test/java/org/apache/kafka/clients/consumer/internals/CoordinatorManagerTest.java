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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CoordinatorManagerTest {
    private MockTime time;
    private MockClient client;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private LogContext logContext;
    private ErrorEventHandler errorEventHandler;
    private Node node;
    private final Properties properties = new Properties();
    private Optional<String> groupId;
    private int rebalanceTimeoutMs;
    private int requestTimeoutMs;
    private CoordinatorRequestManager.CoordinatorRequestState coordinatorRequestState;


    @BeforeEach
    public void setup() {
        this.logContext = new LogContext();
        this.time = new MockTime(0);
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false,
                false, subscriptions, logContext, new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, Collections.singletonMap("topic", 1)));
        this.node = metadata.fetch().nodes().get(0);
        this.errorEventHandler = mock(ErrorEventHandler.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, "100");
        this.groupId = Optional.of("");
        this.rebalanceTimeoutMs = 60 * 1000;
        this.requestTimeoutMs = 500;
        this.coordinatorRequestState = mock(CoordinatorRequestManager.CoordinatorRequestState.class);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, 100);
    }
    
    @Test
    public void testPoll() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager();
        when(coordinatorRequestState.canSendRequest(time.milliseconds())).thenReturn(true);
        NetworkClientDelegate.PollResult res = coordinatorManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        when(coordinatorRequestState.canSendRequest(time.milliseconds())).thenReturn(false);
        NetworkClientDelegate.PollResult res2 = coordinatorManager.poll(time.milliseconds());
        assertTrue(res2.unsentRequests.isEmpty());
    }

    @Test
    public void testOnResponse() {
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager();
        FindCoordinatorResponse resp = FindCoordinatorResponse.prepareResponse(Errors.NONE, "key", node);
        coordinatorManager.onResponse(resp);
        verify(errorEventHandler, never()).handle(any());

        FindCoordinatorResponse errResp = FindCoordinatorResponse.prepareResponse(Errors.COORDINATOR_NOT_AVAILABLE,
                "key-1", node);
        coordinatorManager.onResponse(errResp);
        verify(errorEventHandler, times(1)).handle(Errors.COORDINATOR_NOT_AVAILABLE.exception());
    }

    @Test
    public void testCoordinatorRequestState() {
        ConsumerConfig config = new ConsumerConfig(properties);
        CoordinatorRequestManager.CoordinatorRequestState state = new CoordinatorRequestManager.CoordinatorRequestState(
                100,
                2,
                1000,
                0);

        // ensure not permitting consecutive requests
        assertTrue(state.canSendRequest(0));
        state.updateLastSend(0);
        assertFalse(state.canSendRequest(0));
        state.updateLastFailedAttempt(35);
        assertTrue(state.canSendRequest(135));
        state.updateLastFailedAttempt(140);
        assertFalse(state.canSendRequest(200));
        // exponential backoff
        assertTrue(state.canSendRequest(340));

        // test reset
        state.reset();
        assertTrue(state.canSendRequest(200));
    }

    @Test
    public void testFindCoordinatorBackoff() {
        this.coordinatorRequestState = new CoordinatorRequestManager.CoordinatorRequestState(
                100,
                2,
                1000,
                0);
        CoordinatorRequestManager coordinatorManager = setupCoordinatorManager();

        NetworkClientDelegate.PollResult res = coordinatorManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        coordinatorManager.onResponse(
                FindCoordinatorResponse.prepareResponse(Errors.CLUSTER_AUTHORIZATION_FAILED, "key",
                this.node));
        // Need to wait for 100ms until the next send
        res = coordinatorManager.poll(time.milliseconds());
        assertTrue(res.unsentRequests.isEmpty());
        this.time.sleep(50);
        res = coordinatorManager.poll(time.milliseconds());
        assertTrue(res.unsentRequests.isEmpty());
        this.time.sleep(50);
        // should be able to send after 100ms
        res = coordinatorManager.poll(time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        coordinatorManager.onResponse(
                FindCoordinatorResponse.prepareResponse(Errors.NONE, "key",
                        this.node));
    }

    @Test
    public void testRequestFutureCompletionHandler() {
        NetworkClientDelegate.AbstractRequestFutureCompletionHandler h = new MockRequestFutureCompletionHandlerBase();
        try {
            h.onFailure(new RuntimeException());
        } catch (Exception e) {
            assertEquals("MockRequestFutureCompletionHandlerBase should throw an exception", e.getMessage());
        }
    }

    private static class MockRequestFutureCompletionHandlerBase extends NetworkClientDelegate.AbstractRequestFutureCompletionHandler {
        @Override
        public void handleResponse(ClientResponse r, Throwable t) {
            throw new RuntimeException("MockRequestFutureCompletionHandlerBase should throw an exception");
        }
    }
    
    private CoordinatorRequestManager setupCoordinatorManager() {
        return new CoordinatorRequestManager(
                this.time,
                this.logContext,
                this.errorEventHandler,
                this.groupId,
                this.rebalanceTimeoutMs,
                this.requestTimeoutMs,
                this.coordinatorRequestState);
    }

    private NetworkClientDelegate.PollResult mockPollResult(long time, NetworkClientDelegate.UnsentRequest req) {
        return new NetworkClientDelegate.PollResult(
                time,
                Collections.singletonList(req));
    }
}
