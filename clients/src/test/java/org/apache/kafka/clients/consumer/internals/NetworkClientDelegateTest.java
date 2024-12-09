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
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetworkClientDelegateTest {
    private static final int REQUEST_TIMEOUT_MS = 5000;
    private static final String GROUP_ID = "group";
    private static final long DEFAULT_REQUEST_TIMEOUT_MS = 500;
    private MockTime time;
    private MockClient client;
    private Metadata metadata;
    private BackgroundEventHandler backgroundEventHandler;

    @BeforeEach
    public void setup() {
        this.time = new MockTime(0);
        this.metadata = mock(Metadata.class);
        this.backgroundEventHandler = mock(BackgroundEventHandler.class);
        this.client = new MockClient(time, Collections.singletonList(mockNode()));
    }

    @Test
    void testPollResultTimer() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate(false)) {
            NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                    new FindCoordinatorRequest.Builder(
                            new FindCoordinatorRequestData()
                                    .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                    .setKey("foobar")),
                    Optional.empty());
            req.setTimer(time, DEFAULT_REQUEST_TIMEOUT_MS);

            // purposely setting a non-MAX time to ensure it is returning Long.MAX_VALUE upon success
            NetworkClientDelegate.PollResult success = new NetworkClientDelegate.PollResult(
                    10,
                    Collections.singletonList(req));
            assertEquals(10, ncd.addAll(success));

            NetworkClientDelegate.PollResult failure = new NetworkClientDelegate.PollResult(
                    10,
                    new ArrayList<>());
            assertEquals(10, ncd.addAll(failure));
        }
    }

    @Test
    public void testSuccessfulResponse() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate(false)) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            prepareFindCoordinatorResponse(Errors.NONE);

            ncd.add(unsentRequest);
            ncd.poll(0, time.milliseconds());

            assertTrue(unsentRequest.future().isDone());
            assertNotNull(unsentRequest.future().get());
        }
    }

    @Test
    public void testTimeoutBeforeSend() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate(false)) {
            client.setUnreachable(mockNode(), REQUEST_TIMEOUT_MS);
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            ncd.add(unsentRequest);
            ncd.poll(0, time.milliseconds());
            time.sleep(REQUEST_TIMEOUT_MS);
            ncd.poll(0, time.milliseconds());
            assertTrue(unsentRequest.future().isDone());
            TestUtils.assertFutureThrows(unsentRequest.future(), TimeoutException.class);
        }
    }

    @Test
    public void testTimeoutAfterSend() throws Exception {
        try (NetworkClientDelegate ncd = newNetworkClientDelegate(false)) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            ncd.add(unsentRequest);
            ncd.poll(0, time.milliseconds());
            time.sleep(REQUEST_TIMEOUT_MS);
            ncd.poll(0, time.milliseconds());
            assertTrue(unsentRequest.future().isDone());
            TestUtils.assertFutureThrows(unsentRequest.future(), DisconnectException.class);
        }
    }

    @Test
    public void testEnsureCorrectCompletionTimeOnFailure() {
        NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
        long timeMs = time.milliseconds();
        unsentRequest.handler().onFailure(timeMs, new TimeoutException());

        time.sleep(100);
        assertEquals(timeMs, unsentRequest.handler().completionTimeMs());
    }

    @Test
    public void testEnsureCorrectCompletionTimeOnComplete() {
        NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
        long timeMs = time.milliseconds();
        final ClientResponse response = mock(ClientResponse.class);
        when(response.receivedTimeMs()).thenReturn(timeMs);
        unsentRequest.handler().onComplete(response);
        time.sleep(100);
        assertEquals(timeMs, unsentRequest.handler().completionTimeMs());
    }

    @Test
    public void testEnsureTimerSetOnAdd() {
        NetworkClientDelegate ncd = newNetworkClientDelegate(false);
        NetworkClientDelegate.UnsentRequest findCoordRequest = newUnsentFindCoordinatorRequest();
        assertNull(findCoordRequest.timer());

        // NetworkClientDelegate#add
        ncd.add(findCoordRequest);
        assertEquals(1, ncd.unsentRequests().size());
        assertEquals(REQUEST_TIMEOUT_MS, ncd.unsentRequests().poll().timer().timeoutMs());

        // NetworkClientDelegate#addAll
        ncd.addAll(Collections.singletonList(findCoordRequest));
        assertEquals(1, ncd.unsentRequests().size());
        assertEquals(REQUEST_TIMEOUT_MS, ncd.unsentRequests().poll().timer().timeoutMs());
    }

    @Test
    public void testHasAnyPendingRequests() throws Exception {
        try (NetworkClientDelegate networkClientDelegate = newNetworkClientDelegate(false)) {
            NetworkClientDelegate.UnsentRequest unsentRequest = newUnsentFindCoordinatorRequest();
            networkClientDelegate.add(unsentRequest);

            // unsent
            assertTrue(networkClientDelegate.hasAnyPendingRequests());
            assertFalse(networkClientDelegate.unsentRequests().isEmpty());
            assertFalse(client.hasInFlightRequests());

            networkClientDelegate.poll(0, time.milliseconds());

            // in-flight
            assertTrue(networkClientDelegate.hasAnyPendingRequests());
            assertTrue(networkClientDelegate.unsentRequests().isEmpty());
            assertTrue(client.hasInFlightRequests());

            client.respond(FindCoordinatorResponse.prepareResponse(Errors.NONE, GROUP_ID, mockNode()));
            networkClientDelegate.poll(0, time.milliseconds());

            // get response
            assertFalse(networkClientDelegate.hasAnyPendingRequests());
            assertTrue(networkClientDelegate.unsentRequests().isEmpty());
            assertFalse(client.hasInFlightRequests());
        }
    }

    @Test
    public void testPropagateMetadataError() {
        AuthenticationException authException = new AuthenticationException("Test Auth Exception");
        doThrow(authException).when(metadata).maybeThrowAnyException();

        NetworkClientDelegate networkClientDelegate = newNetworkClientDelegate(false);
        assertTrue(networkClientDelegate.getAndClearMetadataError().isEmpty());
        networkClientDelegate.poll(0, time.milliseconds());

        Optional<Exception> metadataError = networkClientDelegate.getAndClearMetadataError();
        assertTrue(metadataError.isPresent());
        assertInstanceOf(AuthenticationException.class, metadataError.get());
        assertEquals(authException.getMessage(), metadataError.get().getMessage());
    }

    @Test
    public void testPropagateMetadataErrorWithErrorEvent() {
        AuthenticationException authException = new AuthenticationException("Test Auth Exception");
        doThrow(authException).when(metadata).maybeThrowAnyException();

        LinkedList<BackgroundEvent> backgroundEventQueue = new LinkedList<>();
        this.backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);
        NetworkClientDelegate networkClientDelegate = newNetworkClientDelegate(true);

        assertEquals(0, backgroundEventQueue.size());
        networkClientDelegate.poll(0, time.milliseconds());
        assertEquals(1, backgroundEventQueue.size());

        BackgroundEvent event = backgroundEventQueue.poll();
        assertNotNull(event);
        assertEquals(BackgroundEvent.Type.ERROR, event.type());
        assertEquals(authException, ((ErrorEvent) event).error());
    }

    public NetworkClientDelegate newNetworkClientDelegate(boolean notifyMetadataErrorsViaErrorQueue) {
        LogContext logContext = new LogContext();
        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(GROUP_ID_CONFIG, GROUP_ID);
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        return new NetworkClientDelegate(this.time,
                new ConsumerConfig(properties),
                logContext,
                this.client,
                this.metadata,
                this.backgroundEventHandler,
                notifyMetadataErrorsViaErrorQueue);
    }

    public NetworkClientDelegate.UnsentRequest newUnsentFindCoordinatorRequest() {
        Objects.requireNonNull(GROUP_ID);
        return new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(new FindCoordinatorRequestData()
                    .setKey(GROUP_ID)
                    .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())
                ),
            Optional.empty()
        );
    }

    public void prepareFindCoordinatorResponse(Errors error) {
        FindCoordinatorResponse findCoordinatorResponse =
            FindCoordinatorResponse.prepareResponse(error, GROUP_ID, mockNode());
        client.prepareResponse(findCoordinatorResponse);
    }

    private Node mockNode() {
        return new Node(0, "localhost", 99);
    }
}