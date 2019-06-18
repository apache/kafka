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
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsumerNetworkClientTest {

    private String topicName = "test";
    private MockTime time = new MockTime(1);
    private Cluster cluster = TestUtils.singletonCluster(topicName, 1);
    private Node node = cluster.nodes().get(0);
    private Metadata metadata = new Metadata(100, 50000, new LogContext(),
            new ClusterResourceListeners());
    private MockClient client = new MockClient(time, metadata);
    private ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(new LogContext(),
            client, metadata, time, 100, 1000, Integer.MAX_VALUE);

    @Test
    public void send() {
        client.prepareResponse(heartbeatResponse(Errors.NONE));
        RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        assertFalse(future.isDone());

        consumerClient.poll(future);
        assertTrue(future.isDone());
        assertTrue(future.succeeded());

        ClientResponse clientResponse = future.value();
        HeartbeatResponse response = (HeartbeatResponse) clientResponse.responseBody();
        assertEquals(Errors.NONE, response.error());
    }

    @Test
    public void sendWithinBlackoutPeriodAfterAuthenticationFailure() {
        client.authenticationFailed(node, 300);
        client.prepareResponse(heartbeatResponse(Errors.NONE));
        final RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());
        consumerClient.poll(future);
        assertTrue(future.failed());
        assertTrue("Expected only an authentication error.", future.exception() instanceof AuthenticationException);

        time.sleep(30); // wait less than the blackout period
        assertTrue(client.connectionFailed(node));

        final RequestFuture<ClientResponse> future2 = consumerClient.send(node, heartbeat());
        consumerClient.poll(future2);
        assertTrue(future2.failed());
        assertTrue("Expected only an authentication error.", future2.exception() instanceof AuthenticationException);
    }

    @Test
    public void multiSend() {
        client.prepareResponse(heartbeatResponse(Errors.NONE));
        client.prepareResponse(heartbeatResponse(Errors.NONE));
        RequestFuture<ClientResponse> future1 = consumerClient.send(node, heartbeat());
        RequestFuture<ClientResponse> future2 = consumerClient.send(node, heartbeat());
        assertEquals(2, consumerClient.pendingRequestCount());
        assertEquals(2, consumerClient.pendingRequestCount(node));

        consumerClient.awaitPendingRequests(node, time.timer(Long.MAX_VALUE));
        assertTrue(future1.succeeded());
        assertTrue(future2.succeeded());
    }

    @Test
    public void testDisconnectWithUnsentRequests() {
        RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());
        assertTrue(consumerClient.hasPendingRequests(node));
        assertFalse(client.hasInFlightRequests(node.idString()));
        consumerClient.disconnectAsync(node);
        consumerClient.pollNoWakeup();
        assertTrue(future.failed());
        assertTrue(future.exception() instanceof DisconnectException);
    }

    @Test
    public void testDisconnectWithInFlightRequests() {
        RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());
        consumerClient.pollNoWakeup();
        assertTrue(consumerClient.hasPendingRequests(node));
        assertTrue(client.hasInFlightRequests(node.idString()));
        consumerClient.disconnectAsync(node);
        consumerClient.pollNoWakeup();
        assertTrue(future.failed());
        assertTrue(future.exception() instanceof DisconnectException);
    }

    @Test
    public void testTimeoutUnsentRequest() {
        // Delay connection to the node so that the request remains unsent
        client.delayReady(node, 1000);

        RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat(), 500);
        consumerClient.pollNoWakeup();

        // Ensure the request is pending, but hasn't been sent
        assertTrue(consumerClient.hasPendingRequests());
        assertFalse(client.hasInFlightRequests());

        time.sleep(501);
        consumerClient.pollNoWakeup();

        assertFalse(consumerClient.hasPendingRequests());
        assertTrue(future.failed());
        assertTrue(future.exception() instanceof TimeoutException);
    }

    @Test
    public void doNotBlockIfPollConditionIsSatisfied() {
        NetworkClient mockNetworkClient = mock(NetworkClient.class);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(new LogContext(),
                mockNetworkClient, metadata, time, 100, 1000, Integer.MAX_VALUE);

        // expect poll, but with no timeout
        consumerClient.poll(time.timer(Long.MAX_VALUE), () -> false);
        verify(mockNetworkClient).poll(eq(0L), anyLong());
    }

    @Test
    public void blockWhenPollConditionNotSatisfied() {
        long timeout = 4000L;

        NetworkClient mockNetworkClient = mock(NetworkClient.class);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(new LogContext(),
                mockNetworkClient, metadata, time, 100, 1000, Integer.MAX_VALUE);

        when(mockNetworkClient.inFlightRequestCount()).thenReturn(1);
        consumerClient.poll(time.timer(timeout), () -> true);
        verify(mockNetworkClient).poll(eq(timeout), anyLong());
    }

    @Test
    public void blockOnlyForRetryBackoffIfNoInflightRequests() {
        long retryBackoffMs = 100L;

        NetworkClient mockNetworkClient = mock(NetworkClient.class);
        ConsumerNetworkClient consumerClient = new ConsumerNetworkClient(new LogContext(),
                mockNetworkClient, metadata, time, retryBackoffMs, 1000, Integer.MAX_VALUE);

        when(mockNetworkClient.inFlightRequestCount()).thenReturn(0);

        consumerClient.poll(time.timer(Long.MAX_VALUE), () -> true);

        verify(mockNetworkClient).poll(eq(retryBackoffMs), anyLong());
    }

    @Test
    public void wakeup() {
        RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());
        consumerClient.wakeup();
        try {
            consumerClient.poll(time.timer(0));
            fail();
        } catch (WakeupException e) {
        }

        client.respond(heartbeatResponse(Errors.NONE));
        consumerClient.poll(future);
        assertTrue(future.isDone());
    }

    @Test
    public void testDisconnectWakesUpPoll() throws Exception {
        final RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());

        client.enableBlockingUntilWakeup(1);
        Thread t = new Thread() {
            @Override
            public void run() {
                consumerClient.poll(future);
            }
        };
        t.start();

        consumerClient.disconnectAsync(node);
        t.join();
        assertTrue(future.failed());
        assertTrue(future.exception() instanceof DisconnectException);
    }

    @Test
    public void testAuthenticationExceptionPropagatedFromMetadata() {
        metadata.failedUpdate(time.milliseconds(), new AuthenticationException("Authentication failed"));
        try {
            consumerClient.poll(time.timer(Duration.ZERO));
            fail("Expected authentication error thrown");
        } catch (AuthenticationException e) {
            // After the exception is raised, it should have been cleared
            assertNull(metadata.getAndClearMetadataException());
        }
    }

    @Test(expected = InvalidTopicException.class)
    public void testInvalidTopicExceptionPropagatedFromMetadata() {
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap("topic", Errors.INVALID_TOPIC_EXCEPTION), Collections.emptyMap());
        metadata.update(metadataResponse, time.milliseconds());
        consumerClient.poll(time.timer(Duration.ZERO));
    }

    @Test(expected = TopicAuthorizationException.class)
    public void testTopicAuthorizationExceptionPropagatedFromMetadata() {
        MetadataResponse metadataResponse = TestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap("topic", Errors.TOPIC_AUTHORIZATION_FAILED), Collections.emptyMap());
        metadata.update(metadataResponse, time.milliseconds());
        consumerClient.poll(time.timer(Duration.ZERO));
    }

    @Test
    public void testMetadataFailurePropagated() {
        KafkaException metadataException = new KafkaException();
        metadata.failedUpdate(time.milliseconds(), metadataException);
        try {
            consumerClient.poll(time.timer(Duration.ZERO));
            fail("Expected poll to throw exception");
        } catch (Exception e) {
            assertEquals(metadataException, e);
        }
    }

    @Test
    public void testFutureCompletionOutsidePoll() throws Exception {
        // Tests the scenario in which the request that is being awaited in one thread
        // is received and completed in another thread.

        final RequestFuture<ClientResponse> future = consumerClient.send(node, heartbeat());
        consumerClient.pollNoWakeup(); // dequeue and send the request

        client.enableBlockingUntilWakeup(2);
        Thread t1 = new Thread() {
            @Override
            public void run() {
                consumerClient.pollNoWakeup();
            }
        };
        t1.start();

        // Sleep a little so that t1 is blocking in poll
        Thread.sleep(50);

        Thread t2 = new Thread() {
            @Override
            public void run() {
                consumerClient.poll(future);
            }
        };
        t2.start();

        // Sleep a little so that t2 is awaiting the network client lock
        Thread.sleep(50);

        // Simulate a network response and return from the poll in t1
        client.respond(heartbeatResponse(Errors.NONE));
        client.wakeup();

        // Both threads should complete since t1 should wakeup t2
        t1.join();
        t2.join();
        assertTrue(future.succeeded());
    }

    @Test
    public void testAwaitForMetadataUpdateWithTimeout() {
        assertFalse(consumerClient.awaitMetadataUpdate(time.timer(10L)));
    }

    @Test
    public void sendExpiry() {
        int requestTimeoutMs = 10;
        final AtomicBoolean isReady = new AtomicBoolean();
        final AtomicBoolean disconnected = new AtomicBoolean();
        client = new MockClient(time, metadata) {
            @Override
            public boolean ready(Node node, long now) {
                if (isReady.get())
                    return super.ready(node, now);
                else
                    return false;
            }
            @Override
            public boolean connectionFailed(Node node) {
                return disconnected.get();
            }
        };
        // Queue first send, sleep long enough for this to expire and then queue second send
        consumerClient = new ConsumerNetworkClient(new LogContext(), client, metadata, time, 100, requestTimeoutMs, Integer.MAX_VALUE);
        RequestFuture<ClientResponse> future1 = consumerClient.send(node, heartbeat());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        assertFalse(future1.isDone());

        time.sleep(requestTimeoutMs + 1);
        RequestFuture<ClientResponse> future2 = consumerClient.send(node, heartbeat());
        assertEquals(2, consumerClient.pendingRequestCount());
        assertEquals(2, consumerClient.pendingRequestCount(node));
        assertFalse(future2.isDone());

        // First send should have expired and second send still pending
        consumerClient.poll(time.timer(0));
        assertTrue(future1.isDone());
        assertFalse(future1.succeeded());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        assertFalse(future2.isDone());

        // Enable send, the un-expired send should succeed on poll
        isReady.set(true);
        client.prepareResponse(heartbeatResponse(Errors.NONE));
        consumerClient.poll(future2);
        ClientResponse clientResponse = future2.value();
        HeartbeatResponse response = (HeartbeatResponse) clientResponse.responseBody();
        assertEquals(Errors.NONE, response.error());

        // Disable ready flag to delay send and queue another send. Disconnection should remove pending send
        isReady.set(false);
        RequestFuture<ClientResponse> future3 = consumerClient.send(node, heartbeat());
        assertEquals(1, consumerClient.pendingRequestCount());
        assertEquals(1, consumerClient.pendingRequestCount(node));
        disconnected.set(true);
        consumerClient.poll(time.timer(0));
        assertTrue(future3.isDone());
        assertFalse(future3.succeeded());
        assertEquals(0, consumerClient.pendingRequestCount());
        assertEquals(0, consumerClient.pendingRequestCount(node));
    }

    @Test
    public void testTrySend() {
        final AtomicBoolean isReady = new AtomicBoolean();
        final AtomicInteger checkCount = new AtomicInteger();
        client = new MockClient(time, metadata) {
            @Override
            public boolean ready(Node node, long now) {
                checkCount.incrementAndGet();
                if (isReady.get())
                    return super.ready(node, now);
                else
                    return false;
            }
        };
        consumerClient = new ConsumerNetworkClient(new LogContext(), client, metadata, time, 100, 10, Integer.MAX_VALUE);
        consumerClient.send(node, heartbeat());
        consumerClient.send(node, heartbeat());
        assertEquals(2, consumerClient.pendingRequestCount(node));
        assertEquals(0, client.inFlightRequestCount(node.idString()));

        consumerClient.trySend(time.milliseconds());
        // only check one time when the node doesn't ready
        assertEquals(1, checkCount.getAndSet(0));
        assertEquals(2, consumerClient.pendingRequestCount(node));
        assertEquals(0, client.inFlightRequestCount(node.idString()));

        isReady.set(true);
        consumerClient.trySend(time.milliseconds());
        // check node ready or not for every request
        assertEquals(2, checkCount.getAndSet(0));
        assertEquals(2, consumerClient.pendingRequestCount(node));
        assertEquals(2, client.inFlightRequestCount(node.idString()));
    }

    private HeartbeatRequest.Builder heartbeat() {
        return new HeartbeatRequest.Builder(new HeartbeatRequestData()
                .setGroupId("group")
                .setGenerationId(1)
                .setMemberId("memberId"));
    }

    private HeartbeatResponse heartbeatResponse(Errors error) {
        return new HeartbeatResponse(new HeartbeatResponseData().setErrorCode(error.code()));
    }

}
