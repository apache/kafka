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

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NetworkClientDelegateTest {
    private MockTime time;
    private Properties properties;
    private LogContext logContext;
    private KafkaClient client;
    private String groupId;
    private SubscriptionState subscription;
    private ConsumerMetadata metadata;
    private Node node;
    private int requestTimeoutMs = 500;

    @BeforeEach
    public void setup() {
        this.time = new MockTime(0);
        this.properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, 100);
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        this.logContext = new LogContext();
        this.client = mock(NetworkClient.class);
        this.node = mockNode();
        this.groupId = "group-1";
    }

    @Test
    public void testPoll() {
        NetworkClientDelegate ncd = mockNetworkClientDelegate();

        // Successful case
        NetworkClientDelegate.DefaultRequestFutureCompletionHandler callback = mock(NetworkClientDelegate.DefaultRequestFutureCompletionHandler.class);
        NetworkClientDelegate.UnsentRequest r = mockUnsentFindCoordinatorRequest(callback);
        ncd.add(r);
        when(this.client.leastLoadedNode(time.milliseconds())).thenReturn(this.node);
        when(this.client.isReady(this.node, time.milliseconds())).thenReturn(true);
        ncd.poll(100, time.milliseconds());
        verify(client, times(1)).send(any(), eq(time.milliseconds()));
        verify(callback, never()).onFailure(any());

        // Timeout case
        NetworkClientDelegate.DefaultRequestFutureCompletionHandler callback2 = mock(NetworkClientDelegate.DefaultRequestFutureCompletionHandler.class);
        NetworkClientDelegate.UnsentRequest r2 = mockUnsentFindCoordinatorRequest(callback2);
        ncd.add(r2);
        time.sleep(501);
        when(this.client.leastLoadedNode(time.milliseconds())).thenReturn(this.node);
        ncd.poll(100, time.milliseconds());
        verify(client, never()).send(any(), eq(time.milliseconds()));
        verify(callback2, times(1)).onFailure(any());

        // Node found but not ready: first loop (the request should get re-enqueued into the unsentRequests
        NetworkClientDelegate.DefaultRequestFutureCompletionHandler callback3 = mock(NetworkClientDelegate.DefaultRequestFutureCompletionHandler.class);
        NetworkClientDelegate.UnsentRequest r3 = mockUnsentFindCoordinatorRequest(callback3);
        ncd.add(r3);
        when(this.client.leastLoadedNode(time.milliseconds())).thenReturn(this.node);
        when(this.client.isReady(this.node, time.milliseconds())).thenReturn(false);
        ncd.poll(100, time.milliseconds());
        verify(client, never()).send(any(), eq(time.milliseconds()));
        verify(callback3, never()).onFailure(any());

        // The request expires in 500ms.
        time.sleep(499);
        when(this.client.leastLoadedNode(time.milliseconds())).thenReturn(this.node);
        when(this.client.isReady(this.node, time.milliseconds())).thenReturn(true);
        ncd.poll(100, time.milliseconds());
        verify(client, times(1)).send(any(), eq(time.milliseconds()));
        verify(callback3, never()).onFailure(any());
    }

    @Test
    public void testUnableToFindBrokerAndTimeout() {
        NetworkClientDelegate ncd = mockNetworkClientDelegate();

        NetworkClientDelegate.DefaultRequestFutureCompletionHandler callback = mock(NetworkClientDelegate.DefaultRequestFutureCompletionHandler.class);
        NetworkClientDelegate.UnsentRequest r = mockUnsentFindCoordinatorRequest(callback);
        ncd.add(r);
        final long timeoutMs = 100;
        long totalTimeoutMs = 0;
        while (totalTimeoutMs <= this.requestTimeoutMs) {
            when(this.client.leastLoadedNode(time.milliseconds())).thenReturn(null);
            ncd.poll(timeoutMs, time.milliseconds());
            totalTimeoutMs += timeoutMs;
            this.time.sleep(timeoutMs);
        }
        verify(client, times(6)).poll(eq(timeoutMs), anyLong());
        verify(callback, times(1)).onFailure(isA(TimeoutException.class));
    }

    @Test
    public void testNodeUnready() {
        NetworkClientDelegate ncd = mockNetworkClientDelegate();

        NetworkClientDelegate.DefaultRequestFutureCompletionHandler callback = mock(NetworkClientDelegate.DefaultRequestFutureCompletionHandler.class);
        NetworkClientDelegate.UnsentRequest r = mockUnsentFindCoordinatorRequest(callback);
        ncd.add(r);
        final long timeoutMs = 100;
        long totalTimeoutMs = 0;
        while (totalTimeoutMs <= this.requestTimeoutMs) {
            when(this.client.leastLoadedNode(time.milliseconds())).thenReturn(this.node);
            when(this.client.isReady(this.node, time.milliseconds())).thenReturn(false);
            ncd.poll(timeoutMs, time.milliseconds());
            totalTimeoutMs += timeoutMs;
            this.time.sleep(timeoutMs);
        }
        verify(client, times(6)).poll(eq(timeoutMs), anyLong());
        verify(callback, times(1)).onFailure(isA(TimeoutException.class));
    }

    public NetworkClientDelegate mockNetworkClientDelegate() {
        return new NetworkClientDelegate(this.time, new ConsumerConfig(this.properties), this.logContext, this.client);
    }

    public NetworkClientDelegate.UnsentRequest mockUnsentFindCoordinatorRequest(NetworkClientDelegate.AbstractRequestFutureCompletionHandler callback) {
        Objects.requireNonNull(groupId);
        NetworkClientDelegate.UnsentRequest req = new NetworkClientDelegate.UnsentRequest(
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKey(this.groupId)
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.GROUP.id())),
                callback);
        req.setTimer(this.time, this.requestTimeoutMs);
        return req;
    }

    private Node mockNode() {
        return new Node(0, "localhost", 99);
    }
}
