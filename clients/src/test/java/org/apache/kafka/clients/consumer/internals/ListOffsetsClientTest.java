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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.IsolationLevel;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.apache.kafka.test.TestUtils.assertOptional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("deprecation")
public class ListOffsetsClientTest {

    private ConsumerNetworkClient consumerClient;
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private MockClient client;
    private Time time;

    private TopicPartition tp0 = new TopicPartition("topic", 0);

    @Test
    public void testEmptyResponse() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.emptyMap(), false, IsolationLevel.READ_COMMITTED));

        ListOffsetResponse resp = new ListOffsetResponse(Collections.emptyMap());
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        ListOffsetsClient.ResultData result = future.value();
        assertTrue(result.partitionsToRetry.isEmpty());
        assertTrue(result.fetchedOffsets.isEmpty());
    }

    @Test
    public void testUnexpectedEmptyResponse() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.singletonMap(tp0, new ListOffsetRequest.PartitionData(-1, Optional.of(0))),
                    false, IsolationLevel.READ_COMMITTED));

        ListOffsetResponse resp = new ListOffsetResponse(Collections.emptyMap());
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        ListOffsetsClient.ResultData result = future.value();
        assertFalse(result.partitionsToRetry.isEmpty());
        assertTrue(result.partitionsToRetry.contains(tp0));
        assertTrue(result.fetchedOffsets.isEmpty());
    }

    @Test
    public void testOkV0Response() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.singletonMap(tp0, new ListOffsetRequest.PartitionData(-1, Optional.empty())),
                false, IsolationLevel.READ_COMMITTED));

        ListOffsetResponse resp = new ListOffsetResponse(Collections.singletonMap(tp0,
            new ListOffsetResponse.PartitionData(Errors.NONE, Collections.singletonList(10L))));
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        ListOffsetsClient.ResultData result = future.value();
        assertTrue(result.partitionsToRetry.isEmpty());
        assertFalse(result.fetchedOffsets.isEmpty());
        assertEquals(result.fetchedOffsets.get(tp0).offset, 10L);
        assertEquals(result.fetchedOffsets.get(tp0).leaderEpoch, Optional.empty());
        assertNull(result.fetchedOffsets.get(tp0).timestamp);
    }

    @Test
    public void testOkV1Response() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.singletonMap(tp0, new ListOffsetRequest.PartitionData(-1, Optional.of(1))),
                false, IsolationLevel.READ_COMMITTED));

        ListOffsetResponse resp = new ListOffsetResponse(Collections.singletonMap(tp0,
            new ListOffsetResponse.PartitionData(Errors.NONE, -1, 10L, Optional.of(1))));
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        ListOffsetsClient.ResultData result = future.value();
        assertTrue(result.partitionsToRetry.isEmpty());
        assertFalse(result.fetchedOffsets.isEmpty());
        assertEquals(result.fetchedOffsets.get(tp0).offset, 10L);
        assertOptional(result.fetchedOffsets.get(tp0).leaderEpoch, epoch -> assertEquals(epoch.longValue(), 1));
        assertEquals(result.fetchedOffsets.get(tp0).timestamp.longValue(), -1);
    }


    @Test
    public void testErrorResponse() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.singletonMap(tp0, new ListOffsetRequest.PartitionData(-1, Optional.of(1))),
                false, IsolationLevel.READ_COMMITTED));

        ListOffsetResponse resp = new ListOffsetResponse(Collections.singletonMap(tp0,
            new ListOffsetResponse.PartitionData(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, Optional.empty())));
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        ListOffsetsClient.ResultData result = future.value();
        assertTrue(result.fetchedOffsets.isEmpty());
        assertFalse(result.partitionsToRetry.isEmpty());
    }

    @Test
    public void testTimestampsNotSupportedByBroker() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.singletonMap(tp0, new ListOffsetRequest.PartitionData(-1, Optional.of(1))),
                false, IsolationLevel.READ_COMMITTED));

        // When we get this error, we treat it as a valid empty response (no offsets returned)
        ListOffsetResponse resp = new ListOffsetResponse(Collections.singletonMap(tp0,
            new ListOffsetResponse.PartitionData(Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT, -1, -1, Optional.empty())));
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        ListOffsetsClient.ResultData result = future.value();
        assertTrue(result.fetchedOffsets.isEmpty());
        assertTrue(result.partitionsToRetry.isEmpty());
    }

    @Test
    public void testUnauthorizedTopic() {
        ListOffsetsClient offsetClient = newClient();
        RequestFuture<ListOffsetsClient.ResultData> future = offsetClient.sendAsyncRequest(
            Node.noNode(), new ListOffsetsClient.RequestData(
                Collections.singletonMap(tp0, new ListOffsetRequest.PartitionData(-1, Optional.of(1))),
                false, IsolationLevel.READ_COMMITTED));

        ListOffsetResponse resp = new ListOffsetResponse(Collections.singletonMap(tp0,
            new ListOffsetResponse.PartitionData(Errors.TOPIC_AUTHORIZATION_FAILED, -1, -1, Optional.empty())));
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        assertTrue(future.failed());
        assertEquals(future.exception().getClass(), TopicAuthorizationException.class);
        assertTrue(((TopicAuthorizationException) future.exception()).unauthorizedTopics().contains(tp0.topic()));
    }

    private ListOffsetsClient newClient() {
        buildDependencies(OffsetResetStrategy.EARLIEST);
        return new ListOffsetsClient(consumerClient, new LogContext());
    }

    private void buildDependencies(OffsetResetStrategy offsetResetStrategy) {
        LogContext logContext = new LogContext();
        time = new MockTime(1);
        subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
        metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
    }
}
