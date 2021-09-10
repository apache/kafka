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
import org.apache.kafka.common.requests.EpochEndOffset;
import org.apache.kafka.common.requests.OffsetsForLeaderEpochResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class OffsetForLeaderEpochClientTest {

    private ConsumerNetworkClient consumerClient;
    private SubscriptionState subscriptions;
    private Metadata metadata;
    private MockClient client;
    private Time time;

    private TopicPartition tp0 = new TopicPartition("topic", 0);

    @Test
    public void testEmptyResponse() {
        OffsetsForLeaderEpochClient offsetClient = newOffsetClient();
        RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future =
                offsetClient.sendAsyncRequest(Node.noNode(), Collections.emptyMap());

        OffsetsForLeaderEpochResponse resp = new OffsetsForLeaderEpochResponse(Collections.emptyMap());
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        OffsetsForLeaderEpochClient.OffsetForEpochResult result = future.value();
        assertTrue(result.partitionsToRetry().isEmpty());
        assertTrue(result.endOffsets().isEmpty());
    }

    @Test
    public void testUnexpectedEmptyResponse() {
        Map<TopicPartition, SubscriptionState.FetchPosition> positionMap = new HashMap<>();
        positionMap.put(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1),
                new Metadata.LeaderAndEpoch(Node.noNode(), Optional.of(1))));

        OffsetsForLeaderEpochClient offsetClient = newOffsetClient();
        RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future =
                offsetClient.sendAsyncRequest(Node.noNode(), positionMap);

        OffsetsForLeaderEpochResponse resp = new OffsetsForLeaderEpochResponse(Collections.emptyMap());
        client.prepareResponse(resp);
        consumerClient.pollNoWakeup();

        OffsetsForLeaderEpochClient.OffsetForEpochResult result = future.value();
        assertFalse(result.partitionsToRetry().isEmpty());
        assertTrue(result.endOffsets().isEmpty());
    }

    @Test
    public void testOkResponse() {
        Map<TopicPartition, SubscriptionState.FetchPosition> positionMap = new HashMap<>();
        positionMap.put(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1),
                new Metadata.LeaderAndEpoch(Node.noNode(), Optional.of(1))));

        OffsetsForLeaderEpochClient offsetClient = newOffsetClient();
        RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future =
                offsetClient.sendAsyncRequest(Node.noNode(), positionMap);

        Map<TopicPartition, EpochEndOffset> endOffsetMap = new HashMap<>();
        endOffsetMap.put(tp0, new EpochEndOffset(Errors.NONE, 1, 10L));
        client.prepareResponse(new OffsetsForLeaderEpochResponse(endOffsetMap));
        consumerClient.pollNoWakeup();

        OffsetsForLeaderEpochClient.OffsetForEpochResult result = future.value();
        assertTrue(result.partitionsToRetry().isEmpty());
        assertTrue(result.endOffsets().containsKey(tp0));
        assertEquals(result.endOffsets().get(tp0).error(), Errors.NONE);
        assertEquals(result.endOffsets().get(tp0).leaderEpoch(), 1);
        assertEquals(result.endOffsets().get(tp0).endOffset(), 10L);
    }

    @Test
    public void testUnauthorizedTopic() {
        Map<TopicPartition, SubscriptionState.FetchPosition> positionMap = new HashMap<>();
        positionMap.put(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1),
                new Metadata.LeaderAndEpoch(Node.noNode(), Optional.of(1))));

        OffsetsForLeaderEpochClient offsetClient = newOffsetClient();
        RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future =
                offsetClient.sendAsyncRequest(Node.noNode(), positionMap);

        Map<TopicPartition, EpochEndOffset> endOffsetMap = new HashMap<>();
        endOffsetMap.put(tp0, new EpochEndOffset(Errors.TOPIC_AUTHORIZATION_FAILED, -1, -1));
        client.prepareResponse(new OffsetsForLeaderEpochResponse(endOffsetMap));
        consumerClient.pollNoWakeup();

        assertTrue(future.failed());
        assertEquals(future.exception().getClass(), TopicAuthorizationException.class);
        assertTrue(((TopicAuthorizationException) future.exception()).unauthorizedTopics().contains(tp0.topic()));
    }

    @Test
    public void testRetriableError() {
        Map<TopicPartition, SubscriptionState.FetchPosition> positionMap = new HashMap<>();
        positionMap.put(tp0, new SubscriptionState.FetchPosition(0, Optional.of(1),
                new Metadata.LeaderAndEpoch(Node.noNode(), Optional.of(1))));

        OffsetsForLeaderEpochClient offsetClient = newOffsetClient();
        RequestFuture<OffsetsForLeaderEpochClient.OffsetForEpochResult> future =
                offsetClient.sendAsyncRequest(Node.noNode(), positionMap);

        Map<TopicPartition, EpochEndOffset> endOffsetMap = new HashMap<>();
        endOffsetMap.put(tp0, new EpochEndOffset(Errors.LEADER_NOT_AVAILABLE, -1, -1));
        client.prepareResponse(new OffsetsForLeaderEpochResponse(endOffsetMap));
        consumerClient.pollNoWakeup();

        assertFalse(future.failed());
        OffsetsForLeaderEpochClient.OffsetForEpochResult result = future.value();
        assertTrue(result.partitionsToRetry().contains(tp0));
        assertFalse(result.endOffsets().containsKey(tp0));
    }

    private OffsetsForLeaderEpochClient newOffsetClient() {
        buildDependencies(OffsetResetStrategy.EARLIEST);
        return new OffsetsForLeaderEpochClient(consumerClient, new LogContext());
    }

    private void buildDependencies(OffsetResetStrategy offsetResetStrategy) {
        LogContext logContext = new LogContext();
        time = new MockTime(1);
        subscriptions = new SubscriptionState(logContext, offsetResetStrategy);
        metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
    }
}
