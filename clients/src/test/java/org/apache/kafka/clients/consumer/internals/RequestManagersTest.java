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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.test.TestUtils.requiredConsumerConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class RequestManagersTest {

    @Test
    public void testMemberStateListenerRegistered() {

        final MemberStateListener listener = (memberEpoch, memberId) -> { };

        final Properties properties = requiredConsumerConfig();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroup");
        final ConsumerConfig config = new ConsumerConfig(properties);
        final GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            config,
            GroupRebalanceConfig.ProtocolType.CONSUMER
        );
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        ConsumerMetadata metadata = mock(ConsumerMetadata.class);
        SubscriptionState subscriptions = mock(SubscriptionState.class);
        ApiVersions apiVersions = mock(ApiVersions.class);
        final RequestManagers requestManagers = RequestManagers.supplier(
            time,
            logContext,
            mock(BackgroundEventHandler.class),
            metadata,
            subscriptions,
            mock(FetchBuffer.class),
            config,
            groupRebalanceConfig,
            apiVersions,
            mock(FetchMetricsManager.class),
            () -> mock(NetworkClientDelegate.class),
            Optional.empty(),
            new Metrics(),
            mock(OffsetCommitCallbackInvoker.class),
            listener,
            Optional.empty(),
            new PositionsValidator(logContext, time, subscriptions, metadata)
        ).get();
        assertTrue(requestManagers.consumerMembershipManager.isPresent());
        assertTrue(requestManagers.streamsMembershipManager.isEmpty());
        assertTrue(requestManagers.streamsGroupHeartbeatRequestManager.isEmpty());

        assertEquals(2, requestManagers.consumerMembershipManager.get().stateListeners().size());
        assertTrue(requestManagers.consumerMembershipManager.get().stateListeners().stream()
            .anyMatch(m -> m instanceof CommitRequestManager));
        assertTrue(requestManagers.consumerMembershipManager.get().stateListeners().contains(listener));
    }

    @Test
    public void testStreamMemberStateListenerRegistered() {

        final MemberStateListener listener = (memberEpoch, memberId) -> { };

        final Properties properties = requiredConsumerConfig();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroup");
        final ConsumerConfig config = new ConsumerConfig(properties);
        final GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            config,
            GroupRebalanceConfig.ProtocolType.CONSUMER
        );
        LogContext logContext = new LogContext();
        MockTime time = new MockTime();
        ConsumerMetadata metadata = mock(ConsumerMetadata.class);
        SubscriptionState subscriptions = mock(SubscriptionState.class);
        ApiVersions apiVersions = mock(ApiVersions.class);
        final RequestManagers requestManagers = RequestManagers.supplier(
            time,
            logContext,
            mock(BackgroundEventHandler.class),
            metadata,
            subscriptions,
            mock(FetchBuffer.class),
            config,
            groupRebalanceConfig,
            apiVersions,
            mock(FetchMetricsManager.class),
            () -> mock(NetworkClientDelegate.class),
            Optional.empty(),
            new Metrics(),
            mock(OffsetCommitCallbackInvoker.class),
            listener,
            Optional.of(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of())),
            new PositionsValidator(logContext, time, subscriptions, metadata)
        ).get();
        assertTrue(requestManagers.streamsMembershipManager.isPresent());
        assertTrue(requestManagers.streamsGroupHeartbeatRequestManager.isPresent());
        assertTrue(requestManagers.consumerMembershipManager.isEmpty());

        assertEquals(2, requestManagers.streamsMembershipManager.get().stateListeners().size());
        assertTrue(requestManagers.streamsMembershipManager.get().stateListeners().stream()
            .anyMatch(m -> m instanceof CommitRequestManager));
        assertTrue(requestManagers.streamsMembershipManager.get().stateListeners().contains(listener));
    }
}
