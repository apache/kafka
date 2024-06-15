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

import java.util.Optional;
import java.util.Properties;

import static org.apache.kafka.test.TestUtils.requiredConsumerConfig;
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
        final RequestManagers requestManagers = RequestManagers.supplier(
            new MockTime(),
            new LogContext(),
            mock(BackgroundEventHandler.class),
            mock(ConsumerMetadata.class),
            mock(SubscriptionState.class),
            mock(FetchBuffer.class),
            config,
            groupRebalanceConfig,
            mock(ApiVersions.class),
            mock(FetchMetricsManager.class),
            () -> mock(NetworkClientDelegate.class),
            Optional.empty(),
            new Metrics(),
            mock(OffsetCommitCallbackInvoker.class),
            listener
        ).get();
        requestManagers.membershipManager.ifPresent(
            membershipManager -> assertTrue(((MembershipManagerImpl) membershipManager).stateListeners().contains(listener))
        );
    }
}