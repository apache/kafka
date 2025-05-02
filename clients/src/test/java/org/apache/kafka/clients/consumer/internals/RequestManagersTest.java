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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.requiredConsumerConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
            listener,
            Optional.empty()
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
            listener,
            Optional.of(new StreamsRebalanceData(UUID.randomUUID(), Optional.empty(), Map.of(), Map.of()))
        ).get();
        assertTrue(requestManagers.streamsMembershipManager.isPresent());
        assertTrue(requestManagers.streamsGroupHeartbeatRequestManager.isPresent());
        assertTrue(requestManagers.consumerMembershipManager.isEmpty());

        assertEquals(2, requestManagers.streamsMembershipManager.get().stateListeners().size());
        assertTrue(requestManagers.streamsMembershipManager.get().stateListeners().stream()
            .anyMatch(m -> m instanceof CommitRequestManager));
        assertTrue(requestManagers.streamsMembershipManager.get().stateListeners().contains(listener));
    }

    /**
     * Test that the runtime checks in {@link RequestManagers#ENTRIES_MAPPER} work as expected.
     */
    @ParameterizedTest
    @MethodSource("testListOfSource")
    public void testListOf(int expectedSize, List<Object> listOfArguments) {
        List<RequestManager> requestManagers = listOfArguments.stream()
            .map(RequestManagers.ENTRIES_MAPPER)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

        assertEquals(expectedSize, requestManagers.size());
    }

    /**
     * Test that the runtime checks in {@link RequestManagers#ENTRIES_MAPPER} work as expected.
     */
    @ParameterizedTest
    @MethodSource("testListOfChecksSource")
    public void testListOfChecks(Class<?> unexpectedClass, List<Object> listOfArguments) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> listOfArguments.stream()
            .map(RequestManagers.ENTRIES_MAPPER)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList()));

        String expectedMessage = String.format(
            "Objects passed to listOf() must be %s or %s, not %s",
            Optional.class.getName(),
            RequestManager.class.getName(),
            unexpectedClass.getName()
        );

        assertEquals(expectedMessage, e.getMessage());
    }

    private static Stream<Arguments> testListOfSource() {
        return Stream.of(
            Arguments.of(0, List.of(Optional.empty())),
            Arguments.of(1, List.of(mock(RequestManager.class))),
            Arguments.of(2, List.of(Optional.empty(), mock(RequestManager.class), Optional.of(mock(RequestManager.class))))
        );
    }

    private static Stream<Arguments> testListOfChecksSource() {
        return Stream.of(
            Arguments.of(String.class, List.of("Whoops! A String!")),
            Arguments.of(Integer.class, List.of(Optional.of(1999)))
        );
    }
}
