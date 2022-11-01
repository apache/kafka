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

import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.MockCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitBackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.buildRebalanceConfig;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.groupCoordinatorResponse;
import static org.apache.kafka.clients.consumer.internals.AsyncCoordinatorTestUtils.prepareOffsetCommitRequest;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultBackgroundThreadTest {
    private static final long REFRESH_BACK_OFF_MS = 100;
    private final Properties properties = new Properties();
    private MockTime time;
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private LogContext context;
    private ConsumerNetworkClient consumerClient;
    private Metrics metrics;
    private BlockingQueue<BackgroundEvent> backgroundEventsQueue;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private DefaultAsyncCoordinator coordinator;
    private MockClient client;
    private final String testTopic = "test-topic";
    private final TopicPartition testTopicPartition = new TopicPartition(testTopic, 0);
    private MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
        {
            put(testTopic, 1);
        }
    });
    private Node node = metadataResponse.brokers().iterator().next();
    private LogContext logContext;
    private int sessionTimeoutMs = 1000;
    private int rebalanceTimeoutMs = 100;
    private int heartbeatIntervalMs = 2;
    private int retryBackoffMs = 100;
    private int defaultApiTimeoutMs = 60000;
    private int requestTimeoutMs = defaultApiTimeoutMs / 2;
    private int maxPollIntervalMs = 900;


    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        this.time = new MockTime();
        this.subscriptions = mock(SubscriptionState.class);
        this.metadata = mock(ConsumerMetadata.class);
        this.context = new LogContext();
        this.consumerClient = mock(ConsumerNetworkClient.class);
        this.metrics = mock(Metrics.class);
        this.applicationEventsQueue = (BlockingQueue<ApplicationEvent>) mock(BlockingQueue.class);
        this.backgroundEventsQueue = (BlockingQueue<BackgroundEvent>) mock(BlockingQueue.class);
        this.coordinator = mock(DefaultAsyncCoordinator.class);
        this.logContext = new LogContext();
        this.client = new MockClient(time);
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, REFRESH_BACK_OFF_MS);
    }

    @Test
    public void testStartupAndTearDown() throws InterruptedException {
        final MockClient client = new MockClient(time, metadata);
        this.consumerClient = new ConsumerNetworkClient(
            context,
            client,
            metadata,
            time,
            100,
            1000,
            100
        );
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        final DefaultBackgroundThread backgroundThread = setupMockHandler();
        backgroundThread.start();
        assertTrue(client.active());
        backgroundThread.close();
        assertFalse(client.active());
    }

    @Test
    public void testInterruption() {
        final MockClient client = new MockClient(time, metadata);
        this.consumerClient = new ConsumerNetworkClient(
            context,
            client,
            metadata,
            time,
            100,
            1000,
            100
        );
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        final DefaultBackgroundThread backgroundThread = setupMockHandler();
        backgroundThread.start();
        assertTrue(client.active());
        backgroundThread.close();
        assertFalse(client.active());
    }

    @Test
    void testWakeup() {
        this.time = new MockTime(0);
        final MockClient client = new MockClient(time, metadata);
        this.consumerClient = new ConsumerNetworkClient(
            context,
            client,
            metadata,
            time,
            100,
            1000,
            100
        );
        when(applicationEventsQueue.isEmpty()).thenReturn(true);
        when(applicationEventsQueue.isEmpty()).thenReturn(true);
        final DefaultBackgroundThread runnable = setupMockHandler();
        client.poll(0, time.milliseconds());
        runnable.wakeup();

        assertThrows(WakeupException.class, runnable::runOnce);
        runnable.close();
    }

    @Test
    void testNetworkAndBlockingQueuePoll() {
        // ensure network poll and application queue poll will happen in a
        // single iteration
        this.time = new MockTime(100);
        final DefaultBackgroundThread runnable = setupMockHandler();
        runnable.runOnce();

        when(applicationEventsQueue.isEmpty()).thenReturn(false);
        when(applicationEventsQueue.poll())
            .thenReturn(new NoopApplicationEvent(backgroundEventsQueue, "nothing"));
        final InOrder inOrder = Mockito.inOrder(applicationEventsQueue, this.consumerClient);
        assertFalse(inOrder.verify(applicationEventsQueue).isEmpty());
        inOrder.verify(applicationEventsQueue).poll();
        inOrder.verify(this.consumerClient).poll(any(Timer.class));
        runnable.close();
    }

    @Test
    void testCommitEventInvokedCoordinatorOnce() {
        this.time = new MockTime();
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        DefaultBackgroundThread runnable = setupMockHandler();
        Map<TopicPartition, OffsetAndMetadata> offset = singletonMap(testTopicPartition,
                new OffsetAndMetadata(100L, "hello"));
        OffsetCommitCallback callback = new MockCommitCallback();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offset, callback);
        assertTrue(applicationEventsQueue.add(commitEvent));

        runnable.runOnce();
        verify(this.coordinator, times(1)).commitOffsets(offset, Optional.of(callback), commitEvent.commitFuture);
        verify(this.consumerClient, times(1)).poll(any(Timer.class));
        runnable.close();
    }

    private static Collection<Arguments> commitErrorParameters() {
        List<Arguments> arguments = new ArrayList<>();
        arguments.add(Arguments.of(Errors.NONE));
        arguments.add(Arguments.of(Errors.COORDINATOR_NOT_AVAILABLE));
        arguments.add(Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION));
        return arguments;
    }

    @ParameterizedTest(name = "commitError={0}")
    @MethodSource("commitErrorParameters")
    void testCoordinatorResponse(Errors commitError) {
        this.time = new MockTime();
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        String groupId = "group-id";
        this.coordinator = setupCoordinator(groupId, "group-instance-id");
        DefaultBackgroundThread runnable = setupMockHandler();
        Map<TopicPartition, OffsetAndMetadata> offset = singletonMap(testTopicPartition,
                new OffsetAndMetadata(100L, "hello"));
        OffsetCommitCallback callback = new MockCommitCallback();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offset, callback);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));

        // successful commit event
        assertTrue(applicationEventsQueue.add(commitEvent));
        prepareOffsetCommitRequest(singletonMap(testTopicPartition, 100L), commitError, client);
        runnable.runOnce();
        assertFalse(backgroundEventsQueue.isEmpty());
        BackgroundEvent event = backgroundEventsQueue.poll();
        assertTrue(event instanceof CommitBackgroundEvent);
        assertDoesNotThrow(() -> ((CommitBackgroundEvent) event).invokeCallback());
    }

    @Test
    void testCoordinatorThrowsFencedInstanceException() {
        this.time = new MockTime();
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        this.backgroundEventsQueue = new LinkedBlockingQueue<>();
        String groupId = "group-id";
        this.coordinator = setupCoordinator(groupId, "group-instance-id");
        DefaultBackgroundThread runnable = setupMockHandler();
        Map<TopicPartition, OffsetAndMetadata> offset = singletonMap(testTopicPartition,
                new OffsetAndMetadata(100L, "hello"));
        OffsetCommitCallback callback = new MockCommitCallback();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offset, callback);

        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE, groupId));

        // fenced instance id error
        assertTrue(applicationEventsQueue.add(commitEvent));
        prepareOffsetCommitRequest(singletonMap(testTopicPartition, 100L), Errors.FENCED_INSTANCE_ID, client);
        runnable.runOnce();
        assertFalse(backgroundEventsQueue.isEmpty());
        BackgroundEvent fencedEvent = backgroundEventsQueue.poll();
        assertTrue(fencedEvent instanceof CommitBackgroundEvent);
        assertThrows(FencedInstanceIdException.class,
                () -> ((CommitBackgroundEvent) fencedEvent).invokeCallback());
    }

    private DefaultBackgroundThread setupMockHandler() {
        return new DefaultBackgroundThread(
                this.time,
                new ConsumerConfig(properties),
                this.logContext,
                this.applicationEventsQueue,
                this.backgroundEventsQueue,
                this.subscriptions,
                this.metadata,
                this.consumerClient,
                this.coordinator,
                this.metrics
        );
    }

    private DefaultAsyncCoordinator setupCoordinator(String groupId, String groupInstanceId) {
        Objects.requireNonNull(groupInstanceId);
        Objects.requireNonNull(groupId);
        GroupRebalanceConfig rebalanceConfig = buildRebalanceConfig(
                sessionTimeoutMs,
                rebalanceTimeoutMs,
                heartbeatIntervalMs,
                groupId,
                Optional.of(groupInstanceId),
                retryBackoffMs);
        ConsumerMetadata metadata = new ConsumerMetadata(
                retryBackoffMs,
                60 * 60 * 1000L,
                false,
                false,
                new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST),
                logContext,
                new ClusterResourceListeners());
        this.client = new MockClient(time, metadata);
        this.consumerClient = new ConsumerNetworkClient(
                this.logContext,
                this.client,
                this.metadata,
                this.time,
                this.retryBackoffMs,
                this.requestTimeoutMs,
                this.maxPollIntervalMs);
        this.metrics = new Metrics(this.time);
        this.client.updateMetadata(RequestTestUtils.metadataUpdateWith(1, emptyMap()));
        this.node = metadata.fetch().nodes().get(0);
        return new DefaultAsyncCoordinator(
                this.time,
                this.logContext,
                rebalanceConfig,
                this.consumerClient,
                this.subscriptions,
                this.backgroundEventsQueue,
                this.metrics,
                "metric_grp_prefix");
    }
}
