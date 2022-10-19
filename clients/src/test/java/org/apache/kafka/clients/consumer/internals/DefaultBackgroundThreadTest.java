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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.NoopApplicationEvent;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
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
    public void testInterruption() throws InterruptedException {
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

    private DefaultBackgroundThread setupMockHandler() {
        return new DefaultBackgroundThread(
            this.time,
            new ConsumerConfig(properties),
            new LogContext(),
            applicationEventsQueue,
            backgroundEventsQueue,
            this.subscriptions,
            this.metadata,
            this.consumerClient,
            this.metrics
        );
    }
}
