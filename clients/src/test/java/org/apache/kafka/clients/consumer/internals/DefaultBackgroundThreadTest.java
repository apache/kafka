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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class DefaultBackgroundThreadTest {
    private static final long REFRESH_BACK_OFF_MS = 100;
    private final Properties properties = new Properties();
    private MockTime time;
    private ConsumerMetadata metadata;
    private NetworkClientDelegate networkClient;
    private BlockingQueue<BackgroundEvent> backgroundEventsQueue;
    private BlockingQueue<ApplicationEvent> applicationEventsQueue;
    private ApplicationEventProcessor processor;
    private CoordinatorManager coordinatorManager;
    private DefaultBackgroundThread backgroundThread;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setup() {
        this.time = new MockTime(0);
        this.metadata = mock(ConsumerMetadata.class);
        this.networkClient = mock(NetworkClientDelegate.class);
        this.applicationEventsQueue = (BlockingQueue<ApplicationEvent>) mock(BlockingQueue.class);
        this.backgroundEventsQueue = (BlockingQueue<BackgroundEvent>) mock(BlockingQueue.class);
        this.processor = mock(ApplicationEventProcessor.class);
        this.coordinatorManager = mock(CoordinatorManager.class);
        this.backgroundThread = mockBackgroundThread();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(RETRY_BACKOFF_MS_CONFIG, REFRESH_BACK_OFF_MS);
        this.backgroundThread = mockBackgroundThread();
    }

    @AfterEach
    public void tearDown() {
        this.backgroundThread.close();
    }

    @Test
    public void testStartupAndTearDown() {
        this.applicationEventsQueue = new LinkedBlockingQueue<>();
        backgroundThread.start();
        backgroundThread.close();
    }

    @Test
    void testWakeup() {
        backgroundThread.wakeup();
        assertThrows(WakeupException.class, backgroundThread::runOnce);
        backgroundThread.close();
    }

    @Test
    void testFindCoordinator() {
        when(this.coordinatorManager.tryFindCoordinator()).thenReturn(Optional.of(findCoordinatorUnsentRequest(this.time.timer(0))));
        backgroundThread.runOnce();
        Mockito.verify(coordinatorManager, times(1)).tryFindCoordinator();
        Mockito.verify(networkClient, times(1)).poll();
    }

    private static NetworkClientDelegate.UnsentRequest findCoordinatorUnsentRequest(Timer timer) {
        return new NetworkClientDelegate.UnsentRequest(
                timer,
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                                .setKeyType(FindCoordinatorRequest.CoordinatorType.TRANSACTION.id())
                                .setKey("foobar")),
                null);
    }

    private DefaultBackgroundThread mockBackgroundThread() {
        return new DefaultBackgroundThread(
                this.time,
                new ConsumerConfig(properties),
                new LogContext(),
                applicationEventsQueue,
                backgroundEventsQueue,
                processor,
                this.metadata,
                this.networkClient,
                this.coordinatorManager);
    }
}
