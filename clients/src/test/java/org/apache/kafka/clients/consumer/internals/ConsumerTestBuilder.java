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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import java.io.Closeable;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredIsolationLevel;
import static org.mockito.Mockito.spy;

public class ConsumerTestBuilder implements Closeable {

    static final long RETRY_BACKOFF_MS = 80;
    static final int REQUEST_TIMEOUT_MS = 500;

    final LogContext logContext = new LogContext();
    final Time time = new MockTime(1, 0, 0);
    final BlockingQueue<ApplicationEvent> applicationEventQueue;
    final LinkedBlockingQueue<BackgroundEvent> backgroundEventQueue;
    final ConsumerConfig config;
    final SubscriptionState subscriptions;
    final ConsumerMetadata metadata;
    final FetchConfig<String, String> fetchConfig;
    final FetchMetricsManager metricsManager;
    final NetworkClientDelegate networkClientDelegate;
    final ListOffsetsRequestManager listOffsetsRequestManager;
    final TopicMetadataRequestManager topicMetadataRequestManager;
    final CoordinatorRequestManager coordinatorRequestManager;
    final CommitRequestManager commitRequestManager;
    final FetchRequestManager<String, String> fetchRequestManager;
    final RequestManagers<String, String> requestManagers;
    final ApplicationEventProcessor<String, String> applicationEventProcessor;

    public ConsumerTestBuilder() {
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        ErrorEventHandler errorEventHandler = new ErrorEventHandler(backgroundEventQueue);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                100,
                100,
                100,
                "group_id",
                Optional.empty(),
                RETRY_BACKOFF_MS,
                true);
        GroupState groupState = new GroupState(groupRebalanceConfig);
        ApiVersions apiVersions = new ApiVersions();

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, RETRY_BACKOFF_MS);
        properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);

        this.config = new ConsumerConfig(properties);
        IsolationLevel isolationLevel = getConfiguredIsolationLevel(config);
        final long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        final long requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        Metrics metrics = createMetrics(config, time);

        this.subscriptions = createSubscriptionState(config, logContext);
        this.metadata = spy(new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners()));
        this.fetchConfig = createFetchConfig(config);
        this.metricsManager = createFetchMetricsManager(metrics);

        KafkaClient client = new MockClient(time, Collections.singletonList(new Node(0, "localhost", 99)));
        this.networkClientDelegate = spy(new NetworkClientDelegate(time,
                config,
                logContext,
                client));
        this.listOffsetsRequestManager = spy(new ListOffsetsRequestManager(subscriptions,
                metadata,
                isolationLevel,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                apiVersions,
                logContext));
        this.topicMetadataRequestManager = spy(new TopicMetadataRequestManager(logContext, config));
        this.coordinatorRequestManager = spy(new CoordinatorRequestManager(time,
                logContext,
                RETRY_BACKOFF_MS,
                errorEventHandler,
                "group_id"));
        this.commitRequestManager = spy(new CommitRequestManager(time,
                logContext,
                subscriptions,
                config,
                coordinatorRequestManager,
                groupState));
        this.fetchRequestManager = spy(new FetchRequestManager<>(logContext,
                time,
                errorEventHandler,
                metadata,
                subscriptions,
                fetchConfig,
                metricsManager,
                networkClientDelegate));
        this.requestManagers = new RequestManagers<>(logContext,
                listOffsetsRequestManager,
                topicMetadataRequestManager,
                fetchRequestManager,
                Optional.of(coordinatorRequestManager),
                Optional.of(commitRequestManager));
        this.applicationEventProcessor = spy(new ApplicationEventProcessor<>(
                backgroundEventQueue,
                requestManagers,
                metadata,
                logContext));
    }

    @Override
    public void close() {
        requestManagers.close();
    }

    public static class DefaultBackgroundThreadTestBuilder extends ConsumerTestBuilder {

        final DefaultBackgroundThread<String, String> backgroundThread;

        public DefaultBackgroundThreadTestBuilder() {
            this.backgroundThread = new DefaultBackgroundThread<>(
                    time,
                    logContext,
                    applicationEventQueue,
                    () -> applicationEventProcessor,
                    () -> networkClientDelegate,
                    () -> requestManagers);
        }

        @Override
        public void close() {
            backgroundThread.close();
        }
    }

    public static class DefaultEventHandlerTestBuilder extends ConsumerTestBuilder {

        final EventHandler eventHandler;

        public DefaultEventHandlerTestBuilder() {
            this.eventHandler = spy(new DefaultEventHandler<>(
                    time,
                    logContext,
                    applicationEventQueue,
                    backgroundEventQueue,
                    () -> applicationEventProcessor,
                    () -> networkClientDelegate,
                    () -> requestManagers));
        }

        @Override
        public void close() {
            eventHandler.close();
        }
    }

    public static class PrototypeAsyncConsumerTestBuilder extends DefaultEventHandlerTestBuilder {

        final PrototypeAsyncConsumer<String, String> consumer;

        public PrototypeAsyncConsumerTestBuilder(Optional<String> groupIdOpt) {
            FetchCollector<String, String> fetchCollector = new FetchCollector<>(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    metricsManager,
                    time);
            this.consumer = spy(new PrototypeAsyncConsumer<>(logContext,
                    time,
                    eventHandler,
                    groupIdOpt,
                    subscriptions,
                    3000,
                    metadata,
                    new ConsumerInterceptors<>(Collections.emptyList()),
                    new FetchBuffer<>(logContext),
                    fetchCollector));
        }

        @Override
        public void close() {
            consumer.close();
        }
    }
}
