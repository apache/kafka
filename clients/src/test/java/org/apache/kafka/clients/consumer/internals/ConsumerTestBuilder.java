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
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceMetricsManager;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.io.Closeable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_INSTANCE_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class ConsumerTestBuilder implements Closeable {

    static final long DEFAULT_RETRY_BACKOFF_MS = 80;
    static final long DEFAULT_RETRY_BACKOFF_MAX_MS = 1000;
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 500;
    static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    static final String DEFAULT_GROUP_INSTANCE_ID = "group-instance-id";
    static final String DEFAULT_GROUP_ID = "group-id";
    static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
    static final double DEFAULT_HEARTBEAT_JITTER_MS = 0.0;
    static final String DEFAULT_REMOTE_ASSIGNOR = "uniform";

    final LogContext logContext = new LogContext();
    final Time time;
    public final BlockingQueue<ApplicationEvent> applicationEventQueue;
    public final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    final ConsumerConfig config;
    final long retryBackoffMs;
    final SubscriptionState subscriptions;
    final ConsumerMetadata metadata;
    final FetchConfig fetchConfig;
    final FetchBuffer fetchBuffer;
    final Metrics metrics;
    final Timer pollTimer;
    final FetchMetricsManager metricsManager;
    final NetworkClientDelegate networkClientDelegate;
    final OffsetsRequestManager offsetsRequestManager;
    final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    final Optional<CommitRequestManager> commitRequestManager;
    final Optional<HeartbeatRequestManager> heartbeatRequestManager;
    final Optional<MembershipManager> membershipManager;
    final Optional<HeartbeatRequestManager.HeartbeatState> heartbeatState;
    final Optional<HeartbeatRequestManager.HeartbeatRequestState> heartbeatRequestState;
    final TopicMetadataRequestManager topicMetadataRequestManager;
    final FetchRequestManager fetchRequestManager;
    final RequestManagers requestManagers;
    public final ApplicationEventProcessor applicationEventProcessor;
    public final BackgroundEventHandler backgroundEventHandler;
    public final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    final MockClient client;
    final Optional<GroupInformation> groupInfo;
    final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;

    public ConsumerTestBuilder(Optional<GroupInformation> groupInfo) {
        this(groupInfo, true, true);
    }

    public ConsumerTestBuilder(Optional<GroupInformation> groupInfo, boolean enableAutoCommit, boolean enableAutoTick) {
        this.groupInfo = groupInfo;
        this.time = enableAutoTick ? new MockTime(1) : new MockTime();
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventHandler = spy(new BackgroundEventHandler(logContext, backgroundEventQueue));
        this.offsetCommitCallbackInvoker = mock(OffsetCommitCallbackInvoker.class);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
            100,
            DEFAULT_MAX_POLL_INTERVAL_MS,
            DEFAULT_HEARTBEAT_INTERVAL_MS,
            groupInfo.map(gi -> gi.groupId).orElse(null),
            groupInfo.flatMap(gi -> gi.groupInstanceId),
            DEFAULT_RETRY_BACKOFF_MS,
            DEFAULT_RETRY_BACKOFF_MAX_MS,
            true);
        ApiVersions apiVersions = new ApiVersions();

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, DEFAULT_RETRY_BACKOFF_MS);
        properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_REQUEST_TIMEOUT_MS);
        properties.put(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG, DEFAULT_MAX_POLL_INTERVAL_MS);

        if (!enableAutoCommit)
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        groupInfo.ifPresent(gi -> {
            properties.put(GROUP_ID_CONFIG, gi.groupId);
            gi.groupInstanceId.ifPresent(groupInstanceId -> properties.put(GROUP_INSTANCE_ID_CONFIG, groupInstanceId));
        });

        this.config = new ConsumerConfig(properties);

        this.fetchConfig = new FetchConfig(config);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        final long requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.metrics = createMetrics(config, time);

        this.subscriptions = spy(createSubscriptionState(config, logContext));
        this.metadata = spy(new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners()));
        this.metricsManager = createFetchMetricsManager(metrics);
        this.pollTimer = time.timer(groupRebalanceConfig.rebalanceTimeoutMs);

        this.client = new MockClient(time, metadata);
        MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
            {
                String topic1 = "test1";
                put(topic1, 1);
                String topic2 = "test2";
                put(topic2, 1);
            }
        });
        this.client.updateMetadata(metadataResponse);

        this.networkClientDelegate = spy(new NetworkClientDelegate(time,
                config,
                logContext,
                client));
        this.offsetsRequestManager = spy(new OffsetsRequestManager(subscriptions,
                metadata,
                fetchConfig.isolationLevel,
                time,
                retryBackoffMs,
                requestTimeoutMs,
                apiVersions,
                networkClientDelegate,
                backgroundEventHandler,
                logContext));

        this.topicMetadataRequestManager = spy(new TopicMetadataRequestManager(logContext, config));

        if (groupInfo.isPresent()) {
            GroupInformation gi = groupInfo.get();
            CoordinatorRequestManager coordinator = spy(new CoordinatorRequestManager(
                    time,
                    logContext,
                    DEFAULT_RETRY_BACKOFF_MS,
                    DEFAULT_RETRY_BACKOFF_MAX_MS,
                    backgroundEventHandler,
                    gi.groupId
            ));
            CommitRequestManager commit = spy(new CommitRequestManager(time,
                    logContext,
                    subscriptions,
                    config,
                    coordinator,
                    offsetCommitCallbackInvoker,
                    gi.groupId,
                    gi.groupInstanceId,
                    metrics));
            MembershipManager mm = spy(
                new MembershipManagerImpl(
                    gi.groupId,
                    gi.groupInstanceId,
                    groupRebalanceConfig.rebalanceTimeoutMs,
                    gi.serverAssignor,
                    subscriptions,
                    commit,
                    metadata,
                    logContext,
                    Optional.empty(),
                    backgroundEventHandler,
                    time,
                    mock(RebalanceMetricsManager.class)
                )
            );
            HeartbeatRequestManager.HeartbeatState heartbeatState = spy(new HeartbeatRequestManager.HeartbeatState(
                    subscriptions,
                    mm,
                    DEFAULT_MAX_POLL_INTERVAL_MS));
            HeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState = spy(new HeartbeatRequestManager.HeartbeatRequestState(
                    logContext,
                    time,
                    gi.heartbeatIntervalMs,
                    retryBackoffMs,
                    DEFAULT_RETRY_BACKOFF_MAX_MS,
                    gi.heartbeatJitterMs));
            HeartbeatRequestManager heartbeat = spy(new HeartbeatRequestManager(
                    logContext,
                    pollTimer,
                    config,
                    coordinator,
                    mm,
                    heartbeatState,
                    heartbeatRequestState,
                    backgroundEventHandler,
                    metrics));

            this.coordinatorRequestManager = Optional.of(coordinator);
            this.commitRequestManager = Optional.of(commit);
            this.heartbeatRequestManager = Optional.of(heartbeat);
            this.heartbeatState = Optional.of(heartbeatState);
            this.heartbeatRequestState = Optional.of(heartbeatRequestState);
            this.membershipManager = Optional.of(mm);
        } else {
            this.coordinatorRequestManager = Optional.empty();
            this.commitRequestManager = Optional.empty();
            this.heartbeatRequestManager = Optional.empty();
            this.heartbeatState = Optional.empty();
            this.heartbeatRequestState = Optional.empty();
            this.membershipManager = Optional.empty();
        }

        this.fetchBuffer = new FetchBuffer(logContext);
        this.fetchRequestManager = spy(new FetchRequestManager(logContext,
                time,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                metricsManager,
                networkClientDelegate,
                apiVersions));
        this.requestManagers = new RequestManagers(logContext,
                offsetsRequestManager,
                topicMetadataRequestManager,
                fetchRequestManager,
                coordinatorRequestManager,
                commitRequestManager,
                heartbeatRequestManager,
                membershipManager
            );
        this.applicationEventProcessor = spy(new ApplicationEventProcessor(
                logContext,
                applicationEventQueue,
                requestManagers,
                metadata
            )
        );

        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
                logContext,
                subscriptions,
                time,
                new RebalanceCallbackMetricsManager(metrics)
        );
    }

    @Override
    public void close() {
        closeQuietly(requestManagers, RequestManagers.class.getSimpleName());
        closeQuietly(applicationEventProcessor, ApplicationEventProcessor.class.getSimpleName());
    }

    public static class ConsumerNetworkThreadTestBuilder extends ConsumerTestBuilder {

        final ConsumerNetworkThread consumerNetworkThread;

        public ConsumerNetworkThreadTestBuilder() {
            this(createDefaultGroupInformation());
        }

        public ConsumerNetworkThreadTestBuilder(Optional<GroupInformation> groupInfo) {
            super(groupInfo);
            this.consumerNetworkThread = new ConsumerNetworkThread(
                    logContext,
                    time,
                    () -> applicationEventProcessor,
                    () -> networkClientDelegate,
                    () -> requestManagers
            );
        }

        @Override
        public void close() {
            consumerNetworkThread.close(Duration.ZERO);
        }
    }

    public static class GroupInformation {
        final String groupId;
        final Optional<String> groupInstanceId;
        final int heartbeatIntervalMs;
        final double heartbeatJitterMs;
        final Optional<String> serverAssignor;

        public GroupInformation(String groupId, Optional<String> groupInstanceId) {
            this(groupId, groupInstanceId, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_JITTER_MS,
                Optional.of(DEFAULT_REMOTE_ASSIGNOR));
        }

        public GroupInformation(String groupId, Optional<String> groupInstanceId, int heartbeatIntervalMs, double heartbeatJitterMs, Optional<String> serverAssignor) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatJitterMs = heartbeatJitterMs;
            this.serverAssignor = serverAssignor;
            this.groupId = groupId;
            this.groupInstanceId = groupInstanceId;
        }
    }

    static Optional<GroupInformation> createDefaultGroupInformation() {
        return Optional.of(new GroupInformation(DEFAULT_GROUP_ID, Optional.empty()));
    }
}
