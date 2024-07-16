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
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
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
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createShareFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.createSubscriptionState;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.mockito.Mockito.spy;

@SuppressWarnings("ClassDataAbstractionCoupling")
public class ShareConsumerTestBuilder implements Closeable {

    static final long DEFAULT_RETRY_BACKOFF_MS = 80;
    static final long DEFAULT_RETRY_BACKOFF_MAX_MS = 1000;
    static final int DEFAULT_REQUEST_TIMEOUT_MS = 500;
    static final int DEFAULT_MAX_POLL_INTERVAL_MS = 10000;
    static final String DEFAULT_GROUP_ID = "group-id";
    static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 1000;
    static final double DEFAULT_HEARTBEAT_JITTER_MS = 0.0;

    final LogContext logContext = new LogContext();
    final Time time;
    public final BlockingQueue<ApplicationEvent> applicationEventQueue;
    public final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    final ConsumerConfig config;
    final long retryBackoffMs;
    final SubscriptionState subscriptions;
    final ConsumerMetadata metadata;
    final FetchConfig fetchConfig;
    final Metrics metrics;
    final Timer pollTimer;
    final ShareFetchMetricsManager metricsManager;
    final NetworkClientDelegate networkClientDelegate;
    final Optional<ShareConsumeRequestManager> shareConsumeRequestManager;
    final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    final Optional<ShareHeartbeatRequestManager> heartbeatRequestManager;
    final Optional<ShareMembershipManager> shareMembershipManager;
    final Optional<ShareHeartbeatRequestManager.HeartbeatState> heartbeatState;
    final Optional<ShareHeartbeatRequestManager.HeartbeatRequestState> heartbeatRequestState;
    final RequestManagers requestManagers;
    public final ApplicationEventProcessor applicationEventProcessor;
    public final BackgroundEventHandler backgroundEventHandler;
    final MockClient client;
    final GroupInformation groupInfo;

    public ShareConsumerTestBuilder(GroupInformation groupInfo) {
        this(groupInfo, true);
    }

    public ShareConsumerTestBuilder(GroupInformation groupInfo, boolean enableAutoTick) {
        this.groupInfo = groupInfo;
        this.time = enableAutoTick ? new MockTime(1) : new MockTime();
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventHandler = spy(new BackgroundEventHandler(backgroundEventQueue));

        Properties properties = new Properties();
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, DEFAULT_REQUEST_TIMEOUT_MS);
        properties.put(SESSION_TIMEOUT_MS_CONFIG, 100);
        properties.put(MAX_POLL_INTERVAL_MS_CONFIG, DEFAULT_MAX_POLL_INTERVAL_MS);
        properties.put(HEARTBEAT_INTERVAL_MS_CONFIG, DEFAULT_HEARTBEAT_INTERVAL_MS);
        properties.put(GROUP_ID_CONFIG, groupInfo.groupId);
        properties.put(RETRY_BACKOFF_MS_CONFIG, DEFAULT_RETRY_BACKOFF_MS);
        properties.put(RETRY_BACKOFF_MAX_MS_CONFIG, DEFAULT_RETRY_BACKOFF_MAX_MS);
        properties.put("internal.leave.group.on.close", true);

        this.config = new ConsumerConfig(properties);
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(
                config,
                GroupRebalanceConfig.ProtocolType.SHARE);

        this.fetchConfig = new FetchConfig(config);
        this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
        this.metrics = createMetrics(config, time);

        this.subscriptions = spy(createSubscriptionState(config, logContext));
        this.metadata = spy(new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners()));
        this.metricsManager = createShareFetchMetricsManager(metrics);
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
                client,
                metadata,
                backgroundEventHandler));

        ShareFetchBuffer fetchBuffer = new ShareFetchBuffer(logContext);
        ShareConsumeRequestManager consumeRequestManager = spy(new ShareConsumeRequestManager(
                time,
                logContext,
                groupRebalanceConfig.groupId,
                metadata,
                subscriptions,
                fetchConfig,
                fetchBuffer,
                backgroundEventHandler,
                metricsManager,
                retryBackoffMs,
                retryBackoffMs));

        ShareMembershipManager membershipManager = spy(new ShareMembershipManager(
                logContext,
                groupInfo.groupId,
                null,
                subscriptions,
                metadata,
                Optional.empty(),
                time,
                metrics));

        CoordinatorRequestManager coordinator = spy(new CoordinatorRequestManager(
                logContext,
                DEFAULT_RETRY_BACKOFF_MS,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                backgroundEventHandler,
                groupInfo.groupId));

        ShareHeartbeatRequestManager.HeartbeatState heartbeatState = spy(new ShareHeartbeatRequestManager.HeartbeatState(
                subscriptions,
                membershipManager));
        ShareHeartbeatRequestManager.HeartbeatRequestState heartbeatRequestState = spy(new ShareHeartbeatRequestManager.HeartbeatRequestState(
                logContext,
                time,
                groupInfo.heartbeatIntervalMs,
                retryBackoffMs,
                DEFAULT_RETRY_BACKOFF_MAX_MS,
                groupInfo.heartbeatJitterMs));
        ShareHeartbeatRequestManager heartbeat = spy(new ShareHeartbeatRequestManager(
                logContext,
                pollTimer,
                config,
                coordinator,
                membershipManager,
                heartbeatState,
                heartbeatRequestState,
                backgroundEventHandler,
                metrics));

        this.shareConsumeRequestManager = Optional.of(consumeRequestManager);
        this.coordinatorRequestManager = Optional.of(coordinator);
        this.heartbeatRequestManager = Optional.of(heartbeat);
        this.heartbeatState = Optional.of(heartbeatState);
        this.heartbeatRequestState = Optional.of(heartbeatRequestState);
        this.shareMembershipManager = Optional.of(membershipManager);

        this.requestManagers = new RequestManagers(logContext,
                consumeRequestManager,
                coordinatorRequestManager,
                heartbeatRequestManager,
                shareMembershipManager);
        this.applicationEventProcessor = spy(new ApplicationEventProcessor(
                        logContext,
                        requestManagers,
                        metadata,
                        subscriptions
                )
        );
    }

    @Override
    public void close() {
        closeQuietly(requestManagers, RequestManagers.class.getSimpleName());
    }

    public static class GroupInformation {
        final String groupId;
        final int heartbeatIntervalMs;
        final double heartbeatJitterMs;

        public GroupInformation(String groupId) {
            this(groupId, DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_HEARTBEAT_JITTER_MS);
        }

        public GroupInformation(String groupId, int heartbeatIntervalMs, double heartbeatJitterMs) {
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            this.heartbeatJitterMs = heartbeatJitterMs;
            this.groupId = groupId;
        }
    }

    static GroupInformation createDefaultGroupInformation() {
        return new GroupInformation(DEFAULT_GROUP_ID);
    }
}
