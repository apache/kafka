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
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryProvider;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * {@code RequestManagers} provides a means to pass around the set of {@link RequestManager} instances in the system.
 * This allows callers to both use the specific {@link RequestManager} instance, or to iterate over the list via
 * the {@link #entries()} method.
 */
public class RequestManagers implements Closeable {

    private final Logger log;
    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    public final Optional<CommitRequestManager> commitRequestManager;
    public final Optional<ConsumerHeartbeatRequestManager> consumerHeartbeatRequestManager;
    public final Optional<ShareHeartbeatRequestManager> shareHeartbeatRequestManager;
    public final Optional<ConsumerMembershipManager> consumerMembershipManager;
    public final Optional<ShareMembershipManager> shareMembershipManager;
    public final OffsetsRequestManager offsetsRequestManager;
    public final TopicMetadataRequestManager topicMetadataRequestManager;
    public final FetchRequestManager fetchRequestManager;
    public final Optional<ShareConsumeRequestManager> shareConsumeRequestManager;
    private final List<Optional<? extends RequestManager>> entries;
    private final IdempotentCloser closer = new IdempotentCloser();

    public RequestManagers(LogContext logContext,
                           OffsetsRequestManager offsetsRequestManager,
                           TopicMetadataRequestManager topicMetadataRequestManager,
                           FetchRequestManager fetchRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager,
                           Optional<ConsumerHeartbeatRequestManager> heartbeatRequestManager,
                           Optional<ConsumerMembershipManager> membershipManager) {
        this.log = logContext.logger(RequestManagers.class);
        this.offsetsRequestManager = requireNonNull(offsetsRequestManager, "OffsetsRequestManager cannot be null");
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = commitRequestManager;
        this.topicMetadataRequestManager = topicMetadataRequestManager;
        this.fetchRequestManager = fetchRequestManager;
        this.shareConsumeRequestManager = Optional.empty();
        this.consumerHeartbeatRequestManager = heartbeatRequestManager;
        this.shareHeartbeatRequestManager = Optional.empty();
        this.consumerMembershipManager = membershipManager;
        this.shareMembershipManager = Optional.empty();

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(commitRequestManager);
        list.add(heartbeatRequestManager);
        list.add(membershipManager);
        list.add(Optional.of(offsetsRequestManager));
        list.add(Optional.of(topicMetadataRequestManager));
        list.add(Optional.of(fetchRequestManager));
        entries = Collections.unmodifiableList(list);
    }

    public RequestManagers(LogContext logContext,
                           ShareConsumeRequestManager shareConsumeRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<ShareHeartbeatRequestManager> shareHeartbeatRequestManager,
                           Optional<ShareMembershipManager> shareMembershipManager) {
        this.log = logContext.logger(RequestManagers.class);
        this.shareConsumeRequestManager = Optional.of(shareConsumeRequestManager);
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = Optional.empty();
        this.consumerHeartbeatRequestManager = Optional.empty();
        this.shareHeartbeatRequestManager = shareHeartbeatRequestManager;
        this.consumerMembershipManager = Optional.empty();
        this.shareMembershipManager = shareMembershipManager;
        this.offsetsRequestManager = null;
        this.topicMetadataRequestManager = null;
        this.fetchRequestManager = null;

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(shareHeartbeatRequestManager);
        list.add(shareMembershipManager);
        list.add(Optional.of(shareConsumeRequestManager));
        entries = Collections.unmodifiableList(list);
    }

    public List<Optional<? extends RequestManager>> entries() {
        return entries;
    }

    @Override
    public void close() {
        closer.close(
                () -> {
                    log.debug("Closing RequestManagers");

                    entries.stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .filter(rm -> rm instanceof Closeable)
                            .map(rm -> (Closeable) rm)
                            .forEach(c -> closeQuietly(c, c.getClass().getSimpleName()));
                    log.debug("RequestManagers has been closed");
                },
                () -> log.debug("RequestManagers was already closed")
        );
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link AsyncKafkaConsumer}.
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public static Supplier<RequestManagers> supplier(final Time time,
                                                     final LogContext logContext,
                                                     final BackgroundEventHandler backgroundEventHandler,
                                                     final ConsumerMetadata metadata,
                                                     final SubscriptionState subscriptions,
                                                     final FetchBuffer fetchBuffer,
                                                     final ConsumerConfig config,
                                                     final GroupRebalanceConfig groupRebalanceConfig,
                                                     final ApiVersions apiVersions,
                                                     final FetchMetricsManager fetchMetricsManager,
                                                     final Supplier<NetworkClientDelegate> networkClientDelegateSupplier,
                                                     final Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                                     final Metrics metrics,
                                                     final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker,
                                                     final MemberStateListener applicationThreadMemberStateListener
                                                     ) {
        return new CachedSupplier<>() {
            @Override
            protected RequestManagers create() {
                final NetworkClientDelegate networkClientDelegate = networkClientDelegateSupplier.get();
                final FetchConfig fetchConfig = new FetchConfig(config);
                long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
                final int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                final int defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

                final FetchRequestManager fetch = new FetchRequestManager(logContext,
                        time,
                        metadata,
                        subscriptions,
                        fetchConfig,
                        fetchBuffer,
                        fetchMetricsManager,
                        networkClientDelegate,
                        apiVersions);
                final TopicMetadataRequestManager topic = new TopicMetadataRequestManager(
                        logContext,
                        time,
                        config);
                ConsumerHeartbeatRequestManager heartbeatRequestManager = null;
                ConsumerMembershipManager membershipManager = null;
                CoordinatorRequestManager coordinator = null;
                CommitRequestManager commitRequestManager = null;

                if (groupRebalanceConfig != null && groupRebalanceConfig.groupId != null) {
                    Optional<String> serverAssignor = Optional.ofNullable(config.getString(ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG));
                    coordinator = new CoordinatorRequestManager(
                            logContext,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            backgroundEventHandler,
                            groupRebalanceConfig.groupId);
                    commitRequestManager = new CommitRequestManager(
                            time,
                            logContext,
                            subscriptions,
                            config,
                            coordinator,
                            offsetCommitCallbackInvoker,
                            groupRebalanceConfig.groupId,
                            groupRebalanceConfig.groupInstanceId,
                            metrics,
                            metadata);
                    membershipManager = new ConsumerMembershipManager(
                            groupRebalanceConfig.groupId,
                            groupRebalanceConfig.groupInstanceId,
                            groupRebalanceConfig.rebalanceTimeoutMs,
                            serverAssignor,
                            subscriptions,
                            commitRequestManager,
                            metadata,
                            logContext,
                            backgroundEventHandler,
                            time,
                            metrics);

                    // Update the group member ID label in the client telemetry reporter.
                    // According to KIP-1082, the consumer will generate the member ID as the incarnation ID of the process.
                    // Therefore, we can update the group member ID during initialization.
                    if (clientTelemetryReporter.isPresent()) {
                        clientTelemetryReporter.get()
                            .updateMetricsLabels(Map.of(ClientTelemetryProvider.GROUP_MEMBER_ID, membershipManager.memberId()));
                    }

                    membershipManager.registerStateListener(commitRequestManager);
                    membershipManager.registerStateListener(applicationThreadMemberStateListener);
                    heartbeatRequestManager = new ConsumerHeartbeatRequestManager(
                            logContext,
                            time,
                            config,
                            coordinator,
                            subscriptions,
                            membershipManager,
                            backgroundEventHandler,
                            metrics);
                }

                final OffsetsRequestManager listOffsets = new OffsetsRequestManager(subscriptions,
                    metadata,
                    fetchConfig.isolationLevel,
                    time,
                    retryBackoffMs,
                    requestTimeoutMs,
                    defaultApiTimeoutMs,
                    apiVersions,
                    networkClientDelegate,
                    commitRequestManager,
                    logContext);

                return new RequestManagers(
                        logContext,
                        listOffsets,
                        topic,
                        fetch,
                        Optional.ofNullable(coordinator),
                        Optional.ofNullable(commitRequestManager),
                        Optional.ofNullable(heartbeatRequestManager),
                        Optional.ofNullable(membershipManager)
                );
            }
        };
    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link ShareConsumerImpl}.
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public static Supplier<RequestManagers> supplier(final Time time,
                                                     final LogContext logContext,
                                                     final BackgroundEventHandler backgroundEventHandler,
                                                     final ConsumerMetadata metadata,
                                                     final SubscriptionState subscriptions,
                                                     final ShareFetchBuffer fetchBuffer,
                                                     final ConsumerConfig config,
                                                     final GroupRebalanceConfig groupRebalanceConfig,
                                                     final ShareFetchMetricsManager shareFetchMetricsManager,
                                                     final Optional<ClientTelemetryReporter> clientTelemetryReporter,
                                                     final Metrics metrics
    ) {
        return new CachedSupplier<>() {
            @Override
            protected RequestManagers create() {
                long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
                FetchConfig fetchConfig = new FetchConfig(config);

                CoordinatorRequestManager coordinator = new CoordinatorRequestManager(
                        logContext,
                        retryBackoffMs,
                        retryBackoffMaxMs,
                        backgroundEventHandler,
                        groupRebalanceConfig.groupId);
                ShareMembershipManager shareMembershipManager = new ShareMembershipManager(
                        logContext,
                        groupRebalanceConfig.groupId,
                        null,
                        subscriptions,
                        metadata,
                    time,
                        metrics);

                // Update the group member ID label in the client telemetry reporter.
                // According to KIP-1082, the consumer will generate the member ID as the incarnation ID of the process.
                // Therefore, we can update the group member ID during initialization.
                clientTelemetryReporter.ifPresent(telemetryReporter -> telemetryReporter
                    .updateMetricsLabels(Map.of(ClientTelemetryProvider.GROUP_MEMBER_ID, shareMembershipManager.memberId())));

                ShareHeartbeatRequestManager shareHeartbeatRequestManager = new ShareHeartbeatRequestManager(
                        logContext,
                        time,
                        config,
                        coordinator,
                        subscriptions,
                        shareMembershipManager,
                        backgroundEventHandler,
                        metrics);
                ShareConsumeRequestManager shareConsumeRequestManager = new ShareConsumeRequestManager(
                        time,
                        logContext,
                        groupRebalanceConfig.groupId,
                        metadata,
                        subscriptions,
                        fetchConfig,
                        fetchBuffer,
                        backgroundEventHandler,
                        shareFetchMetricsManager,
                        retryBackoffMs,
                        retryBackoffMaxMs);
                shareMembershipManager.registerStateListener(shareConsumeRequestManager);

                return new RequestManagers(
                        logContext,
                        shareConsumeRequestManager,
                        Optional.of(coordinator),
                        Optional.of(shareHeartbeatRequestManager),
                        Optional.of(shareMembershipManager)
                );
            }
        };
    }
}
