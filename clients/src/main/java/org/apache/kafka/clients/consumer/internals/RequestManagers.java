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
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.common.internals.IdempotentCloser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.closeQuietly;

import static java.util.Objects.requireNonNull;

/**
 * {@code RequestManagers} provides a means to pass around the set of {@link RequestManager} instances in the system.
 * This allows callers to both use the specific {@link RequestManager} instance, or to iterate over the list via
 * the {@link #entries()} method.
 */
public class RequestManagers implements Closeable {

    private final Logger log;
    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    public final Optional<CommitRequestManager> commitRequestManager;
    public final Optional<HeartbeatRequestManager> heartbeatRequestManager;
    public final OffsetsRequestManager offsetsRequestManager;
    public final TopicMetadataRequestManager topicMetadataRequestManager;
    public final FetchRequestManager fetchRequestManager;
    private final List<Optional<? extends RequestManager>> entries;
    private final IdempotentCloser closer = new IdempotentCloser();

    public RequestManagers(LogContext logContext,
                           OffsetsRequestManager offsetsRequestManager,
                           TopicMetadataRequestManager topicMetadataRequestManager,
                           FetchRequestManager fetchRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager,
                           Optional<HeartbeatRequestManager> heartbeatRequestManager) {
        this.log = logContext.logger(RequestManagers.class);
        this.offsetsRequestManager = requireNonNull(offsetsRequestManager, "OffsetsRequestManager cannot be null");
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = commitRequestManager;
        this.topicMetadataRequestManager = topicMetadataRequestManager;
        this.fetchRequestManager = fetchRequestManager;
        this.heartbeatRequestManager = heartbeatRequestManager;

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(commitRequestManager);
        list.add(heartbeatRequestManager);
        list.add(Optional.of(offsetsRequestManager));
        list.add(Optional.of(topicMetadataRequestManager));
        list.add(Optional.of(fetchRequestManager));
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
     * {@link ConsumerNetworkThread}.
     */
    public static Supplier<RequestManagers> supplier(final Time time,
                                                     final LogContext logContext,
                                                     final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                                     final ConsumerMetadata metadata,
                                                     final SubscriptionState subscriptions,
                                                     final FetchBuffer fetchBuffer,
                                                     final ConsumerConfig config,
                                                     final GroupRebalanceConfig groupRebalanceConfig,
                                                     final ApiVersions apiVersions,
                                                     final FetchMetricsManager fetchMetricsManager,
                                                     final Supplier<NetworkClientDelegate> networkClientDelegateSupplier) {
        return new CachedSupplier<RequestManagers>() {
            @Override
            protected RequestManagers create() {
                final NetworkClientDelegate networkClientDelegate = networkClientDelegateSupplier.get();
                final BackgroundEventHandler backgroundEventHandler = new BackgroundEventHandler(logContext, backgroundEventQueue);
                final FetchConfig fetchConfig = new FetchConfig(config);
                long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                long retryBackoffMaxMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
                final int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                final OffsetsRequestManager listOffsets = new OffsetsRequestManager(subscriptions,
                        metadata,
                        fetchConfig.isolationLevel,
                        time,
                        retryBackoffMs,
                        requestTimeoutMs,
                        apiVersions,
                        networkClientDelegate,
                        backgroundEventHandler,
                        logContext);
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
                        config);
                HeartbeatRequestManager heartbeatRequestManager = null;
                CoordinatorRequestManager coordinator = null;
                CommitRequestManager commit = null;

                if (groupRebalanceConfig != null && groupRebalanceConfig.groupId != null) {
                    final GroupState groupState = new GroupState(groupRebalanceConfig);
                    coordinator = new CoordinatorRequestManager(time,
                            logContext,
                            retryBackoffMs,
                            retryBackoffMaxMs,
                            backgroundEventHandler,
                            groupState.groupId);
                    commit = new CommitRequestManager(time, logContext, subscriptions, config, coordinator, groupState);
                    MembershipManager membershipManager = new MembershipManagerImpl(groupState.groupId, logContext);
                    heartbeatRequestManager = new HeartbeatRequestManager(
                            logContext,
                            time,
                            config,
                            coordinator,
                            subscriptions,
                            membershipManager,
                            backgroundEventHandler);
                }

                return new RequestManagers(
                        logContext,
                        listOffsets,
                        topic,
                        fetch,
                        Optional.ofNullable(coordinator),
                        Optional.ofNullable(commit),
                        Optional.ofNullable(heartbeatRequestManager)
                );
            }
        };
    }
}
