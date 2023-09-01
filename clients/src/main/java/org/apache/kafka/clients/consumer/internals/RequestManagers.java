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
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
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

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredIsolationLevel;

/**
 * {@code RequestManagers} provides a means to pass around the set of {@link RequestManager} instances in the system.
 * This allows callers to both use the specific {@link RequestManager} instance, or to iterate over the list via
 * the {@link #entries()} method.
 */
public class RequestManagers<K, V> implements Closeable {

    private final Logger log;
    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    public final Optional<CommitRequestManager> commitRequestManager;
    public final OffsetsRequestManager offsetsRequestManager;
    public final TopicMetadataRequestManager topicMetadataRequestManager;
    public final FetchRequestManager<K, V> fetchRequestManager;

    private final List<Optional<? extends RequestManager>> entries;
    private final IdempotentCloser closer = new IdempotentCloser();

    public RequestManagers(LogContext logContext,
                           OffsetsRequestManager offsetsRequestManager,
                           TopicMetadataRequestManager topicMetadataRequestManager,
                           FetchRequestManager<K, V> fetchRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager) {
        this.log = logContext.logger(RequestManagers.class);
        this.offsetsRequestManager = requireNonNull(offsetsRequestManager, "OffsetsRequestManager cannot be null");
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = commitRequestManager;
        this.topicMetadataRequestManager = topicMetadataRequestManager;
        this.fetchRequestManager = fetchRequestManager;

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(commitRequestManager);
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

                    entries.forEach(rm -> {
                        rm.ifPresent(requestManager -> {
                            try {
                                requestManager.close();
                            } catch (Throwable t) {
                                log.debug("Error closing request manager {}", requestManager.getClass().getSimpleName(), t);
                            }
                        });
                    });
                    log.debug("RequestManagers has been closed");
                },
                () -> log.debug("RequestManagers was already closed"));

    }

    /**
     * Creates a {@link Supplier} for deferred creation during invocation by
     * {@link org.apache.kafka.clients.consumer.internals.DefaultBackgroundThread}.
     */
    public static <K, V> Supplier<RequestManagers<K, V>> supplier(final Time time,
                                                                  final LogContext logContext,
                                                                  final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                                                  final ConsumerMetadata metadata,
                                                                  final SubscriptionState subscriptions,
                                                                  final ConsumerConfig config,
                                                                  final GroupRebalanceConfig groupRebalanceConfig,
                                                                  final ApiVersions apiVersions,
                                                                  final FetchMetricsManager fetchMetricsManager,
                                                                  final Supplier<NetworkClientDelegate> networkClientDelegateSupplier) {
        return new CachedSupplier<RequestManagers<K, V>>() {
            @Override
            protected RequestManagers<K, V> create() {
                Deserializer<K> keyDeserializer = new NoopDeserializer<>();
                Deserializer<V> valueDeserializer = new NoopDeserializer<>();
                final NetworkClientDelegate networkClientDelegate = networkClientDelegateSupplier.get();
                final ErrorEventHandler errorEventHandler = new ErrorEventHandler(backgroundEventQueue);
                final IsolationLevel isolationLevel = getConfiguredIsolationLevel(config);
                final FetchConfig<K, V> fetchConfig = createFetchConfig(config, new Deserializers<>(keyDeserializer, valueDeserializer));
                final long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                final int requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                final OffsetsRequestManager listOffsets = new OffsetsRequestManager(subscriptions,
                        metadata,
                        isolationLevel,
                        time,
                        retryBackoffMs,
                        requestTimeoutMs,
                        apiVersions,
                        networkClientDelegate,
                        logContext);
                final TopicMetadataRequestManager topicMetadata = new TopicMetadataRequestManager(logContext, config);
                final FetchRequestManager<K, V> fetch = new FetchRequestManager<>(logContext,
                        time,
                        errorEventHandler,
                        metadata,
                        subscriptions,
                        fetchConfig,
                        fetchMetricsManager,
                        networkClientDelegate);
                CoordinatorRequestManager coordinator = null;
                CommitRequestManager commit = null;

                if (groupRebalanceConfig != null && groupRebalanceConfig.groupId != null) {
                    final GroupState groupState = new GroupState(groupRebalanceConfig);
                    coordinator = new CoordinatorRequestManager(time,
                            logContext,
                            retryBackoffMs,
                            errorEventHandler,
                            groupState.groupId);
                    commit = new CommitRequestManager(time, logContext, subscriptions, config, coordinator, groupState);
                }

                return new RequestManagers<>(logContext,
                        listOffsets,
                        topicMetadata,
                        fetch,
                        Optional.ofNullable(coordinator),
                        Optional.ofNullable(commit));
            }
        };
    }

    private static class NoopDeserializer<T> implements Deserializer<T> {

        @Override
        public T deserialize(final String topic, final byte[] data) {
            throw new RuntimeException("who dares call me!?!?!");
        }
    }
}
