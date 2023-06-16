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
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredIsolationLevel;

/**
 * {@code RequestManagers} provides a means to pass around the set of {@link RequestManager} instances in the system.
 * This allows callers to both use the specific {@link RequestManager} instance, or to iterate over the list via
 * the {@link #entries()} method.
 */
public class RequestManagers implements Closeable {

    private final Logger log;
    public final Optional<CoordinatorRequestManager> coordinatorRequestManager;
    public final Optional<CommitRequestManager> commitRequestManager;
    public final ListOffsetsRequestManager listOffsetsRequestManager;
    public final TopicMetadataRequestManager topicMetadataRequestManager;
    private final List<Optional<? extends RequestManager>> entries;
    private final IdempotentCloser closer = new IdempotentCloser();

    public RequestManagers(LogContext logContext,
                           ListOffsetsRequestManager listOffsetsRequestManager,
                           TopicMetadataRequestManager topicMetadataRequestManager,
                           Optional<CoordinatorRequestManager> coordinatorRequestManager,
                           Optional<CommitRequestManager> commitRequestManager) {
        this.log = logContext.logger(RequestManagers.class);
        this.listOffsetsRequestManager = requireNonNull(listOffsetsRequestManager, "ListOffsetsRequestManager cannot be null");
        this.coordinatorRequestManager = coordinatorRequestManager;
        this.commitRequestManager = commitRequestManager;
        this.topicMetadataRequestManager = topicMetadataRequestManager;

        List<Optional<? extends RequestManager>> list = new ArrayList<>();
        list.add(coordinatorRequestManager);
        list.add(commitRequestManager);
        list.add(Optional.of(listOffsetsRequestManager));
        list.add(Optional.of(topicMetadataRequestManager));
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
    public static Supplier<RequestManagers> supplier(final Time time,
                                                     final LogContext logContext,
                                                     final BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                                     final ConsumerMetadata metadata,
                                                     final SubscriptionState subscriptions,
                                                     final ConsumerConfig config,
                                                     final GroupRebalanceConfig groupRebalanceConfig,
                                                     final ApiVersions apiVersions) {
        return new CachedSupplier<RequestManagers>() {
            @Override
            protected RequestManagers create() {
                final ErrorEventHandler errorEventHandler = new ErrorEventHandler(backgroundEventQueue);
                final IsolationLevel isolationLevel = getConfiguredIsolationLevel(config);
                final ListOffsetsRequestManager listOffsets = new ListOffsetsRequestManager(subscriptions,
                        metadata,
                        isolationLevel,
                        time,
                        apiVersions,
                        logContext);
                final TopicMetadataRequestManager topicMetadata = new TopicMetadataRequestManager(logContext, config);
                CoordinatorRequestManager coordinator = null;
                CommitRequestManager commit = null;

                if (groupRebalanceConfig != null && groupRebalanceConfig.groupId != null) {
                    final long retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);
                    final GroupState groupState = new GroupState(groupRebalanceConfig);
                    coordinator = new CoordinatorRequestManager(time,
                            logContext,
                            retryBackoffMs,
                            errorEventHandler,
                            groupState.groupId);
                    commit = new CommitRequestManager(time, logContext, subscriptions, config, coordinator, groupState);
                }

                return new RequestManagers(logContext,
                        listOffsets,
                        topicMetadata,
                        Optional.ofNullable(coordinator),
                        Optional.ofNullable(commit));
            }
        };
    }
}
