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
package org.apache.kafka.connect.storage;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * An {@link OffsetBackingStore} with support for reading from and writing to a worker-global
 * offset backing store and/or a connector-specific offset backing store.
 */
public class ConnectorOffsetBackingStore implements OffsetBackingStore {

    private static final Logger log = LoggerFactory.getLogger(ConnectorOffsetBackingStore.class);

    /**
     * Builds an offset store that uses a connector-specific offset topic as the primary store and
     * the worker-global offset store as the secondary store.
     *
     * @param loggingContext a {@link Supplier} for the {@link LoggingContext} that should be used
     *                       for messages logged by this offset store; may not be null, and may never return null
     * @param workerStore the worker-global offset store; may not be null
     * @param connectorStore the connector-specific offset store; may not be null
     * @param connectorOffsetsTopic the name of the connector-specific offset topic; may not be null
     * @param connectorStoreAdmin the topic admin to use for the connector-specific offset topic; may not be null
     * @return an offset store backed primarily by the connector-specific offset topic and secondarily
     * by the worker-global offset store; never null
     */
    public static ConnectorOffsetBackingStore withConnectorAndWorkerStores(
            Supplier<LoggingContext> loggingContext,
            OffsetBackingStore workerStore,
            KafkaOffsetBackingStore connectorStore,
            String connectorOffsetsTopic,
            TopicAdmin connectorStoreAdmin
    ) {
        Objects.requireNonNull(loggingContext);
        Objects.requireNonNull(workerStore);
        Objects.requireNonNull(connectorStore);
        Objects.requireNonNull(connectorOffsetsTopic);
        Objects.requireNonNull(connectorStoreAdmin);
        return new ConnectorOffsetBackingStore(
                Time.SYSTEM,
                loggingContext,
                connectorOffsetsTopic,
                workerStore,
                connectorStore,
                connectorStoreAdmin
        );
    }

    /**
     * Builds an offset store that uses the worker-global offset store as the primary store, and no secondary store.
     *
     * @param loggingContext a {@link Supplier} for the {@link LoggingContext} that should be used
     *                       for messages logged by this offset store; may not be null, and may never return null
     * @param workerStore the worker-global offset store; may not be null
     * @param workerOffsetsTopic the name of the worker-global offset topic; may be null if the worker
     *                           does not use an offset topic for its offset store
     * @return an offset store for the connector backed solely by the worker-global offset store; never null
     */
    public static ConnectorOffsetBackingStore withOnlyWorkerStore(
            Supplier<LoggingContext> loggingContext,
            OffsetBackingStore workerStore,
            String workerOffsetsTopic
    ) {
        Objects.requireNonNull(loggingContext);
        Objects.requireNonNull(workerStore);
        return new ConnectorOffsetBackingStore(Time.SYSTEM, loggingContext, workerOffsetsTopic, workerStore, null, null);
    }

    /**
     * Builds an offset store that uses a connector-specific offset topic as the primary store, and no secondary store.
     *
     * @param loggingContext a {@link Supplier} for the {@link LoggingContext} that should be used
     *                       for messages logged by this offset store; may not be null, and may never return null
     * @param connectorStore the connector-specific offset store; may not be null
     * @param connectorOffsetsTopic the name of the connector-specific offset topic; may not be null
     * @param connectorStoreAdmin the topic admin to use for the connector-specific offset topic; may not be null
     * @return an offset store for the connector backed solely by the connector-specific offset topic; never null
     */
    public static ConnectorOffsetBackingStore withOnlyConnectorStore(
            Supplier<LoggingContext> loggingContext,
            KafkaOffsetBackingStore connectorStore,
            String connectorOffsetsTopic,
            TopicAdmin connectorStoreAdmin
    ) {
        Objects.requireNonNull(loggingContext);
        Objects.requireNonNull(connectorOffsetsTopic);
        Objects.requireNonNull(connectorStoreAdmin);
        return new ConnectorOffsetBackingStore(
                Time.SYSTEM,
                loggingContext,
                connectorOffsetsTopic,
                null,
                connectorStore,
                connectorStoreAdmin
        );
    }

    private final Time time;
    private final Supplier<LoggingContext> loggingContext;
    private final String primaryOffsetsTopic;
    private final Optional<OffsetBackingStore> workerStore;
    private final Optional<KafkaOffsetBackingStore> connectorStore;
    private final Optional<TopicAdmin> connectorStoreAdmin;

    ConnectorOffsetBackingStore(
            Time time,
            Supplier<LoggingContext> loggingContext,
            String primaryOffsetsTopic,
            OffsetBackingStore workerStore,
            KafkaOffsetBackingStore connectorStore,
            TopicAdmin connectorStoreAdmin
    ) {
        if (workerStore == null && connectorStore == null) {
            throw new IllegalArgumentException("At least one non-null offset store must be provided");
        }
        this.time = time;
        this.loggingContext = loggingContext;
        this.primaryOffsetsTopic = primaryOffsetsTopic;
        this.workerStore = Optional.ofNullable(workerStore);
        this.connectorStore = Optional.ofNullable(connectorStore);
        this.connectorStoreAdmin = Optional.ofNullable(connectorStoreAdmin);
    }

    public String primaryOffsetsTopic() {
        return primaryOffsetsTopic;
    }

    /**
     * If configured to use a connector-specific offset store, {@link OffsetBackingStore#start() start} that store.
     *
     * <p>The worker-global offset store is not modified; it is the caller's responsibility to ensure that it is started
     * before calls to {@link #get(Collection)} and {@link #set(Map, Callback)} take place.
     */
    @Override
    public void start() {
        // Worker offset store should already be started
        connectorStore.ifPresent(OffsetBackingStore::start);
    }

    /**
     * If configured to use a connector-specific offset store, {@link OffsetBackingStore#stop() stop} that store,
     * and {@link TopicAdmin#close(Duration) close} the topic admin used by that store.
     *
     * <p>The worker-global offset store is not modified as it may be used for other connectors that either already exist,
     * or will be created, on this worker.
     */
    @Override
    public void stop() {
        // Worker offset store should not be stopped as it may be used for multiple connectors
        connectorStore.ifPresent(OffsetBackingStore::stop);
        connectorStoreAdmin.ifPresent(TopicAdmin::close);
    }

    /**
     * Get the offset values for the specified keys.
     *
     * <p>If configured to use a connector-specific offset store, priority is given to the values contained in that store,
     * and the values in the worker-global offset store (if one is provided) are used as a fallback for keys that are not
     * present in the connector-specific store.
     *
     * <p>If not configured to use a connector-specific offset store, only the values contained in the worker-global
     * offset store are returned.

     * @param keys list of keys to look up
     * @return future for the resulting map from key to value
     */
    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        Future<Map<ByteBuffer, ByteBuffer>> workerGetFuture = getFromStore(workerStore, keys);
        Future<Map<ByteBuffer, ByteBuffer>> connectorGetFuture = getFromStore(connectorStore, keys);

        return new Future<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                // Note the use of | instead of || here; this causes cancel to be invoked on both futures,
                // even if the first call to cancel returns true
                return workerGetFuture.cancel(mayInterruptIfRunning)
                        | connectorGetFuture.cancel(mayInterruptIfRunning);
            }

            @Override
            public boolean isCancelled() {
                return workerGetFuture.isCancelled()
                        || connectorGetFuture.isCancelled();
            }

            @Override
            public boolean isDone() {
                return workerGetFuture.isDone()
                        && connectorGetFuture.isDone();
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get() throws InterruptedException, ExecutionException {
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>(workerGetFuture.get());
                result.putAll(connectorGetFuture.get());
                return result;
            }

            @Override
            public Map<ByteBuffer, ByteBuffer> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                long timeoutMs = unit.toMillis(timeout);
                long endTime = time.milliseconds() + timeoutMs;
                Map<ByteBuffer, ByteBuffer> result = new HashMap<>(workerGetFuture.get(timeoutMs, unit));
                timeoutMs = Math.max(1, endTime - time.milliseconds());
                result.putAll(connectorGetFuture.get(timeoutMs, TimeUnit.MILLISECONDS));
                return result;
            }
        };
    }

    /**
     * Store the specified offset key/value pairs.
     *
     * <p>If configured to use a connector-specific offset store, the returned {@link Future} corresponds to a
     * write to that store, and the passed-in {@link Callback} is invoked once that write completes. If a worker-global
     * store is provided, a secondary write is made to that store if the write to the connector-specific store
     * succeeds. Errors with this secondary write are not reflected in the returned {@link Future} or the passed-in
     * {@link Callback}; they are only logged as a warning to users.
     *
     * <p>If not configured to use a connector-specific offset store, the returned {@link Future} corresponds to a
     * write to the worker-global offset store, and the passed-in {@link Callback} is invoked once that write completes.

     * @param values map from key to value
     * @param callback callback to invoke on completion of the primary write
     * @return void future for the primary write
     */
    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        final OffsetBackingStore primaryStore;
        final OffsetBackingStore secondaryStore;
        if (connectorStore.isPresent()) {
            primaryStore = connectorStore.get();
            secondaryStore = workerStore.orElse(null);
        } else if (workerStore.isPresent()) {
            primaryStore = workerStore.get();
            secondaryStore = null;
        } else {
            // Should never happen since we check for this case in the constructor, but just in case, this should
            // be more informative than the NPE that would otherwise be thrown
            throw new IllegalStateException("At least one non-null offset store must be provided");
        }

        return primaryStore.set(values, (primaryWriteError, ignored) -> {
            if (secondaryStore != null) {
                if (primaryWriteError != null) {
                    log.trace("Skipping offsets write to secondary store because primary write has failed", primaryWriteError);
                } else {
                    try {
                        // Invoke OffsetBackingStore::set but ignore the resulting future; we don't block on writes to this
                        // backing store.
                        secondaryStore.set(values, (secondaryWriteError, ignored2) -> {
                            try (LoggingContext context = loggingContext()) {
                                if (secondaryWriteError != null) {
                                    log.warn("Failed to write offsets to secondary backing store", secondaryWriteError);
                                } else {
                                    log.debug("Successfully flushed offsets to secondary backing store");
                                }
                            }
                        });
                    } catch (Exception e) {
                        log.warn("Failed to write offsets to secondary backing store", e);
                    }
                }
            }
            try (LoggingContext context = loggingContext()) {
                callback.onCompletion(primaryWriteError, ignored);
            }
        });
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        Set<Map<String, Object>> partitions = new HashSet<>();
        workerStore.ifPresent(offsetBackingStore -> partitions.addAll(offsetBackingStore.connectorPartitions(connectorName)));
        connectorStore.ifPresent(offsetBackingStore -> partitions.addAll(offsetBackingStore.connectorPartitions(connectorName)));
        return partitions;
    }

    /**
     * If configured to use a connector-specific offset store,
     * {@link OffsetBackingStore#configure(WorkerConfig) configure} that store.
     *
     * <p>The worker-global offset store is not modified; it is the caller's responsibility to ensure that it is configured
     * before calls to {@link #start()}, {@link #get(Collection)} and {@link #set(Map, Callback)} take place.
     */
    @Override
    public void configure(WorkerConfig config) {
        // Worker offset store should already be configured
        connectorStore.ifPresent(store -> store.configure(config));
    }

    // For testing
    public boolean hasConnectorSpecificStore() {
        return connectorStore.isPresent();
    }

    // For testing
    public boolean hasWorkerGlobalStore() {
        return workerStore.isPresent();
    }

    private LoggingContext loggingContext() {
        LoggingContext result = loggingContext.get();
        Objects.requireNonNull(result);
        return result;
    }

    private static Future<Map<ByteBuffer, ByteBuffer>> getFromStore(Optional<? extends OffsetBackingStore> store, Collection<ByteBuffer> keys) {
        return store.map(s -> s.get(keys)).orElseGet(() -> CompletableFuture.completedFuture(Collections.emptyMap()));
    }

}
