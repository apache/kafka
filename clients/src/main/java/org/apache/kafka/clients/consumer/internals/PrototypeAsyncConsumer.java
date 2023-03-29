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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.MetadataUpdateApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.FetchEvent;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredConsumerInterceptors;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;
import static org.apache.kafka.common.utils.Utils.propsToMap;

/**
 * This prototype consumer uses the EventHandler to process application
 * events so that the network IO can be processed in a background thread. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 */
public class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final LogContext logContext;
    private final Logger log;
    private final Time time;
    private final ConsumerMetadata metadata;
    final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BlockingQueue<BackgroundEvent> backgroundEventQueue;
    private final DefaultBackgroundThread<K, V> defaultBackgroundThread;
    private final Optional<String> groupId;
    
    private final Metrics metrics;
    private final ConsumerInterceptors<K, V> interceptors;
    private final SubscriptionState subscriptions;
    private final long defaultApiTimeoutMs;
    private final FetchBuffer<K, V> fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;

    public PrototypeAsyncConsumer(final Properties properties,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this(propsToMap(properties), keyDeserializer, valueDeserializer);
    }

    public PrototypeAsyncConsumer(final Map<String, Object> configs,
                                  final Deserializer<K> keyDeser,
                                  final Deserializer<V> valDeser) {
        this(new ConsumerConfig(appendDeserializerToConfig(configs, keyDeser, valDeser)), keyDeser, valDeser);
    }

    public PrototypeAsyncConsumer(final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this(Time.SYSTEM, config, keyDeserializer, valueDeserializer);
    }

    public PrototypeAsyncConsumer(final Time time,
                                  final ConsumerConfig config,
                                  final Deserializer<K> keyDeserializer,
                                  final Deserializer<V> valueDeserializer) {
        this.time = time;
        GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                GroupRebalanceConfig.ProtocolType.CONSUMER);
        this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
        this.logContext = createLogContext(config, groupRebalanceConfig);
        this.log = logContext.logger(getClass());
        Deserializers<K, V> deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
        this.subscriptions = createSubscriptionState(config, logContext);
        this.metrics = createMetrics(config, time);
        List<ConsumerInterceptor<K, V>> interceptorList = getConfiguredConsumerInterceptors(config);
        this.interceptors = new ConsumerInterceptors<>(interceptorList);
        ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                interceptorList,
                Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
        this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
        // Bootstrap the metadata with the bootstrap server IP address, which will be used once for the subsequent
        // metadata refresh once the background thread has started up.
        final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
        metadata.bootstrap(addresses);

        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventQueue = new LinkedBlockingQueue<>();

        FetchConfig<K, V> fetchConfig = createFetchConfig(config, deserializers);
        this.fetchBuffer = new FetchBuffer<>(logContext);
        FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
        this.fetchCollector = new FetchCollector<>(logContext,
                metadata,
                subscriptions,
                fetchConfig,
                fetchMetricsManager,
                time);
        this.defaultBackgroundThread = new DefaultBackgroundThread<>(logContext,
                time,
                config,
                applicationEventQueue,
                backgroundEventQueue,
                metadata,
                new ApiVersions(),
                metrics,
                subscriptions,
                groupRebalanceConfig);
        this.defaultBackgroundThread.start();
    }

    /**
     *  1. Poll for background events. If there's a fetch response event, process the record and return it. If it is
     *  another type of event, process it.
     *  2. Send fetches if needed.
     *  If the timeout expires, return an empty ConsumerRecord.
     *
     * @param timeout timeout of the poll loop
     * @return ConsumerRecord.  It can be empty if time timeout expires.
     */
    @Override
    public ConsumerRecords<K, V> poll(final Duration timeout) {
        try {
            Timer timer = time.timer(timeout);

            do {
                final BackgroundEvent backgroundEvent = backgroundEventQueue.poll();
                // processEvent() may process 3 types of event:
                // 1. Errors
                // 2. Callback Invocation
                // 3. Fetch responses
                // Errors will be handled or rethrown.
                // Callback invocation will trigger callback function execution, which is blocking until completion.
                // Successful fetch responses will be added to the completedFetches in the fetcher, which will then
                // be processed in the collectFetches().
                if (backgroundEvent != null) {
                    log.warn("Do something with this background event: {}", backgroundEvent);
                }

                // Create our event as a means to request the background thread to return any completed fetches.
                // If there are any
                FetchEvent<K, V> event = new FetchEvent<>();
                applicationEventQueue.add(event);

                Queue<CompletedFetch<K, V>> completedFetches = event.get(timer);

                if (completedFetches != null && !completedFetches.isEmpty()) {
                    fetchBuffer.addAll(completedFetches);
                }

                // The idea here is to have the background thread sending fetches autonomously, and the fetcher
                // uses the poll loop to retrieve successful fetchResponse and process them on the polling thread.
                final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);
                if (!fetch.isEmpty()) {
                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
                // We will wait for retryBackoffMs
            } while (timer.notExpired());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        return ConsumerRecords.empty();
    }

    /**
     * Commit offsets returned on the last {@link #poll(Duration) poll()} for all the subscribed list of topics and
     * partitions.
     */
    @Override
    public void commitSync() {
        commitSync(Duration.ofMillis(defaultApiTimeoutMs));
    }

    /**
     * This method sends a commit event to the EventHandler and return.
     */
    @Override
    public void commitAsync() {
        commitAsync(null);
    }

    @Override
    public void commitAsync(OffsetCommitCallback callback) {
        commitAsync(subscriptions.allConsumed(), callback);
    }

    @Override
    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        CompletableFuture<Void> future = commit(offsets);
        final OffsetCommitCallback commitCallback = callback == null ? new DefaultOffsetCommitCallback() : callback;
        future.whenComplete((r, t) -> {
            if (t != null) {
                commitCallback.onComplete(offsets, new KafkaException(t));
            } else {
                commitCallback.onComplete(offsets, null);
            }
        }).exceptionally(e -> {
            log.warn("Error during commitAsync", e);
            throw new KafkaException(e);
        });
    }

    // Visible for testing
    CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        applicationEventQueue.add(commitEvent);
        return commitEvent.future;
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        maybeThrowInvalidGroupIdException();
        if (partitions.isEmpty()) {
            return new HashMap<>();
        }

        final OffsetFetchApplicationEvent event = new OffsetFetchApplicationEvent(partitions);
        applicationEventQueue.add(event);
        return event.get(time.timer(timeout));
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent() || groupId.get().isEmpty()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Set<TopicPartition> paused() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public ConsumerGroupMetadata groupMetadata() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance() {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void enforceRebalance(String reason) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void close() {
        close(Duration.ofMillis(DEFAULT_CLOSE_TIMEOUT_MS));
    }

    @Override
    public void close(Duration timeout) {
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        closeQuietly(this.defaultBackgroundThread, "event handler", firstException);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka consumer", exception);
        }
    }

    @Override
    public void wakeup() {
    }

    /**
     * This method sends a commit event to the EventHandler and waits for
     * the event to finish.
     *
     * @param timeout max wait time for the blocking operation.
     */
    @Override
    public void commitSync(final Duration timeout) {
        commitSync(subscriptions.allConsumed(), timeout);
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        commitSync(offsets, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
        CompletableFuture<Void> commitFuture = commit(offsets);
        try {
            commitFuture.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (final TimeoutException e) {
            throw new org.apache.kafka.common.errors.TimeoutException(e);
        } catch (final InterruptedException e) {
            throw new InterruptException(e);
        } catch (final ExecutionException e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public Set<TopicPartition> assignment() {
        return Collections.unmodifiableSet(this.subscriptions.assignedPartitions());
    }

    /**
     * Get the current subscription.  or an empty set if no such call has
     * been made.
     * @return The set of topics currently subscribed to
     */
    @Override
    public Set<String> subscription() {
        return Collections.unmodifiableSet(this.subscriptions.subscription());
    }

    @Override
    public void subscribe(Collection<String> topics) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void assign(Collection<TopicPartition> partitions) {
        if (partitions == null) {
            throw new IllegalArgumentException("Topic partitions collection to assign to cannot be null");
        }

        if (partitions.isEmpty()) {
            this.unsubscribe();
            return;
        }

        for (TopicPartition tp : partitions) {
            String topic = (tp != null) ? tp.topic() : null;
            if (isBlank(topic))
                throw new IllegalArgumentException("Topic partitions to assign to cannot have null or empty topic");
        }
        // TODO: implement fetcher
        // fetcher.clearBufferedDataForUnassignedPartitions(partitions);

        // make sure the offsets of topic partitions the consumer is unsubscribing from
        // are committed since there will be no following rebalance
        commit(subscriptions.allConsumed());

        log.info("Assigned to partition(s): {}", join(partitions, ", "));
        if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
           updateMetadata(time.milliseconds());
    }

    private void updateMetadata(long milliseconds) {
        final MetadataUpdateApplicationEvent event = new MetadataUpdateApplicationEvent(milliseconds);
        applicationEventQueue.add(event);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener callback) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void subscribe(Pattern pattern) {
        throw new KafkaException("method not implemented");
    }

    @Override
    public void unsubscribe() {
        // fetcher.clearBufferedDataForUnassignedPartitions(Collections.emptySet());
        applicationEventQueue.add(new UnsubscribeApplicationEvent());
        this.subscriptions.unsubscribe();
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(Duration.ofMillis(timeoutMs));
    }

    // This is here temporary as we don't have public access to the ConsumerConfig in this module.
    public static Map<String, Object> appendDeserializerToConfig(Map<String, Object> configs,
                                                                 Deserializer<?> keyDeserializer,
                                                                 Deserializer<?> valueDeserializer) {
        // validate deserializer configuration, if the passed deserializer instance is null, the user must explicitly set a valid deserializer configuration value
        Map<String, Object> newConfigs = new HashMap<>(configs);
        if (keyDeserializer != null)
            newConfigs.put(KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass());
        else if (newConfigs.get(KEY_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(KEY_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        if (valueDeserializer != null)
            newConfigs.put(VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass());
        else if (newConfigs.get(VALUE_DESERIALIZER_CLASS_CONFIG) == null)
            throw new ConfigException(VALUE_DESERIALIZER_CLASS_CONFIG, null, "must be non-null.");
        return newConfigs;
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }
}
