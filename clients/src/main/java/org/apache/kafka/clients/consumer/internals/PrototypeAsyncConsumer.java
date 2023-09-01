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
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.AssignmentChangeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.CommitApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.EventHandler;
import org.apache.kafka.clients.consumer.internals.events.FetchEvent;
import org.apache.kafka.clients.consumer.internals.events.ListOffsetsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.NewTopicsMetadataUpdateRequestEvent;
import org.apache.kafka.clients.consumer.internals.events.OffsetFetchApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ResetPositionsApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicMetadataApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ValidatePositionsApplicationEvent;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;
import org.slf4j.event.Level;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_JMX_PREFIX;
import static org.apache.kafka.clients.consumer.internals.Utils.CONSUMER_METRIC_GROUP_PREFIX;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchConfig;
import static org.apache.kafka.clients.consumer.internals.Utils.createFetchMetricsManager;
import static org.apache.kafka.clients.consumer.internals.Utils.createLogContext;
import static org.apache.kafka.clients.consumer.internals.Utils.createMetrics;
import static org.apache.kafka.clients.consumer.internals.Utils.createSubscriptionState;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredConsumerInterceptors;
import static org.apache.kafka.clients.consumer.internals.Utils.getConfiguredIsolationLevel;
import static org.apache.kafka.common.utils.Utils.closeQuietly;
import static org.apache.kafka.common.utils.Utils.isBlank;
import static org.apache.kafka.common.utils.Utils.join;
import static org.apache.kafka.common.utils.Utils.propsToMap;
import static org.apache.kafka.common.utils.Utils.swallow;

/**
 * This prototype consumer uses the EventHandler to process application
 * events so that the network IO can be processed in a background thread. Visit
 * <a href="https://cwiki.apache.org/confluence/display/KAFKA/Proposal%3A+Consumer+Threading+Model+Refactor" >this document</a>
 * for detail implementation.
 */
public class PrototypeAsyncConsumer<K, V> implements Consumer<K, V> {
    static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

    private final Metrics metrics;
    private final KafkaConsumerMetrics kafkaConsumerMetrics;

    private Logger log;
    private final String clientId;
    private final Optional<String> groupId;
    private final EventHandler eventHandler;
    private final BackgroundEventProcessor backgroundEventProcessor;
    private final Deserializers<K, V> deserializers;
    private final FetchBuffer fetchBuffer;
    private final FetchCollector<K, V> fetchCollector;
    private final ConsumerInterceptors<K, V> interceptors;
    private final IsolationLevel isolationLevel;

    private final Time time;
    private final SubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final long retryBackoffMs;
    private final long requestTimeoutMs;
    private final long defaultApiTimeoutMs;
    private volatile boolean closed = false;
    private final List<ConsumerPartitionAssignor> assignors;

    // to keep from repeatedly scanning subscriptions in poll(), cache the result during metadata updates
    private boolean cachedSubscriptionHasAllFetchPositions;

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
        try {
            GroupRebalanceConfig groupRebalanceConfig = new GroupRebalanceConfig(config,
                    GroupRebalanceConfig.ProtocolType.CONSUMER);

            this.groupId = Optional.ofNullable(groupRebalanceConfig.groupId);
            this.clientId = config.getString(CommonClientConfigs.CLIENT_ID_CONFIG);
            LogContext logContext = createLogContext(config, groupRebalanceConfig);
            this.log = logContext.logger(getClass());
            groupId.ifPresent(groupIdStr -> {
                if (groupIdStr.isEmpty()) {
                    log.warn("Support for using the empty group id by consumers is deprecated and will be removed in the next major release.");
                }
            });

            log.debug("Initializing the Kafka consumer");
            this.requestTimeoutMs = config.getInt(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);
            this.time = time;
            this.metrics = createMetrics(config, time);
            this.retryBackoffMs = config.getLong(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG);

            List<ConsumerInterceptor<K, V>> interceptorList = getConfiguredConsumerInterceptors(config);
            this.interceptors = new ConsumerInterceptors<>(interceptorList);
            this.deserializers = new Deserializers<>(config, keyDeserializer, valueDeserializer);
            this.subscriptions = createSubscriptionState(config, logContext);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(metrics.reporters(),
                    interceptorList,
                    Arrays.asList(deserializers.keyDeserializer, deserializers.valueDeserializer));
            this.metadata = new ConsumerMetadata(config, subscriptions, logContext, clusterResourceListeners);
            final List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config);
            metadata.bootstrap(addresses);

            FetchMetricsManager fetchMetricsManager = createFetchMetricsManager(metrics);
            this.isolationLevel = getConfiguredIsolationLevel(config);

            ApiVersions apiVersions = new ApiVersions();
            final BlockingQueue<ApplicationEvent> applicationEventQueue = new LinkedBlockingQueue<>();
            final BlockingQueue<BackgroundEvent> backgroundEventQueue = new LinkedBlockingQueue<>();
            final Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(time,
                    logContext,
                    metadata,
                    config,
                    apiVersions,
                    metrics,
                    fetchMetricsManager);
            final Supplier<RequestManagers<String, String>> requestManagersSupplier = RequestManagers.supplier(time,
                    logContext,
                    backgroundEventQueue,
                    metadata,
                    subscriptions,
                    config,
                    groupRebalanceConfig,
                    apiVersions,
                    fetchMetricsManager,
                    networkClientDelegateSupplier);
            final Supplier<ApplicationEventProcessor<String, String>> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(logContext,
                    metadata,
                    backgroundEventQueue,
                    requestManagersSupplier);
            this.eventHandler = new DefaultEventHandler<>(time,
                    logContext,
                    applicationEventQueue,
                    backgroundEventQueue,
                    applicationEventProcessorSupplier,
                    networkClientDelegateSupplier,
                    requestManagersSupplier);
            this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);
            this.assignors = ConsumerPartitionAssignor.getAssignorInstances(
                    config.getList(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG),
                    config.originals(Collections.singletonMap(ConsumerConfig.CLIENT_ID_CONFIG, clientId))
            );

            // no coordinator will be constructed for the default (null) group id
            if (!groupId.isPresent()) {
                config.ignore(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG);
                //config.ignore(ConsumerConfig.THROW_ON_FETCH_STABLE_OFFSET_UNSUPPORTED);
            }
            // These are specific to the foreground thread
            FetchConfig<K, V> fetchConfig = createFetchConfig(config, deserializers);
            this.fetchBuffer = new FetchBuffer(logContext);
            this.fetchCollector = new FetchCollector<>(logContext,
                    metadata,
                    subscriptions,
                    fetchConfig,
                    fetchMetricsManager,
                    time);

            this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, CONSUMER_METRIC_GROUP_PREFIX);

            config.logUnused();
            AppInfoParser.registerAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics, time.milliseconds());
            log.debug("Kafka consumer initialized");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed; this is to prevent resource leak. see KAFKA-2121
            // we do not need to call `close` at all when `log` is null, which means no internal objects were initialized.
            if (this.log != null) {
                close(Duration.ZERO, true);
            }
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka consumer", t);
        }
    }

    public PrototypeAsyncConsumer(LogContext logContext,
                                  String clientId,
                                  Deserializers<K, V> deserializers,
                                  FetchBuffer fetchBuffer,
                                  FetchCollector<K, V> fetchCollector,
                                  ConsumerInterceptors<K, V> interceptors,
                                  Time time,
                                  EventHandler eventHandler,
                                  BlockingQueue<BackgroundEvent> backgroundEventQueue,
                                  Metrics metrics,
                                  SubscriptionState subscriptions,
                                  ConsumerMetadata metadata,
                                  long retryBackoffMs,
                                  long requestTimeoutMs,
                                  int defaultApiTimeoutMs,
                                  List<ConsumerPartitionAssignor> assignors,
                                  String groupId) {
        this.log = logContext.logger(getClass());
        this.clientId = clientId;
        this.deserializers = deserializers;
        this.fetchBuffer = fetchBuffer;
        this.fetchCollector = fetchCollector;
        this.isolationLevel = IsolationLevel.READ_UNCOMMITTED;
        this.interceptors = Objects.requireNonNull(interceptors);
        this.time = time;
        this.eventHandler = eventHandler;
        this.backgroundEventProcessor = new BackgroundEventProcessor(logContext, backgroundEventQueue);
        this.metrics = metrics;
        this.subscriptions = subscriptions;
        this.metadata = metadata;
        this.retryBackoffMs = retryBackoffMs;
        this.requestTimeoutMs = requestTimeoutMs;
        this.defaultApiTimeoutMs = defaultApiTimeoutMs;
        this.assignors = assignors;
        this.groupId = Optional.ofNullable(groupId);
        this.kafkaConsumerMetrics = new KafkaConsumerMetrics(metrics, "consumer");
    }

    /**
     * poll implementation using {@link EventHandler}.
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
        Timer timer = time.timer(timeout);

        try {
            backgroundEventProcessor.process();

            this.kafkaConsumerMetrics.recordPollStart(timer.currentTimeMs());

            if (this.subscriptions.hasNoSubscriptionOrUserAssignment()) {
                throw new IllegalStateException("Consumer is not subscribed to any topics or assigned any partitions");
            }

            do {
                updateAssignmentMetadataIfNeeded(timer);
                final Fetch<K, V> fetch = pollForFetches(timer);

                if (!fetch.isEmpty()) {
                    sendFetches();

                    if (fetch.records().isEmpty()) {
                        log.trace("Returning empty records from `poll()` "
                                + "since the consumer's position has advanced for at least one topic partition");
                    }

                    return this.interceptors.onConsume(new ConsumerRecords<>(fetch.records()));
                }
                // We will wait for retryBackoffMs
            } while (timer.notExpired());

            return ConsumerRecords.empty();
        } finally {
            this.kafkaConsumerMetrics.recordPollEnd(timer.currentTimeMs());
        }
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
            System.out.println(e);
            throw new KafkaException(e);
        });
    }

    // Visible for testing
    CompletableFuture<Void> commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        maybeThrowInvalidGroupIdException();
        final CommitApplicationEvent commitEvent = new CommitApplicationEvent(offsets);
        eventHandler.add(commitEvent);
        return commitEvent.future();
    }

    @Override
    public void seek(TopicPartition partition, long offset) {
        if (offset < 0)
            throw new IllegalArgumentException("seek offset must not be a negative number");

        log.info("Seeking to offset {} for partition {}", offset, partition);
        SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
                offset,
                Optional.empty(), // This will ensure we skip validation
                this.metadata.currentLeader(partition));
        this.subscriptions.seekUnvalidated(partition, newPosition);
    }

    @Override
    public void seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata) {
        long offset = offsetAndMetadata.offset();
        if (offset < 0) {
            throw new IllegalArgumentException("seek offset must not be a negative number");
        }

        if (offsetAndMetadata.leaderEpoch().isPresent()) {
            log.info("Seeking to offset {} for partition {} with epoch {}",
                offset, partition, offsetAndMetadata.leaderEpoch().get());
        } else {
            log.info("Seeking to offset {} for partition {}", offset, partition);
        }
        Metadata.LeaderAndEpoch currentLeaderAndEpoch = this.metadata.currentLeader(partition);
        SubscriptionState.FetchPosition newPosition = new SubscriptionState.FetchPosition(
            offsetAndMetadata.offset(),
            offsetAndMetadata.leaderEpoch(),
            currentLeaderAndEpoch);
        this.updateLastSeenEpochIfNewer(partition, offsetAndMetadata);
        this.subscriptions.seekUnvalidated(partition, newPosition);
    }

    @Override
    public void seekToBeginning(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
        subscriptions.requestOffsetReset(parts, OffsetResetStrategy.EARLIEST);
    }

    @Override
    public void seekToEnd(Collection<TopicPartition> partitions) {
        if (partitions == null)
            throw new IllegalArgumentException("Partitions collection cannot be null");

        Collection<TopicPartition> parts = partitions.size() == 0 ? this.subscriptions.assignedPartitions() : partitions;
        subscriptions.requestOffsetReset(parts, OffsetResetStrategy.LATEST);
    }

    @Override
    public long position(TopicPartition partition) {
        return position(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public long position(TopicPartition partition, Duration timeout) {
        if (!this.subscriptions.isAssigned(partition))
            throw new IllegalStateException("You can only check the position for partitions assigned to this consumer.");

        Timer timer = time.timer(timeout);
        do {
            SubscriptionState.FetchPosition position = this.subscriptions.validPosition(partition);
            if (position != null)
                return position.offset;

            updateFetchPositions(timer);
        } while (timer.notExpired());

        throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the position " +
                "for partition " + partition + " could be determined");
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition) {
        return committed(partition, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    @Deprecated
    public OffsetAndMetadata committed(TopicPartition partition, Duration timeout) {
        return committed(Collections.singleton(partition), timeout).get(partition);
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions) {
        return committed(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(final Set<TopicPartition> partitions,
                                                            final Duration timeout) {
        long start = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            final Map<TopicPartition, OffsetAndMetadata> offsets;
            offsets = eventHandler.addAndGet(new OffsetFetchApplicationEvent(partitions), time.timer(timeout));
            if (offsets == null) {
                throw new TimeoutException("Timeout of " + timeout.toMillis() + "ms expired before the last " +
                        "committed offset for partitions " + partitions + " could be determined. Try tuning default.api.timeout.ms " +
                        "larger to relax the threshold.");
            } else {
                offsets.forEach(this::updateLastSeenEpochIfNewer);
                return offsets;
            }
        } finally {
            kafkaConsumerMetrics.recordCommitted(time.nanoseconds() - start);
        }
    }

    private void maybeThrowInvalidGroupIdException() {
        if (!groupId.isPresent() || groupId.get().isEmpty()) {
            throw new InvalidGroupIdException("To use the group management or offset commit APIs, you must " +
                    "provide a valid " + ConsumerConfig.GROUP_ID_CONFIG + " in the consumer configuration.");
        }
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return partitionsFor(topic, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic, Duration timeout) {
        Cluster cluster = this.metadata.fetch();
        List<PartitionInfo> parts = cluster.partitionsForTopic(topic);
        if (!parts.isEmpty())
            return parts;

        TopicMetadataApplicationEvent event = new TopicMetadataApplicationEvent(Optional.of(topic));
        Timer timer = time.timer(timeout);
        Map<String, List<PartitionInfo>> partitionInfo = eventHandler.addAndGet(event, timer);
        return partitionInfo.getOrDefault(topic, Collections.emptyList());
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics() {
        return listTopics(Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<String, List<PartitionInfo>> listTopics(Duration timeout) {
        TopicMetadataApplicationEvent event = new TopicMetadataApplicationEvent(Optional.empty());
        Timer timer = time.timer(timeout);
        return eventHandler.addAndGet(event, timer);
    }

    @Override
    public Set<TopicPartition> paused() {
        return Collections.unmodifiableSet(subscriptions.pausedPartitions());
    }

    @Override
    public void pause(Collection<TopicPartition> partitions) {
        log.debug("Pausing partitions {}", partitions);
        for (TopicPartition partition: partitions) {
            subscriptions.pause(partition);
        }
    }

    @Override
    public void resume(Collection<TopicPartition> partitions) {
        log.debug("Resuming partitions {}", partitions);
        for (TopicPartition partition: partitions) {
            subscriptions.resume(partition);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch) {
        return offsetsForTimes(timestampsToSearch, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch, Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(timestampsToSearch, "Timestamps to search cannot be null");

        if (timestampsToSearch.isEmpty()) {
            return Collections.emptyMap();
        }
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampsToSearch,
                true);

        // If timeout is set to zero return empty immediately; otherwise try to get the results
        // and throw timeout exception if it cannot complete in time.
        if (timeout.toMillis() == 0L)
            return listOffsetsEvent.emptyResult();

        return eventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions) {
        return beginningOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.EARLIEST_TIMESTAMP, timeout);
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions) {
        return endOffsets(partitions, Duration.ofMillis(defaultApiTimeoutMs));
    }

    @Override
    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, Duration timeout) {
        return beginningOrEndOffset(partitions, ListOffsetsRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           Duration timeout) {
        // Keeping same argument validation error thrown by the current consumer implementation
        // to avoid API level changes.
        requireNonNull(partitions, "Partitions cannot be null");

        if (partitions.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, Long> timestampToSearch =
                partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> timestamp));
        final ListOffsetsApplicationEvent listOffsetsEvent = new ListOffsetsApplicationEvent(
                timestampToSearch,
                false);
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap =
                eventHandler.addAndGet(listOffsetsEvent, time.timer(timeout));
        return offsetAndTimestampMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
                e -> e.getValue().offset()));
    }

    @Override
    public OptionalLong currentLag(TopicPartition topicPartition) {
        final Long lag = subscriptions.partitionLag(topicPartition, isolationLevel);

        // if the log end offset is not known and hence cannot return lag and there is
        // no in-flight list offset requested yet,
        // issue a list offset request for that partition so that next time
        // we may get the answer; we do not need to wait for the return value
        // since we would not try to poll the network client synchronously
        if (lag == null) {
            if (subscriptions.partitionEndOffset(topicPartition, isolationLevel) == null &&
                    !subscriptions.partitionEndOffsetRequested(topicPartition)) {
                log.info("Requesting the log end offset for {} in order to compute lag", topicPartition);
                subscriptions.requestPartitionEndOffset(topicPartition);
                endOffsets(Collections.singleton(topicPartition), Duration.ofMillis(0));
            }

            return OptionalLong.empty();
        }

        return OptionalLong.of(lag);
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

    private Timer createTimerForRequest(final Duration timeout) {
        // this.time could be null if an exception occurs in constructor prior to setting the this.time field
        final Time localTime = (time == null) ? Time.SYSTEM : time;
        return localTime.timer(Math.min(timeout.toMillis(), requestTimeoutMs));
    }

    @Override
    public void close(Duration timeout) {
        if (timeout.toMillis() < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        try {
            if (!closed) {
                // need to close before setting the flag since the close function
                // itself may trigger rebalance callback that needs the consumer to be open still
                close(timeout, false);
            }
        } finally {
            closed = true;
        }
    }

    private void close(Duration timeout, boolean swallowException) {
        log.trace("Closing the Kafka consumer");
        AtomicReference<Throwable> firstException = new AtomicReference<>();

        final Timer closeTimer = createTimerForRequest(timeout);
        if (fetchBuffer != null) {
            // the timeout for the session close is at-most the requestTimeoutMs
            long remainingDurationInTimeout = Math.max(0, timeout.toMillis() - closeTimer.elapsedMs());
            if (remainingDurationInTimeout > 0) {
                remainingDurationInTimeout = Math.min(requestTimeoutMs, remainingDurationInTimeout);
            }

            closeTimer.reset(remainingDurationInTimeout);

            // This is a blocking call bound by the time remaining in closeTimer
            swallow(log, Level.ERROR, "Failed to close fetcher", fetchBuffer::close, firstException);
        }


        closeQuietly(interceptors, "consumer interceptors", firstException);
        closeQuietly(kafkaConsumerMetrics, "kafka consumer metrics", firstException);
        closeQuietly(metrics, "consumer metrics", firstException);
        closeQuietly(this.eventHandler, "event handler", firstException);
        closeQuietly(deserializers, "consumer deserializers", firstException);

        AppInfoParser.unregisterAppInfo(CONSUMER_JMX_PREFIX, clientId, metrics);
        log.debug("Kafka consumer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
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
        long commitStart = time.nanoseconds();
        try {
            maybeThrowInvalidGroupIdException();
            offsets.forEach(this::updateLastSeenEpochIfNewer);
            eventHandler.addAndGet(new CommitApplicationEvent(offsets), time.timer(timeout));
        } finally {
            kafkaConsumerMetrics.recordCommitSync(time.nanoseconds() - commitStart);
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
        subscribe(topics, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();
        if (topics == null)
            throw new IllegalArgumentException("Topic collection to subscribe to cannot be null");
        if (topics.isEmpty()) {
            // treat subscribing to empty topic list as the same as unsubscribing
            this.unsubscribe();
        } else {
            for (String topic : topics) {
                if (isBlank(topic))
                    throw new IllegalArgumentException("Topic collection to subscribe to cannot contain null or empty topic");
            }

            throwIfNoAssignorsConfigured();

            // Clear the buffered data which are not a part of newly assigned topics
            final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

            for (TopicPartition tp : subscriptions.assignedPartitions()) {
                if (topics.contains(tp.topic()))
                    currentTopicPartitions.add(tp);
            }

            fetchBuffer.retainAll(currentTopicPartitions);
            log.info("Subscribed to topic(s): {}", join(topics, ", "));
            if (this.subscriptions.subscribe(new HashSet<>(topics), listener))
                metadata.requestUpdateForNewTopics();
        }
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

        // Clear the buffered data which are not a part of newly assigned topics
        final Set<TopicPartition> currentTopicPartitions = new HashSet<>();

        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (partitions.contains(tp))
                currentTopicPartitions.add(tp);
        }

        fetchBuffer.retainAll(currentTopicPartitions);

        // make sure the offsets of topic partitions the consumer is unsubscribing from
        // are committed since there will be no following rebalance
        eventHandler.add(new AssignmentChangeApplicationEvent(this.subscriptions.allConsumed(), time.milliseconds()));

        log.info("Assigned to partition(s): {}", join(partitions, ", "));
        if (this.subscriptions.assignFromUser(new HashSet<>(partitions)))
           updateMetadata();
    }

    private void updateMetadata() {
        final NewTopicsMetadataUpdateRequestEvent event = new NewTopicsMetadataUpdateRequestEvent();
        eventHandler.add(event);
    }

    @Override
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        maybeThrowInvalidGroupIdException();
        if (pattern == null || pattern.toString().equals(""))
            throw new IllegalArgumentException("Topic pattern to subscribe to cannot be " + (pattern == null ?
                    "null" : "empty"));

        throwIfNoAssignorsConfigured();
        log.info("Subscribed to pattern: '{}'", pattern);
        this.subscriptions.subscribe(pattern, listener);
        this.updatePatternSubscription(metadata.fetch());
        this.metadata.requestUpdateForNewTopics();
    }

    /**
     * TODO: remove this when we implement the KIP-848 protocol.
     *
     * <p>
     * The contents of this method are shamelessly stolen from
     * {@link ConsumerCoordinator#updatePatternSubscription(Cluster)} and are used here because we won't have access
     * to a {@link ConsumerCoordinator} in this code. Perhaps it could be moved to a ConsumerUtils class?
     *
     * @param cluster Cluster from which we get the topics
     */
    private void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = cluster.topics().stream()
                .filter(subscriptions::matchesSubscribedPattern)
                .collect(Collectors.toSet());
        if (subscriptions.subscribeFromPattern(topicsToSubscribe))
            metadata.requestUpdateForNewTopics();
    }

    @Override
    public void subscribe(Pattern pattern) {
        subscribe(pattern, new NoOpConsumerRebalanceListener());
    }

    @Override
    public void unsubscribe() {
        fetchBuffer.retainAll(Collections.emptySet());
        eventHandler.add(new UnsubscribeApplicationEvent());
        this.subscriptions.unsubscribe();
    }

    @Override
    @Deprecated
    public ConsumerRecords<K, V> poll(final long timeoutMs) {
        return poll(Duration.ofMillis(timeoutMs));
    }

    private void sendFetches() {
        FetchEvent event = new FetchEvent();
        eventHandler.add(event);

        event.future().whenComplete((completedFetches, error) -> {
            if (completedFetches != null && !completedFetches.isEmpty()) {
                fetchBuffer.addAll(completedFetches);
            }
        });
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    private Fetch<K, V> pollForFetches(Timer timer) {
        long pollTimeout = timer.remainingMs();

        // if data is available already, return it immediately
        final Fetch<K, V> fetch = fetchCollector.collectFetch(fetchBuffer);
        if (!fetch.isEmpty()) {
            return fetch;
        }

        // send any new fetches (won't resend pending fetches)
        sendFetches();

        // We do not want to be stuck blocking in poll if we are missing some positions
        // since the offset lookup may be backing off after a failure

        // NOTE: the use of cachedSubscriptionHasAllFetchPositions means we MUST call
        // updateAssignmentMetadataIfNeeded before this method.
        if (!cachedSubscriptionHasAllFetchPositions && pollTimeout > retryBackoffMs) {
            pollTimeout = retryBackoffMs;
        }

        log.trace("Polling for fetches with timeout {}", pollTimeout);

        Timer pollTimer = time.timer(pollTimeout);

        // Attempt to fetch any data. It's OK if we time out here; it's a best case effort. The
        // data may not be immediately available, but the calling method (poll) will correctly
        // handle the overall timeout.
        try {
            Queue<CompletedFetch> completedFetches = eventHandler.addAndGet(new FetchEvent(), pollTimer);
            if (completedFetches != null && !completedFetches.isEmpty()) {
                fetchBuffer.addAll(completedFetches);
            }
        } catch (TimeoutException e) {
            log.trace("Timeout during fetch", e);
        } finally {
            timer.update(pollTimer.currentTimeMs());
        }

        return fetchCollector.collectFetch(fetchBuffer);
    }

    /**
     * Set the fetch position to the committed position (if there is one)
     * or reset it using the offset reset policy the user has configured.
     *
     * @throws org.apache.kafka.common.errors.AuthenticationException if authentication fails. See the exception for more details
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no offset reset policy is
     *             defined
     * @return true iff the operation completed without timing out
     */
    private boolean updateFetchPositions(final Timer timer) {
        // If any partitions have been truncated due to a leader change, we need to validate the offsets
        ValidatePositionsApplicationEvent validatePositionsEvent = new ValidatePositionsApplicationEvent();
        eventHandler.add(validatePositionsEvent);

        cachedSubscriptionHasAllFetchPositions = subscriptions.hasAllFetchPositions();
        if (cachedSubscriptionHasAllFetchPositions) return true;

        // If there are any partitions which do not have a valid position and are not
        // awaiting reset, then we need to fetch committed offsets. We will only do a
        // coordinator lookup if there are partitions which have missing positions, so
        // a consumer with manually assigned partitions can avoid a coordinator dependence
        // by always ensuring that assigned partitions have an initial position.
        if (isCommittedOffsetsManagementEnabled() && !refreshCommittedOffsetsIfNeeded(timer))
            return false;


        // If there are partitions still needing a position and a reset policy is defined,
        // request reset using the default policy. If no reset strategy is defined and there
        // are partitions with a missing position, then we will raise a NoOffsetForPartitionException exception.
        subscriptions.resetInitializingPositions();

        // Finally send an asynchronous request to look up and update the positions of any
        // partitions which are awaiting reset.
        ResetPositionsApplicationEvent resetPositionsEvent = new ResetPositionsApplicationEvent();
        eventHandler.add(resetPositionsEvent);
        return true;
    }

    /**
     *
     * Indicates if the consumer is using the Kafka-based offset management strategy,
     * according to config {@link CommonClientConfigs#GROUP_ID_CONFIG}
     */
    private boolean isCommittedOffsetsManagementEnabled() {
        return groupId.isPresent();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     *
     * @param timer Timer bounding how long this method can block
     * @return true iff the operation completed within the timeout
     */
    private boolean refreshCommittedOffsetsIfNeeded(Timer timer) {
        final Set<TopicPartition> initializingPartitions = subscriptions.initializingPartitions();

        log.debug("Refreshing committed offsets for partitions {}", initializingPartitions);
        try {
            final Map<TopicPartition, OffsetAndMetadata> offsets = eventHandler.addAndGet(new OffsetFetchApplicationEvent(initializingPartitions), timer);
            return Utils.refreshCommittedOffsets(offsets, this.metadata, this.subscriptions);
        } catch (org.apache.kafka.common.errors.TimeoutException e) {
            log.error("Couldn't refresh committed offsets before timeout expired");
            return false;
        }
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

    private void throwIfNoAssignorsConfigured() {
        if (assignors.isEmpty())
            throw new IllegalStateException("Must configure at least one partition assigner class name to " +
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG + " configuration property");
    }

    private void updateLastSeenEpochIfNewer(TopicPartition topicPartition, OffsetAndMetadata offsetAndMetadata) {
        if (offsetAndMetadata != null)
            offsetAndMetadata.leaderEpoch().ifPresent(epoch -> metadata.updateLastSeenEpochIfNewer(topicPartition, epoch));
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed", offsets, exception);
        }
    }

    // Functions below are for testing only
    String getClientId() {
        return clientId;
    }

    boolean updateAssignmentMetadataIfNeeded(Timer timer) {
        // Keeping this updateAssignmentMetadataIfNeeded wrapping up the updateFetchPositions as
        // in the previous implementation, because it will eventually involve group coordination
        // logic
        return updateFetchPositions(timer);
    }
}
