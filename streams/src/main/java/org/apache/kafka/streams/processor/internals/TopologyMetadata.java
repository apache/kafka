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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig.TaskConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.errors.UnknownTopologyException;
import org.apache.kafka.streams.internals.StreamsConfigUtils;
import org.apache.kafka.streams.internals.StreamsConfigUtils.ProcessingMode;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;

public class TopologyMetadata {
    private Logger log;

    // the "__" (double underscore) string is not allowed for topology names, so it's safe to use to indicate
    // that it's not a named topology
    public static final String UNNAMED_TOPOLOGY = "__UNNAMED_TOPOLOGY__";
    private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");

    private final StreamsConfig config;
    private final ProcessingMode processingMode;
    private final TopologyVersion version;
    private final TaskExecutionMetadata taskExecutionMetadata;
    private final Set<String> pausedTopologies;

    private final ConcurrentNavigableMap<String, InternalTopologyBuilder> builders; // Keep sorted by topology name for readability

    private ProcessorTopology globalTopology;
    private final Map<String, StateStore> globalStateStores = new HashMap<>();
    private final Set<String> allInputTopics = new HashSet<>();
    private final Map<String, Long> threadVersions = new ConcurrentHashMap<>();

    public static class TopologyVersion {
        public AtomicLong topologyVersion = new AtomicLong(0L); // the local topology version
        public ReentrantLock topologyLock = new ReentrantLock();
        public Condition topologyCV = topologyLock.newCondition();
        public List<TopologyVersionListener> activeTopologyUpdateListeners = new LinkedList<>();
    }

    public static class TopologyVersionListener {
        final long topologyVersion; // the (minimum) version to wait for these threads to cross
        final KafkaFutureImpl<Void> future; // the future waiting on all threads to be updated

        public TopologyVersionListener(final long topologyVersion, final KafkaFutureImpl<Void> future) {
            this.topologyVersion = topologyVersion;
            this.future = future;
        }
    }

    public TopologyMetadata(final InternalTopologyBuilder builder,
                            final StreamsConfig config) {
        this.version = new TopologyVersion();
        this.processingMode = StreamsConfigUtils.processingMode(config);
        this.config = config;
        this.log = LoggerFactory.getLogger(getClass());
        this.pausedTopologies = ConcurrentHashMap.newKeySet();

        builders = new ConcurrentSkipListMap<>();
        if (builder.hasNamedTopology()) {
            builders.put(builder.topologyName(), builder);
        } else {
            builders.put(UNNAMED_TOPOLOGY, builder);
        }
        this.taskExecutionMetadata = new TaskExecutionMetadata(builders.keySet(), pausedTopologies, processingMode);
    }

    public TopologyMetadata(final ConcurrentNavigableMap<String, InternalTopologyBuilder> builders,
                            final StreamsConfig config) {
        this.version = new TopologyVersion();
        this.processingMode = StreamsConfigUtils.processingMode(config);
        this.config = config;
        this.log = LoggerFactory.getLogger(getClass());
        this.pausedTopologies = ConcurrentHashMap.newKeySet();

        this.builders = builders;
        if (builders.isEmpty()) {
            log.info("Created an empty KafkaStreams app with no topology");
        }
        this.taskExecutionMetadata = new TaskExecutionMetadata(builders.keySet(), pausedTopologies, processingMode);
    }

    // Need to (re)set the log here to pick up the `processId` part of the clientId in the prefix
    public void setLog(final LogContext logContext) {
        log = logContext.logger(getClass());
    }
    
    public ProcessingMode processingMode() {
        return processingMode;
    }

    public long topologyVersion() {
        return version.topologyVersion.get();
    }

    private void lock() {
        version.topologyLock.lock();
    }

    private void unlock() {
        version.topologyLock.unlock();
    }

    public Collection<String> sourceTopicsForTopology(final String name) {
        return builders.get(name).fullSourceTopicNames();
    }

    public boolean needsUpdate(final String threadName) {
        return threadVersions.get(threadName) < topologyVersion();
    }

    public void registerThread(final String threadName) {
        threadVersions.put(threadName, 0L);
    }

    public void unregisterThread(final String threadName) {
        threadVersions.remove(threadName);
        maybeNotifyTopologyVersionListeners();
    }

    public TaskExecutionMetadata taskExecutionMetadata() {
        return taskExecutionMetadata;
    }

    public void executeTopologyUpdatesAndBumpThreadVersion(final Consumer<Set<String>> handleTopologyAdditions,
                                                           final Consumer<Set<String>> handleTopologyRemovals) {
        try {
            version.topologyLock.lock();
            final long latestTopologyVersion = topologyVersion();
            handleTopologyAdditions.accept(namedTopologiesView());
            handleTopologyRemovals.accept(namedTopologiesView());
            threadVersions.put(Thread.currentThread().getName(), latestTopologyVersion);
        } finally {
            version.topologyLock.unlock();
        }
    }

    public void maybeNotifyTopologyVersionListeners() {
        try {
            lock();
            final long minThreadVersion = minimumThreadVersion();
            final Iterator<TopologyVersionListener> iterator = version.activeTopologyUpdateListeners.listIterator();
            TopologyVersionListener topologyVersionListener;
            while (iterator.hasNext()) {
                topologyVersionListener = iterator.next();
                final long topologyVersionWaitersVersion = topologyVersionListener.topologyVersion;
                if (minThreadVersion >= topologyVersionWaitersVersion) {
                    topologyVersionListener.future.complete(null);
                    iterator.remove();
                    log.info("All threads are now on topology version {}", topologyVersionListener.topologyVersion);
                }
            }
        } finally {
            unlock();
        }
    }

    // Return the minimum version across all live threads, or Long.MAX_VALUE if there are no threads running
    private long minimumThreadVersion() {
        final Optional<Long> minVersion = threadVersions.values().stream().min(Long::compare);
        return minVersion.orElse(Long.MAX_VALUE);
    }

    public void wakeupThreads() {
        try {
            lock();
            version.topologyCV.signalAll();
        } finally {
            unlock();
        }
    }

    public void maybeWaitForNonEmptyTopology(final Supplier<StreamThread.State> threadState) {
        if (isEmpty() && threadState.get().isAlive()) {
            try {
                lock();
                while (isEmpty() && threadState.get().isAlive()) {
                    try {
                        log.debug("Detected that the topology is currently empty, waiting for something to process");
                        version.topologyCV.await();
                    } catch (final InterruptedException e) {
                        log.error("StreamThread was interrupted while waiting on empty topology", e);
                    }
                }
            } finally {
                unlock();
            }
        }
    }

    /**
     * Adds the topology and registers a future that listens for all threads on the older version to see the update
     */
    public void registerAndBuildNewTopology(final KafkaFutureImpl<Void> future, final InternalTopologyBuilder newTopologyBuilder) {
        try {
            lock();
            buildAndVerifyTopology(newTopologyBuilder);
            log.info("New NamedTopology {} passed validation and will be added, old topology version is {}", newTopologyBuilder.topologyName(), version.topologyVersion.get());
            version.topologyVersion.incrementAndGet();
            version.activeTopologyUpdateListeners.add(new TopologyVersionListener(topologyVersion(), future));
            builders.put(newTopologyBuilder.topologyName(), newTopologyBuilder);
            wakeupThreads();
            log.info("Added NamedTopology {} and updated topology version to {}", newTopologyBuilder.topologyName(), version.topologyVersion.get());
        } catch (final Throwable throwable) {
            log.error("Failed to add NamedTopology {}, please retry the operation.", newTopologyBuilder.topologyName());
            future.completeExceptionally(throwable);
        } finally {
            unlock();
        }
    }

    /**
     * Pauses a topology by name
     * @param topologyName Name of the topology to pause
     */
    public void pauseTopology(final String topologyName) {
        pausedTopologies.add(topologyName);
    }

    /**
     * Checks if a given topology is paused.
     * @param topologyName If null, assume that we are checking the `UNNAMED_TOPOLOGY`.
     * @return A boolean indicating if the topology is paused.
     */
    public boolean isPaused(final String topologyName) {
        return pausedTopologies.contains(getTopologyNameOrElseUnnamed(topologyName));
    }

    /**
     * Resumes a topology by name
     * @param topologyName Name of the topology to resume
     */
    public void resumeTopology(final String topologyName) {
        pausedTopologies.remove(topologyName);
    }

    /**
     * Removes the topology and registers a future that listens for all threads on the older version to see the update
     */
    public KafkaFuture<Void> unregisterTopology(final KafkaFutureImpl<Void> removeTopologyFuture,
                                                final String topologyName) {
        try {
            lock();
            log.info("Beginning removal of NamedTopology {}, old topology version is {}", topologyName, version.topologyVersion.get());
            version.topologyVersion.incrementAndGet();
            version.activeTopologyUpdateListeners.add(new TopologyVersionListener(topologyVersion(), removeTopologyFuture));
            final InternalTopologyBuilder removedBuilder = builders.remove(topologyName);
            removedBuilder.fullSourceTopicNames().forEach(allInputTopics::remove);
            removedBuilder.allSourcePatternStrings().forEach(allInputTopics::remove);
            log.info("Finished removing NamedTopology {}, topology version was updated to {}", topologyName, version.topologyVersion.get());
        } catch (final Throwable throwable) {
            log.error("Failed to remove NamedTopology {}, please retry.", topologyName);
            removeTopologyFuture.completeExceptionally(throwable);
        } finally {
            unlock();
        }
        return removeTopologyFuture;
    }

    public TaskConfig taskConfig(final TaskId taskId) {
        final InternalTopologyBuilder builder = lookupBuilderForTask(taskId);
        return builder.topologyConfigs().getTaskConfig();
    }

    public void buildAndRewriteTopology() {
        applyToEachBuilder(this::buildAndVerifyTopology);
    }

    private void buildAndVerifyTopology(final InternalTopologyBuilder builder) {
        builder.rewriteTopology(config);
        builder.buildTopology();

        final Set<String> allInputTopicsCopy = new HashSet<>(allInputTopics);

        // As we go, check each topology for overlap in the set of input topics/patterns
        final int numInputTopics = allInputTopicsCopy.size();
        final List<String> inputTopics = builder.fullSourceTopicNames();
        final Collection<String> inputPatterns = builder.allSourcePatternStrings();

        final Set<String> newInputTopics = new HashSet<>(inputTopics);
        newInputTopics.addAll(inputPatterns);

        final int numNewInputTopics = newInputTopics.size();
        allInputTopicsCopy.addAll(newInputTopics);

        if (allInputTopicsCopy.size() != numInputTopics + numNewInputTopics) {
            inputTopics.retainAll(allInputTopicsCopy);
            inputPatterns.retainAll(allInputTopicsCopy);
            log.error("Tried to add the NamedTopology {} but it had overlap with other input topics {} or patterns {}",
                      builder.topologyName(), inputTopics, inputPatterns);
            throw new TopologyException("Named Topologies may not subscribe to the same input topics or patterns");
        }

        final ProcessorTopology globalTopology = builder.buildGlobalStateTopology();
        if (globalTopology != null) {
            if (builder.topologyName() != null) {
                throw new IllegalStateException("Global state stores are not supported with Named Topologies");
            } else if (this.globalTopology != null) {
                throw new TopologyException("Topology builder had global state, but global topology has already been set");
            } else {
                this.globalTopology = globalTopology;
                globalStateStores.putAll(builder.globalStateStores());
            }
        }
        allInputTopics.addAll(newInputTopics);
    }

    public int numStreamThreads(final StreamsConfig config) {
        final int configuredNumStreamThreads = config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG);

        // If there are named topologies but some are empty, this indicates a bug in user code
        if (hasNamedTopologies()) {
            if (hasNoLocalTopology()) {
                log.error("Detected a named topology with no input topics, a named topology may not be empty.");
                throw new TopologyException("Topology has no stream threads and no global threads, " +
                                                "must subscribe to at least one source topic or pattern.");
            }
        } else {
            // If both the global and non-global topologies are empty, this indicates a bug in user code
            if (hasNoLocalTopology() && !hasGlobalTopology()) {
                log.error("Topology with no input topics will create no stream threads and no global thread.");
                throw new TopologyException("Topology has no stream threads and no global threads, " +
                                                "must subscribe to at least one source topic or global table.");
            }
        }

        // Lastly we check for an empty non-global topology and override the threads to zero if set otherwise
        if (configuredNumStreamThreads != 0 && hasNoLocalTopology()) {
            log.info("Overriding number of StreamThreads to zero for global-only topology");
            return 0;
        }

        return configuredNumStreamThreads;
    }

    /**
     * @return true iff the app is using named topologies, or was started up with no topology at all
     */
    public boolean hasNamedTopologies() {
        return !builders.containsKey(UNNAMED_TOPOLOGY);
    }

    public Set<String> namedTopologiesView() {
        return hasNamedTopologies() ? Collections.unmodifiableSet(builders.keySet()) : emptySet();
    }

    /**
     * @return true iff any of the topologies have a global topology
     */
    public boolean hasGlobalTopology() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasGlobalStores);
    }

    /**
     * @return true iff any of the topologies have no local (aka non-global) topology
     */
    public boolean hasNoLocalTopology() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasNoLocalTopology);
    }

    public boolean hasPersistentStores() {
        // If the app is using named topologies, there may not be any persistent state when it first starts up
        // but a new NamedTopology may introduce it later, so we must return true
        if (hasNamedTopologies()) {
            return true;
        }
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasPersistentStores);
    }

    public boolean hasStore(final String name) {
        return evaluateConditionIsTrueForAnyBuilders(b -> b.hasStore(name));
    }

    public boolean hasOffsetResetOverrides() {
        // Return true if using named topologies, as there may be named topologies added later which do have overrides
        return hasNamedTopologies() || evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasOffsetResetOverrides);
    }

    public Optional<AutoOffsetResetStrategy> offsetResetStrategy(final String topic) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            if (builder.containsTopic(topic)) {
                return Optional.ofNullable(builder.offsetResetStrategy(topic));
            }
        }
        log.warn("Unable to look up offset reset strategy for topic {} " +
            "as this topic does not appear in the sources of any of the current topologies: {}\n " +
                "This may be due to natural race condition when removing a topology but it should not " +
                "persist or appear frequently.",
            topic, namedTopologiesView()
        );
        // returning `null` for an Optional return type triggers spotbugs
        // we added an exception for NP_OPTIONAL_RETURN_NULL for this method
        // when we remove NamedTopologies, we can remove this exception
        return null;
    }


    public Collection<String> fullSourceTopicNamesForTopology(final String topologyName) {
        Objects.requireNonNull(topologyName, "topology name must not be null");
        return lookupBuilderForNamedTopology(topologyName).fullSourceTopicNames();
    }

    public Collection<String> allFullSourceTopicNames() {
        final List<String> sourceTopics = new ArrayList<>();
        applyToEachBuilder(b -> sourceTopics.addAll(b.fullSourceTopicNames()));
        return sourceTopics;
    }

    Pattern sourceTopicPattern() {
        final StringBuilder patternBuilder = new StringBuilder();

        applyToEachBuilder(b -> {
            final String patternString = b.sourceTopicPatternString();
            if (!patternString.isEmpty()) {
                patternBuilder.append(patternString).append("|");
            }
        });

        if (patternBuilder.length() > 0) {
            patternBuilder.setLength(patternBuilder.length() - 1);
            return Pattern.compile(patternBuilder.toString());
        } else {
            return EMPTY_ZERO_LENGTH_PATTERN;
        }
    }

    public boolean usesPatternSubscription() {
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::usesPatternSubscription);
    }

    // Can be empty if app is started up with no Named Topologies, in order to add them on later
    public boolean isEmpty() {
        return builders.isEmpty();
    }

    public String topologyDescriptionString() {
        if (isEmpty()) {
            return "";
        }
        final StringBuilder sb = new StringBuilder();

        applyToEachBuilder(b -> sb.append(b.describe().toString()));

        return sb.toString();
    }

    /**
     * @return the {@link ProcessorTopology subtopology} built for this task, guaranteed to be non-null
     *
     * @throws UnknownTopologyException  if the task is from a named topology that this client isn't aware of
     */
    public ProcessorTopology buildSubtopology(final TaskId task) {
        final InternalTopologyBuilder builder = lookupBuilderForTask(task);
        return builder.buildSubtopology(task.subtopology());
    }

    public ProcessorTopology globalTaskTopology() {
        if (hasNamedTopologies()) {
            throw new IllegalStateException("Global state stores are not supported with Named Topologies");
        }
        return globalTopology;
    }

    public Map<String, StateStore> globalStateStores() {
        return globalStateStores;
    }

    public Map<String, List<String>> stateStoreNameToSourceTopicsForTopology(final String topologyName) {
        return lookupBuilderForNamedTopology(topologyName).stateStoreNameToFullSourceTopicNames();
    }

    public Set<String> stateStoreNamesForSubtopology(final String topologyName, final int subtopologyId) {
        return lookupBuilderForNamedTopology(topologyName).stateStoreNamesForSubtopology(subtopologyId);
    }

    public Map<String, List<String>> stateStoreNameToSourceTopics() {
        final Map<String, List<String>> stateStoreNameToSourceTopics = new HashMap<>();
        applyToEachBuilder(b -> stateStoreNameToSourceTopics.putAll(b.stateStoreNameToFullSourceTopicNames()));
        return stateStoreNameToSourceTopics;
    }

    public String storeForChangelogTopic(final String topicName) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            final String store = builder.storeForChangelogTopic(topicName);
            if (store != null) {
                return store;
            }
        }
        log.warn("Unable to locate any store for topic {}", topicName);
        return "";
    }

    /**
     * @param storeName       the name of the state store
     * @param topologyName    the name of the topology to search for stores within
     * @return topics subscribed from source processors that are connected to these state stores
     */
    public Collection<String> sourceTopicsForStore(final String storeName, final String topologyName) {
        return lookupBuilderForNamedTopology(topologyName).sourceTopicsForStore(storeName);
    }

    public static String getTopologyNameOrElseUnnamed(final String topologyName) {
        return topologyName == null ? UNNAMED_TOPOLOGY : topologyName;
    }

    /**
     * @param topologiesToExclude the names of any topologies to exclude from the returned topic groups,
     *                            eg because they have missing source topics and can't be processed yet
     *
     * @return                    flattened map of all subtopologies (from all topologies) to topics info
     */
    public Map<Subtopology, TopicsInfo> subtopologyTopicsInfoMapExcluding(final Set<String> topologiesToExclude) {
        final Map<Subtopology, TopicsInfo> subtopologyTopicsInfo = new HashMap<>();
        applyToEachBuilder(b -> {
            if (!topologiesToExclude.contains(b.topologyName())) {
                subtopologyTopicsInfo.putAll(b.subtopologyToTopicsInfo());
            }
        });
        return subtopologyTopicsInfo;
    }

    /**
     * @return    map from topology to its subtopologies and their topics info
     */
    public Map<String, Map<Subtopology, TopicsInfo>> topologyToSubtopologyTopicsInfoMap() {
        final Map<String, Map<Subtopology, TopicsInfo>> topologyToSubtopologyTopicsInfoMap = new HashMap<>();
        applyToEachBuilder(b -> topologyToSubtopologyTopicsInfoMap.put(b.topologyName(), b.subtopologyToTopicsInfo()));
        return  topologyToSubtopologyTopicsInfoMap;
    }

    public Map<String, List<String>> nodeToSourceTopics(final TaskId task) {
        return lookupBuilderForTask(task).nodeToSourceTopics();
    }

    void addSubscribedTopicsFromMetadata(final Set<String> topics, final String logPrefix) {
        applyToEachBuilder(b -> b.addSubscribedTopicsFromMetadata(topics, logPrefix));
    }

    void addSubscribedTopicsFromAssignment(final Set<TopicPartition> partitions, final String logPrefix) {
        applyToEachBuilder(b -> b.addSubscribedTopicsFromAssignment(partitions, logPrefix));
    }

    public Collection<Set<String>> copartitionGroups() {
        final List<Set<String>> copartitionGroups = new ArrayList<>();
        applyToEachBuilder(b -> copartitionGroups.addAll(b.copartitionGroups()));
        return copartitionGroups;
    }

    /**
     * @return the {@link InternalTopologyBuilder} for this task's topology, guaranteed to be non-null
     *
     * @throws UnknownTopologyException  if the task is from a named topology that this client isn't aware of
     */
    private InternalTopologyBuilder lookupBuilderForTask(final TaskId task) {
        final InternalTopologyBuilder builder = task.topologyName() == null ?
            builders.get(UNNAMED_TOPOLOGY) :
            builders.get(task.topologyName());
        if (builder == null) {
            throw new UnknownTopologyException("Unable to locate topology builder", task.topologyName());
        } else {
            return builder;
        }
    }

    @SuppressWarnings("deprecation")
    public Collection<NamedTopology> allNamedTopologies() {
        return builders.values()
            .stream()
            .map(InternalTopologyBuilder::namedTopology)
            .collect(Collectors.toSet());
    }


    /**
     * @return the InternalTopologyBuilder for the NamedTopology with the given {@code topologyName}
     *         or the builder for a regular Topology if {@code topologyName} is {@code null},
     *         else returns {@code null} if {@code topologyName} is non-null but no such NamedTopology exists
     */
    public InternalTopologyBuilder lookupBuilderForNamedTopology(final String topologyName) {
        return builders.get(getTopologyNameOrElseUnnamed(topologyName));
    }

    private boolean evaluateConditionIsTrueForAnyBuilders(final Function<InternalTopologyBuilder, Boolean> condition) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            if (condition.apply(builder)) {
                return true;
            }
        }
        return false;
    }

    private void applyToEachBuilder(final Consumer<InternalTopologyBuilder> function) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            function.accept(builder);
        }
    }

    public static class Subtopology implements Comparable<Subtopology> {
        final int nodeGroupId;
        final String namedTopology;

        public Subtopology(final int nodeGroupId, final String namedTopology) {
            this.nodeGroupId = nodeGroupId;
            this.namedTopology = namedTopology;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Subtopology that = (Subtopology) o;
            return nodeGroupId == that.nodeGroupId &&
                    Objects.equals(namedTopology, that.namedTopology);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeGroupId, namedTopology);
        }

        @Override
        public int compareTo(final Subtopology other) {
            if (nodeGroupId != other.nodeGroupId) {
                return Integer.compare(nodeGroupId, other.nodeGroupId);
            }
            if (namedTopology == null) {
                return other.namedTopology == null ? 0 : -1;
            }
            if (other.namedTopology == null) {
                return 1;
            }

            // Both not null
            return namedTopology.compareTo(other.namedTopology);
        }
    }
}
