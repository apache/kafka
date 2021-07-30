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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO KAFKA-12648:
//  1) synchronize on these methods instead of individual InternalTopologyBuilder methods, where applicable

public class TopologyMetadata {
    private final Logger log = LoggerFactory.getLogger(TopologyMetadata.class);

    // the "__" (double underscore) string is not allowed for topology names, so it's safe to use to indicate
    // that it's not a named topology
    private static final String UNNAMED_TOPOLOGY = "__UNNAMED_TOPOLOGY__";
    private static final Pattern EMPTY_ZERO_LENGTH_PATTERN = Pattern.compile("");

    private final StreamsConfig config;
    private final SortedMap<String, InternalTopologyBuilder> builders; // Keep sorted by topology name for readability

    private ProcessorTopology globalTopology;
    private Map<String, StateStore> globalStateStores = new HashMap<>();
    final Set<String> allInputTopics = new HashSet<>();

    public TopologyMetadata(final InternalTopologyBuilder builder, final StreamsConfig config) {
        this.config = config;
        builders = new TreeMap<>();
        if (builder.hasNamedTopology()) {
            builders.put(builder.topologyName(), builder);
        } else {
            builders.put(UNNAMED_TOPOLOGY, builder);
        }
    }

    public TopologyMetadata(final SortedMap<String, InternalTopologyBuilder> builders, final StreamsConfig config) {
        this.config = config;
        this.builders = builders;
        if (builders.isEmpty()) {
            log.debug("Building KafkaStreams app with no empty topology");
        }
    }

    public int getNumStreamThreads(final StreamsConfig config) {
        final int configuredNumStreamThreads = config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG);

        // If the application uses named topologies, it's possible to start up with no topologies at all and only add them later
        if (builders.isEmpty()) {
            if (configuredNumStreamThreads != 0) {
                log.info("Overriding number of StreamThreads to zero for empty topology");
            }
            return 0;
        }

        // If there are named topologies but some are empty, this indicates a bug in user code
        if (hasNamedTopologies()) {
            if (hasNoLocalTopology() && !hasGlobalTopology()) {
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

    public boolean hasNamedTopologies() {
        // This includes the case of starting up with no named topologies at all
        return !builders.containsKey(UNNAMED_TOPOLOGY);
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
        return evaluateConditionIsTrueForAnyBuilders(InternalTopologyBuilder::hasOffsetResetOverrides);
    }

    public OffsetResetStrategy offsetResetStrategy(final String topic) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            final OffsetResetStrategy resetStrategy = builder.offsetResetStrategy(topic);
            if (resetStrategy != null) {
                return resetStrategy;
            }
        }
        return null;
    }

    Collection<String> sourceTopicCollection() {
        final List<String> sourceTopics = new ArrayList<>();
        applyToEachBuilder(b -> sourceTopics.addAll(b.sourceTopicCollection()));
        return sourceTopics;
    }

    Pattern sourceTopicPattern() {
        final StringBuilder patternBuilder = new StringBuilder();

        applyToEachBuilder(b -> {
            final String patternString = b.sourceTopicsPatternString();
            if (patternString.length() > 0) {
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

        applyToEachBuilder(b -> {
            sb.append(b.describe().toString());
        });

        return sb.toString();
    }

    public final void buildAndRewriteTopology() {
        applyToEachBuilder(builder -> {
            builder.rewriteTopology(config);
            builder.buildTopology();

            // As we go, check each topology for overlap in the set of input topics/patterns
            final int numInputTopics = allInputTopics.size();
            final List<String> inputTopics = builder.fullSourceTopicNames();
            final Collection<String> inputPatterns = builder.allSourcePatternStrings();

            final int numNewInputTopics = inputTopics.size() + inputPatterns.size();
            allInputTopics.addAll(inputTopics);
            allInputTopics.addAll(inputPatterns);
            if (allInputTopics.size() != numInputTopics + numNewInputTopics) {
                inputTopics.retainAll(allInputTopics);
                inputPatterns.retainAll(allInputTopics);
                inputTopics.addAll(inputPatterns);
                log.error("Tried to add the NamedTopology {} but it had overlap with other input topics: {}", builder.topologyName(), inputTopics);
                throw new TopologyException("Named Topologies may not subscribe to the same input topics or patterns");
            }

            final ProcessorTopology globalTopology = builder.buildGlobalStateTopology();
            if (globalTopology != null) {
                if (builder.topologyName() != null) {
                    throw new IllegalStateException("Global state stores are not supported with Named Topologies");
                } else if (this.globalTopology == null) {
                    this.globalTopology = globalTopology;
                } else {
                    throw new IllegalStateException("Topology builder had global state, but global topology has already been set");
                }
            }
            globalStateStores.putAll(builder.globalStateStores());
        });
    }

    public ProcessorTopology buildSubtopology(final TaskId task) {
        return lookupBuilderForTask(task).buildSubtopology(task.subtopology());
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

    public Map<String, List<String>> stateStoreNameToSourceTopics() {
        final Map<String, List<String>> stateStoreNameToSourceTopics = new HashMap<>();
        applyToEachBuilder(b -> stateStoreNameToSourceTopics.putAll(b.stateStoreNameToSourceTopics()));
        return stateStoreNameToSourceTopics;
    }

    public String getStoreForChangelogTopic(final String topicName) {
        for (final InternalTopologyBuilder builder : builders.values()) {
            final String store = builder.getStoreForChangelogTopic(topicName);
            if (store != null) {
                return store;
            }
        }
        log.warn("Unable to locate any store for topic {}", topicName);
        return "";
    }

    public Collection<String> sourceTopicsForStore(final String storeName) {
        final List<String> sourceTopics = new ArrayList<>();
        applyToEachBuilder(b -> sourceTopics.addAll(b.sourceTopicsForStore(storeName)));
        return sourceTopics;
    }

    public Map<Subtopology, TopicsInfo> topicGroups() {
        final Map<Subtopology, TopicsInfo> topicGroups = new HashMap<>();
        applyToEachBuilder(b -> topicGroups.putAll(b.topicGroups()));
        return topicGroups;
    }

    public Map<String, List<String>> nodeToSourceTopics(final TaskId task) {
        return lookupBuilderForTask(task).nodeToSourceTopics();
    }

    void addSubscribedTopicsFromMetadata(final Set<String> topics, final String logPrefix) {
        applyToEachBuilder(b -> b.addSubscribedTopicsFromMetadata(topics, logPrefix));
    }

    void addSubscribedTopicsFromAssignment(final List<TopicPartition> partitions, final String logPrefix) {
        applyToEachBuilder(b -> b.addSubscribedTopicsFromAssignment(partitions, logPrefix));
    }

    public Collection<Set<String>> copartitionGroups() {
        final List<Set<String>> copartitionGroups = new ArrayList<>();
        applyToEachBuilder(b -> copartitionGroups.addAll(b.copartitionGroups()));
        return copartitionGroups;
    }

    private InternalTopologyBuilder lookupBuilderForTask(final TaskId task) {
        return task.namedTopology() == null ? builders.get(UNNAMED_TOPOLOGY) : builders.get(task.namedTopology());
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

    public static class Subtopology {
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
    }
}
