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
package org.apache.kafka.streams;

import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;
import org.apache.kafka.streams.processor.internals.ProcessorTopology;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Computes the multi-partition layout for a {@link TopologyTestDriver}: which node groups are task
 * sub-topologies, how many partitions each (input, output, and internal repartition) topic has, and
 * the resulting partition count of each sub-topology. This is pure planning logic — it reads the
 * topology and the user-declared partition counts and produces data structures; it builds no tasks
 * and touches no producer or consumer. {@link TopologyTestDriver#init()} runs {@link #compute()}
 * once and hands the results to the runtime that builds the task graph.
 *
 * <p>The partition count of an internal repartition topic is resolved by a layered rule, highest
 * precedence first: an explicit {@code Repartitioned.withNumberOfPartitions(N)} (or a count the user
 * declared); inheritance from a co-partition-group peer; the max partition count across the producing
 * sub-topology's source topics; otherwise a fallback to 1 with a warning. The resolution iterates to
 * a fixed point because chains of internal topics can depend on each other.</p>
 */
final class MultiPartitionTopologyPlan {

    private static final Logger log = LoggerFactory.getLogger(MultiPartitionTopologyPlan.class);

    private final InternalTopologyBuilder internalTopologyBuilder;
    private final Set<String> globalSourceTopics;

    // Resolved partition count per topic. Seeded with the user's declarations and completed by compute().
    private final Map<String, Integer> partitionsByTopic;

    private final List<Integer> subtopologyIds = new ArrayList<>();
    private final Map<Integer, ProcessorTopology> subtopologyTopologies = new HashMap<>();
    private final Map<Integer, Integer> partitionsBySubtopology = new HashMap<>();
    private final Map<String, Integer> subtopologyByInputTopic = new HashMap<>();
    private final Map<String, Integer> sinkTopicToSubtopology = new HashMap<>();

    /**
     * @param internalTopologyBuilder the rewritten topology under test
     * @param globalTopology          the global {@link ProcessorTopology}, or {@code null} if the
     *                                topology has no global stores; its source topics are excluded
     *                                from task sub-topologies
     * @param declaredPartitions      the user-declared partition counts (copied defensively)
     */
    MultiPartitionTopologyPlan(final InternalTopologyBuilder internalTopologyBuilder,
                               final ProcessorTopology globalTopology,
                               final Map<String, Integer> declaredPartitions) {
        this.internalTopologyBuilder = internalTopologyBuilder;
        this.globalSourceTopics = globalTopology == null
            ? Collections.emptySet()
            : new HashSet<>(globalTopology.sourceTopics());
        this.partitionsByTopic = new HashMap<>(declaredPartitions);
    }

    /**
     * Run the full planning sequence: enumerate task sub-topologies, resolve every topic's partition
     * count, validate co-partitioning, and compute each sub-topology's partition count.
     *
     * @throws TopologyException if a co-partition group has mismatching declared partition counts
     */
    void compute() {
        enumerateTaskSubtopologies();
        final Set<String> internalRepartitionTopics = collectInternalRepartitionTopics();
        seedPartitionCounts(internalRepartitionTopics);
        resolveInternalRepartitionTopicPartitions(internalRepartitionTopics);
        validateCopartitioning();
        computePartitionsBySubtopology();
    }

    // --- results ---

    List<Integer> subtopologyIds() {
        return Collections.unmodifiableList(subtopologyIds);
    }

    ProcessorTopology subtopology(final int subtopologyId) {
        return subtopologyTopologies.get(subtopologyId);
    }

    int partitionsOfSubtopology(final int subtopologyId) {
        return partitionsBySubtopology.getOrDefault(subtopologyId, 0);
    }

    int partitionsOfTopic(final String topic) {
        return partitionsByTopic.getOrDefault(topic, 1);
    }

    Integer subtopologyForInputTopic(final String topic) {
        return subtopologyByInputTopic.get(topic);
    }

    Map<String, Integer> resolvedTopicPartitions() {
        return Collections.unmodifiableMap(partitionsByTopic);
    }

    // --- planning steps ---

    /**
     * Build each task sub-topology's {@link ProcessorTopology} and record the {@code (sink topic ->
     * sub-topology)} mapping. A node group whose only source topics are global is not a task
     * sub-topology (global state is fed separately), so it is skipped -- this mirrors
     * {@code InternalTopologyBuilder#subtopologyToTopicsInfo()}, which drops any group left with no
     * non-global source topic.
     */
    private void enumerateTaskSubtopologies() {
        for (final int id : internalTopologyBuilder.nodeGroups().keySet()) {
            final ProcessorTopology pt = internalTopologyBuilder.buildSubtopology(id);
            if (!hasNonGlobalSourceTopic(pt, globalSourceTopics)) {
                continue;
            }
            subtopologyIds.add(id);
            subtopologyTopologies.put(id, pt);
            // A repartition topic is produced by the sub-topology whose sink writes it; remember that
            // mapping so the upstream-max resolution can find the producer.
            for (final String sink : pt.sinkTopics()) {
                sinkTopicToSubtopology.putIfAbsent(sink, id);
            }
        }
        Collections.sort(subtopologyIds);
    }

    private static boolean hasNonGlobalSourceTopic(final ProcessorTopology pt,
                                                   final Set<String> globalSourceTopics) {
        for (final String src : pt.sourceTopics()) {
            if (!globalSourceTopics.contains(src)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Seed the partition-count map before the 3-layer resolution runs: pin any repartition topic the
     * builder already fixed (e.g. via {@code Repartitioned.withNumberOfPartitions}) so the upstream-max
     * layer cannot overwrite it, and default user-declared source topics to 1. Internal repartition
     * topics are deliberately left unset here so {@link #resolveInternalRepartitionTopicPartitions(Set)}
     * resolves them.
     */
    private void seedPartitionCounts(final Set<String> allInternalRepartitionTopics) {
        for (final Map.Entry<String, Integer> entry : explicitRepartitionTopicPartitionCounts().entrySet()) {
            partitionsByTopic.putIfAbsent(entry.getKey(), entry.getValue());
        }
        for (final int sid : subtopologyIds) {
            final ProcessorTopology pt = subtopologyTopologies.get(sid);
            for (final String src : pt.sourceTopics()) {
                subtopologyByInputTopic.put(src, sid);
                if (!allInternalRepartitionTopics.contains(src)) {
                    partitionsByTopic.putIfAbsent(src, 1);
                }
            }
        }
    }

    /** Per-sub-topology partition count = max across its source topics. */
    private void computePartitionsBySubtopology() {
        for (final int sid : subtopologyIds) {
            final ProcessorTopology pt = subtopologyTopologies.get(sid);
            int max = 1;
            for (final String src : pt.sourceTopics()) {
                max = Math.max(max, partitionsByTopic.getOrDefault(src, 1));
            }
            partitionsBySubtopology.put(sid, max);
        }
    }

    private Set<String> collectInternalRepartitionTopics() {
        final Set<String> internalTopics = new HashSet<>();
        for (final InternalTopologyBuilder.TopicsInfo info : internalTopologyBuilder.subtopologyToTopicsInfo().values()) {
            internalTopics.addAll(info.repartitionSourceTopics.keySet());
        }
        return internalTopics;
    }

    /**
     * Internal repartition topics whose partition count was pinned explicitly (e.g. via
     * {@code Repartitioned.withNumberOfPartitions}); topics left to upstream inheritance are absent.
     */
    private Map<String, Integer> explicitRepartitionTopicPartitionCounts() {
        final Map<String, Integer> result = new HashMap<>();
        for (final InternalTopologyBuilder.TopicsInfo info : internalTopologyBuilder.subtopologyToTopicsInfo().values()) {
            info.repartitionSourceTopics.forEach((name, config) ->
                config.numberOfPartitions().ifPresent(n -> result.put(name, n)));
        }
        return result;
    }

    /**
     * Resolve partition counts for internal repartition topics using the 3-layer rule:
     * (1) explicit declaration wins; (2) co-partition group inheritance from a declared peer; (3) max
     * partition count across the producing sub-topology's source topics; iterate to a fixed point
     * because chains of internal topics can depend on each other. Topics still unresolved fall back to
     * 1 partition with a warning.
     */
    private void resolveInternalRepartitionTopicPartitions(final Set<String> internalTopics) {
        boolean changed = true;
        while (changed) {
            changed = false;
            for (final String topic : internalTopics) {
                if (tryResolveOneInternalTopic(topic)) {
                    changed = true;
                }
            }
        }

        for (final String topic : internalTopics) {
            if (!partitionsByTopic.containsKey(topic)) {
                log.warn("Could not resolve partition count for internal repartition topic '{}'; defaulting to 1. "
                    + "Declare it explicitly via declareTopic() if a different count is needed.", topic);
                partitionsByTopic.put(topic, 1);
            }
        }
    }

    /**
     * Try to resolve a single internal repartition topic's partition count this round. Returns
     * {@code true} if progress was made (the topic now has a count); {@code false} if it cannot be
     * resolved yet (caller will iterate to a fixed point) or is already resolved.
     */
    private boolean tryResolveOneInternalTopic(final String topic) {
        if (partitionsByTopic.containsKey(topic)) {
            return false;
        }
        final Integer fromCopartition = resolveFromCopartitionGroup(topic);
        if (fromCopartition != null) {
            partitionsByTopic.put(topic, fromCopartition);
            return true;
        }
        return tryResolveFromUpstreamSubtopology(topic);
    }

    private boolean tryResolveFromUpstreamSubtopology(final String topic) {
        final Integer producerSid = sinkTopicToSubtopology.get(topic);
        if (producerSid == null) {
            return false;
        }
        final ProcessorTopology pt = subtopologyTopologies.get(producerSid);
        if (pt == null) {
            return false;
        }
        Integer max = null;
        for (final String src : pt.sourceTopics()) {
            final Integer n = partitionsByTopic.get(src);
            if (n == null) {
                return false;
            }
            if (max == null || n > max) {
                max = n;
            }
        }
        if (max == null) {
            return false;
        }
        partitionsByTopic.put(topic, max);
        return true;
    }

    /**
     * If {@code topic} participates in a co-partition group with any topic that already has a declared
     * count, return that count. Returns {@code null} if the topic is unconstrained by co-partitioning.
     */
    private Integer resolveFromCopartitionGroup(final String topic) {
        for (final Set<String> group : internalTopologyBuilder.copartitionGroups()) {
            if (!group.contains(topic)) {
                continue;
            }
            for (final String peer : group) {
                if (peer.equals(topic)) {
                    continue;
                }
                final Integer n = partitionsByTopic.get(peer);
                if (n != null) {
                    return n;
                }
            }
        }
        return null;
    }

    /**
     * Validate that all topics in each co-partition group share the same declared partition count.
     * Throws {@link TopologyException} naming the two witnessing topics on conflict.
     */
    private void validateCopartitioning() {
        for (final Set<String> group : internalTopologyBuilder.copartitionGroups()) {
            Integer expected = null;
            String witness = null;
            for (final String topic : group) {
                final Integer n = partitionsByTopic.get(topic);
                if (n == null) {
                    continue;
                }
                if (expected == null) {
                    expected = n;
                    witness = topic;
                } else if (!expected.equals(n)) {
                    throw new TopologyException(
                        "Co-partitioned topics have mismatching partition counts: '" + witness + "' has "
                            + expected + " but '" + topic + "' has " + n
                            + ". Declare matching counts via declareTopic() before piping records.");
                }
            }
        }
    }
}
