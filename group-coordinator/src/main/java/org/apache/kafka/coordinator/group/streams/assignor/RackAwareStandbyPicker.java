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
package org.apache.kafka.coordinator.group.streams.assignor;

import org.apache.kafka.coordinator.group.Utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

/**
 * Picks the conditions for the next standby of one task: the most tag keys that a process with room can meet,
 * preferring the keys earlier in {@code rack.aware.assignment.tags}. A process meets a tag key when it has a value
 * for it that no holder of the task has yet. It can then tell whether a process meets the conditions and return the
 * least-loaded process with room among those that do. The caller reports every change to a load or room with
 * {@link #updated(Object)} or {@link #updatedAll()}.
 * <p>
 * The processes sit in a tree with one level per tag key, those with the same values sharing a leaf. Scanning every
 * leaf per standby is too slow once there are many, e.g. one per process for a host tag. Each node keeps its
 * least-loaded process with room, and its children are ordered by theirs. A query skips a subtree whose own value is
 * used and takes a subtree with no used value without looking inside.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    private final ToDoubleFunction<P> processLoad;
    private final Predicate<P> processHasRoom;
    // The keys in the order of rack.aware.assignment.tags.
    private final List<TagKey> keys;
    // Per key index, the number of values.
    private final int[] numValues;
    // Per level from the top, the index of its key. The priority of a key is still its order in
    // rack.aware.assignment.tags, not this one.
    private final int[] tagKeysInTreeOrder;
    // Per process, its current entry, which knows its leaf.
    private final Map<P, QueuedProcess<P>> queuedByProcess;
    private final Node<P> root;

    // Per key index, one bit per value index, set once a holder of the task carries that value.
    // Cleared by startTask.
    private final BitSet[] usedValues;
    // A process meeting the conditions the last pick returned, which leastLoaded starts from.
    private QueuedProcess<P> foundByPick;

    /**
     * @param tagKeys    The keys of {@code rack.aware.assignment.tags}; not empty, since without them there is nothing to pick.
     * @param processes  All processes of the streams group; an earlier one wins a load tie.
     * @param clientTags The client tags of a process.
     * @param load       The load of a process.
     * @param hasRoom    Whether a process can take another task; {@code process -> true} without a limit.
     */
    RackAwareStandbyPicker(
        final List<String> tagKeys,
        final Collection<P> processes,
        final Function<P, Map<String, String>> clientTags,
        final ToDoubleFunction<P> load,
        final Predicate<P> hasRoom
    ) {
        if (tagKeys.isEmpty()) {
            throw new IllegalArgumentException("Tag keys must not be empty");
        }
        processLoad = load;
        processHasRoom = hasRoom;
        // One level per tag key, so this is also the number of tag keys.
        final int treeHeight = tagKeys.size();
        final List<Map<String, Integer>> indexOfValues = indexValues(tagKeys, processes, clientTags);

        // The keys and the number of values of each, with no value used yet.
        keys = new ArrayList<>(treeHeight);
        numValues = new int[treeHeight];
        usedValues = new BitSet[treeHeight];
        for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
            keys.add(new TagKey(tagKeys.get(keyIndex), keyIndex));
            numValues[keyIndex] = indexOfValues.get(keyIndex).size();
            usedValues[keyIndex] = new BitSet(numValues[keyIndex] + 1);
        }
        tagKeysInTreeOrder = tagKeysInTreeOrder(numValues);

        root = new Node<>(null, -1, -1, treeHeight, false);
        queuedByProcess = Utils.newHashMap(processes.size());
        // The children of each inner node by their value index, only needed to build the tree.
        final Map<Node<P>, Map<Integer, Node<P>>> childrenByValueIndex = new HashMap<>();
        int processIndex = 0;
        for (final P process : processes) {
            insert(
                process,
                processIndex++,
                valueIndexes(clientTags.apply(process), tagKeys, indexOfValues),
                childrenByValueIndex
            );
        }
    }

    /** Per key, the index of each of its values, in the order the values first appear on the processes. */
    private static <P> List<Map<String, Integer>> indexValues(
        final List<String> tagKeys,
        final Collection<P> processes,
        final Function<P, Map<String, String>> clientTags
    ) {
        final List<Map<String, Integer>> indexOfValues = new ArrayList<>(tagKeys.size());
        for (int keyIndex = 0; keyIndex < tagKeys.size(); keyIndex++) {
            final Map<String, Integer> indexOfValue = new HashMap<>();
            for (final P process : processes) {
                final String value = clientTags.apply(process).get(tagKeys.get(keyIndex));
                if (value != null) {
                    indexOfValue.putIfAbsent(value, indexOfValue.size());
                }
            }
            indexOfValues.add(indexOfValue);
        }
        return indexOfValues;
    }

    /** The value index of a process with these tags for each key, the missing value's index for a key it has no value for. */
    private int[] valueIndexes(final Map<String, String> tags, final List<String> tagKeys, final List<Map<String, Integer>> indexOfValues) {
        final int[] valueIndexes = new int[tagKeys.size()];
        for (int keyIndex = 0; keyIndex < valueIndexes.length; keyIndex++) {
            final String value = tags.get(tagKeys.get(keyIndex));
            valueIndexes[keyIndex] = indexOfValues.get(keyIndex).getOrDefault(value, missingValueIndex(keyIndex));
        }
        return valueIndexes;
    }

    /** Adds the process to the leaf of its values, creating the leaf and the inner nodes on its path. */
    private void insert(
        final P process,
        final int processIndex,
        final int[] valueIndexes,
        final Map<Node<P>, Map<Integer, Node<P>>> childrenByValueIndex
    ) {
        final int treeHeight = tagKeysInTreeOrder.length;
        Node<P> node = root;
        for (int level = 0; level < treeHeight; level++) {
            // The values of the process on this level and the ones below are below the node.
            for (int levelBelow = level; levelBelow < treeHeight; levelBelow++) {
                final int keyIndexBelow = tagKeysInTreeOrder[levelBelow];
                node.valuesBelowOf(keyIndexBelow).set(valueIndexes[keyIndexBelow]);
            }
            final int keyIndex = tagKeysInTreeOrder[level];
            final int valueIndex = valueIndexes[keyIndex];
            final Map<Integer, Node<P>> children = childrenByValueIndex.computeIfAbsent(node, __ -> new HashMap<>());
            Node<P> child = children.get(valueIndex);
            if (child == null) {
                child = new Node<>(node, keyIndex, valueIndex, treeHeight, level == treeHeight - 1);
                children.put(valueIndex, child);
            }
            node = child;
        }
        node.valueIndexes = valueIndexes;
        final QueuedProcess<P> queued = new QueuedProcess<>(process, processIndex, node, processLoad.applyAsDouble(process));
        if (processHasRoom.test(process)) {
            node.processesByLoad.add(queued);
            repairPath(node);
        }
        queuedByProcess.put(process, queued);
    }

    /** The key indexes as the levels from the top, fewest values first; the order affects speed, not results. */
    private static int[] tagKeysInTreeOrder(final int[] numValues) {
        final Integer[] keyIndexes = new Integer[numValues.length];
        for (int keyIndex = 0; keyIndex < keyIndexes.length; keyIndex++) {
            keyIndexes[keyIndex] = keyIndex;
        }
        Arrays.sort(
            keyIndexes,
            Comparator.<Integer>comparingInt(keyIndex -> numValues[keyIndex]).thenComparingInt(keyIndex -> keyIndex)
        );
        final int[] tagKeysInTreeOrder = new int[keyIndexes.length];
        for (int level = 0; level < keyIndexes.length; level++) {
            tagKeysInTreeOrder[level] = keyIndexes[level];
        }
        return tagKeysInTreeOrder;
    }

    /** The tag keys in priority order: that of {@code rack.aware.assignment.tags}, not of the levels. */
    List<TagKey> tagKeys() {
        return keys;
    }

    /**
     * Starts the picks for a task, forgetting the values of the holders of the previous task; a missing value stays used.
     *
     * @param holders The holders of the task: its active owner and the standbys placed so far.
     */
    void startTask(final Collection<P> holders) {
        for (final BitSet used : usedValues) {
            used.clear();
        }
        for (final P holder : holders) {
            markUsed(holder);
        }
    }

    /** Records the tag values of a new holder of the task as used. */
    void markUsed(final P holder) {
        final int[] valueIndexes = queuedByProcess.get(holder).leaf.valueIndexes;
        for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
            usedValues[keyIndex].set(valueIndexes[keyIndex]);
        }
    }

    /** Reads the load and room of the process again; call it after either changes. */
    void updated(final P process) {
        queuedByProcess.put(process, requeue(queuedByProcess.get(process)));
    }

    /** Reads the load and room of every process again; call it after a change that may affect many. */
    void updatedAll() {
        queuedByProcess.replaceAll((process, queued) -> requeue(queued));
    }

    /** The conditions for the next standby, or empty once no process with room can make the task more diverse. */
    List<TagKey> pick() {
        // Only if a tag key has an unused value is it possible to choose a process that makes the task more diverse
        // in this tag key.
        final List<TagKey> tagKeysWithUnusedValue = new ArrayList<>();
        for (final TagKey tagKey : keys) {
            if (hasUnusedValue(tagKey)) {
                tagKeysWithUnusedValue.add(tagKey);
            }
        }
        if (tagKeysWithUnusedValue.isEmpty()) {
            return List.of();
        }
        // No process has room for the standby.
        if (root.leastLoaded == null) {
            return List.of();
        }

        // Most picks find a process with room that meets the condition of every such tag key.
        List<TagKey> conditions = tagKeysWithUnusedValue;
        foundByPick = anyProcessMeeting(conditions);
        if (foundByPick == null) {
            // Otherwise the tag keys are added in priority order; a tag key is given up when no process with room
            // meets it together with the tag keys kept so far.
            conditions = new ArrayList<>();
            for (final TagKey tagKey : tagKeysWithUnusedValue) {
                final List<TagKey> withTagKey = new ArrayList<>(conditions);
                withTagKey.add(tagKey);
                // If withTagKey holds every tag key, the query above already found no process for it.
                if (withTagKey.size() < tagKeysWithUnusedValue.size()) {
                    final QueuedProcess<P> foundWithTagKey = anyProcessMeeting(withTagKey);
                    if (foundWithTagKey != null) {
                        conditions = withTagKey;
                        foundByPick = foundWithTagKey;
                    }
                }
            }
        }
        return conditions;
    }

    /** Whether a process is a candidate under the conditions of a pick: it meets them and has room. */
    boolean isCandidate(final P process, final List<TagKey> conditions) {
        return meets(process, conditions) && processHasRoom.test(process);
    }

    /**
     * The least-loaded process with room of the candidates; call it before placing the standby. Only valid for the
     * non-empty conditions {@link #pick()} returned: without conditions, nothing keeps out the holders of the task.
     */
    P leastLoaded(final List<TagKey> conditions) {
        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("Conditions must be the non-empty result of pick()");
        }
        return leastLoadedProcessMeeting(conditions, foundByPick).process;
    }

    /** Whether some value of the key is not used yet. */
    boolean hasUnusedValue(final TagKey key) {
        // The first unused value index is a value, not the missing one after the last value.
        return usedValues[key.index].nextClearBit(0) < numValues[key.index];
    }

    /** Whether the values of the process for {@code conditions} are all unused. */
    boolean meets(final P process, final List<TagKey> conditions) {
        final int[] valueIndexes = queuedByProcess.get(process).leaf.valueIndexes;
        for (final TagKey key : conditions) {
            if (isUsed(key.index, valueIndexes[key.index])) {
                return false;
            }
        }
        return true;
    }

    /** The value index of a missing value, after the last value. */
    private int missingValueIndex(final int keyIndex) {
        return numValues[keyIndex];
    }

    /** Whether the value is used: a holder of the task carries it, or it is missing. */
    private boolean isUsed(final int keyIndex, final int valueIndex) {
        return valueIndex == missingValueIndex(keyIndex) || usedValues[keyIndex].get(valueIndex);
    }

    /** The first process found with room meeting {@code conditions}, or null if none. */
    QueuedProcess<P> anyProcessMeeting(final List<TagKey> conditions) {
        return anyProcessBelowMeeting(root, keyBits(conditions));
    }

    /**
     * The least-loaded process with room meeting {@code conditions}, starting from {@code bound}, a process meeting
     * them found in this pick.
     */
    QueuedProcess<P> leastLoadedProcessMeeting(final List<TagKey> conditions, final QueuedProcess<P> bound) {
        return leastLoadedProcessBelowMeeting(root, keyBits(conditions), bound);
    }

    /** The keys as a BitSet with bit k set for the key with index k. */
    private static BitSet keyBits(final List<TagKey> keys) {
        final BitSet keyBits = new BitSet();
        for (final TagKey key : keys) {
            keyBits.set(key.index);
        }
        return keyBits;
    }

    /** The first process found with room below the node meeting {@code conditionBits}, or null if none. */
    private QueuedProcess<P> anyProcessBelowMeeting(final Node<P> node, final BitSet conditionBits) {
        if (valuesBelowAllMeeting(node, conditionBits)) {
            return node.leastLoaded;
        }
        // Walk into the children in order until one has a process meeting the conditions.
        for (final Node<P> child : node.childrenByLeastLoaded) {
            if (ownValueMeeting(child, conditionBits)) {
                final QueuedProcess<P> found = anyProcessBelowMeeting(child, conditionBits);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * The least-loaded process with room below the node meeting {@code conditionBits} if lighter than {@code bound},
     * the least-loaded process meeting them found so far, else bound.
     */
    private QueuedProcess<P> leastLoadedProcessBelowMeeting(
        final Node<P> node,
        final BitSet conditionBits,
        final QueuedProcess<P> bound
    ) {
        // Every process below the node meets the conditions. The node is the root, with bound below it, or a child
        // walked into for being lighter than bound, so its least-loaded process is never heavier than bound.
        if (valuesBelowAllMeeting(node, conditionBits)) {
            return node.leastLoaded;
        }
        // Walk into the children in order until the next one cannot beat the best found.
        QueuedProcess<P> leastLoaded = bound;
        for (final Node<P> child : node.childrenByLeastLoaded) {
            if (QueuedProcess.ORDER.compare(child.leastLoaded, leastLoaded) >= 0) {
                break;
            }
            if (ownValueMeeting(child, conditionBits)) {
                leastLoaded = leastLoadedProcessBelowMeeting(child, conditionBits, leastLoaded);
            }
        }
        return leastLoaded;
    }

    /** Whether the node's own value meets {@code conditionBits}; every process below the node carries it. */
    private boolean ownValueMeeting(final Node<P> node, final BitSet conditionBits) {
        return !conditionBits.get(node.keyIndex) || !isUsed(node.keyIndex, node.valueIndex);
    }

    /** Whether every value below the node meets {@code conditionBits}. */
    private boolean valuesBelowAllMeeting(final Node<P> node, final BitSet conditionBits) {
        // A leaf has no values below it.
        if (node.isLeaf) {
            return true;
        }
        // One key of conditionBits at a time, lowest index first.
        for (int keyIndex = conditionBits.nextSetBit(0); keyIndex >= 0; keyIndex = conditionBits.nextSetBit(keyIndex + 1)) {
            final BitSet valuesBelow = node.valuesBelow[keyIndex];
            if (valuesBelow == null) {
                continue;
            }
            if (valuesBelow.get(missingValueIndex(keyIndex)) || valuesBelow.intersects(usedValues[keyIndex])) {
                return false;
            }
        }
        return true;
    }

    /**
     * Queues the process with its current load while it has room and repairs its path; returns the entry it is
     * queued with, a new one if its load or room changed.
     */
    private QueuedProcess<P> requeue(final QueuedProcess<P> queued) {
        final double load = processLoad.applyAsDouble(queued.process);
        final boolean hasRoom = processHasRoom.test(queued.process);
        final TreeSet<QueuedProcess<P>> processesByLoad = queued.leaf.processesByLoad;
        if (load == queued.load && hasRoom == processesByLoad.contains(queued)) {
            return queued;
        }
        processesByLoad.remove(queued);
        final QueuedProcess<P> requeued = new QueuedProcess<>(queued.process, queued.processIndex, queued.leaf, load);
        if (hasRoom) {
            processesByLoad.add(requeued);
        }
        repairPath(queued.leaf);
        return requeued;
    }

    /** Updates the least-loaded process of the node and its ancestors, up to the first whose stays the same. */
    private void repairPath(final Node<P> leaf) {
        Node<P> node = leaf;
        while (node != null) {
            final QueuedProcess<P> leastLoaded = node.leastLoadedInQueue();
            if (leastLoaded == node.leastLoaded) {
                return;
            }
            // The parent orders its children by their least-loaded process, so the node leaves it while that changes.
            final Node<P> parent = node.parent;
            if (parent != null && node.leastLoaded != null) {
                parent.childrenByLeastLoaded.remove(node);
            }
            node.leastLoaded = leastLoaded;
            if (parent != null && leastLoaded != null) {
                parent.childrenByLeastLoaded.add(node);
            }
            node = parent;
        }
    }

    /** A key of {@code rack.aware.assignment.tags}. */
    static final class TagKey {
        private final String name;
        private final int index;

        private TagKey(final String name, final int index) {
            this.name = name;
            this.index = index;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /** A process with its index in {@code processes}, its leaf and the load it is queued with; replaced on a change. */
    static final class QueuedProcess<P> {
        // By load, then by process index.
        static final Comparator<QueuedProcess<?>> ORDER = (process1, process2) -> {
            final int byLoad = Double.compare(process1.load, process2.load);
            return byLoad != 0 ? byLoad : Integer.compare(process1.processIndex, process2.processIndex);
        };

        final P process;
        final int processIndex;
        private final Node<P> leaf;
        final double load;

        private QueuedProcess(final P process, final int processIndex, final Node<P> leaf, final double load) {
            this.process = process;
            this.processIndex = processIndex;
            this.leaf = leaf;
            this.load = load;
        }
    }

    private static final class Node<P> {
        private static final Comparator<Node<?>> BY_LEAST_LOADED = (node1, node2) ->
            QueuedProcess.ORDER.compare(node1.leastLoaded, node2.leastLoaded);

        // Null for the root.
        private final Node<P> parent;
        // The key index and value index of the node's own value, -1 for the root.
        private final int keyIndex;
        private final int valueIndex;
        private final boolean isLeaf;
        // Inner nodes only: per key index, the value indexes of the nodes below this one, not its own; null for a key
        // with none.
        private final BitSet[] valuesBelow;
        // Inner nodes only: the children with room below, by their least-loaded process.
        private final TreeSet<Node<P>> childrenByLeastLoaded;
        // Leaves only: the processes with room, by load.
        private final TreeSet<QueuedProcess<P>> processesByLoad;
        // Leaves only: the value index of the processes for each key.
        private int[] valueIndexes;
        // The least-loaded process with room below this node, or null if none.
        private QueuedProcess<P> leastLoaded;

        private Node(final Node<P> parent, final int keyIndex, final int valueIndex, final int numKeys, final boolean isLeaf) {
            this.parent = parent;
            this.keyIndex = keyIndex;
            this.valueIndex = valueIndex;
            this.isLeaf = isLeaf;
            this.valuesBelow = isLeaf ? null : new BitSet[numKeys];
            this.childrenByLeastLoaded = isLeaf ? null : new TreeSet<>(BY_LEAST_LOADED);
            this.processesByLoad = isLeaf ? new TreeSet<>(QueuedProcess.ORDER) : null;
        }

        /** The least-loaded process with room below this node, from the head of its queue. */
        private QueuedProcess<P> leastLoadedInQueue() {
            if (isLeaf) {
                return processesByLoad.isEmpty() ? null : processesByLoad.first();
            }
            return childrenByLeastLoaded.isEmpty() ? null : childrenByLeastLoaded.first().leastLoaded;
        }

        /** The value indexes below this node for the key, created on first use. */
        private BitSet valuesBelowOf(final int keyIndex) {
            if (valuesBelow[keyIndex] == null) {
                valuesBelow[keyIndex] = new BitSet();
            }
            return valuesBelow[keyIndex];
        }
    }
}
