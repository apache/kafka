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
import java.util.stream.IntStream;

/**
 * Picks the conditions for the next standby of one task: the most tag keys that a process with room can meet,
 * preferring the keys earlier in {@code rack.aware.assignment.tags}. A process meets a tag key when it has a value
 * for it that no holder of the task has yet. A pick also returns the least-loaded process with room that meets the
 * conditions, and the picker can tell whether a process meets them. The caller reports every change to a load or room
 * with {@link #updated(Object)} or {@link #updatedAll()}.
 * <p>
 * The processes sit in a tree with one level per tag key, those with the same values sharing a leaf. Scanning every
 * leaf per standby is too slow once there are many, e.g. one per process for a host tag. Each node keeps its
 * least-loaded process with room, and its children are ordered by theirs. A query skips a subtree whose own value is
 * used and stops at the first child no lighter than the best process found.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    // The value index of a process for a key it has no value for.
    private static final int MISSING_VALUE_INDEX = -1;
    // The key index and value index of the root, which has no own value.
    private static final int ROOT_PLACEHOLDER_INDEX = -2;

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

    /**
     * @param tagKeys    The keys of {@code rack.aware.assignment.tags}; not empty, since without them there is nothing to pick.
     * @param processes  All processes of the streams group, distinct; an earlier one wins a load tie.
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
        // Per key, the index of each of its values, in the order the values first appear on the processes.
        final List<Map<String, Integer>> valueIndexAllKeys = new ArrayList<>(treeHeight);
        for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
            valueIndexAllKeys.add(new HashMap<>());
        }
        // Per process in order, its value index for each key.
        final List<int[]> valueIndexesOfProcesses = new ArrayList<>(processes.size());
        for (final P process : processes) {
            final Map<String, String> tags = clientTags.apply(process);
            final int[] valueIndexes = new int[treeHeight];
            for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
                final String value = tags.get(tagKeys.get(keyIndex));
                final Map<String, Integer> valueIndexPerKey = valueIndexAllKeys.get(keyIndex);
                valueIndexes[keyIndex] = value == null
                    ? MISSING_VALUE_INDEX
                    : valueIndexPerKey.computeIfAbsent(value, __ -> valueIndexPerKey.size());
            }
            valueIndexesOfProcesses.add(valueIndexes);
        }

        // The keys and the number of values of each, with no value used yet.
        keys = new ArrayList<>(treeHeight);
        numValues = new int[treeHeight];
        usedValues = new BitSet[treeHeight];
        for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
            keys.add(new TagKey(tagKeys.get(keyIndex), keyIndex));
            numValues[keyIndex] = valueIndexAllKeys.get(keyIndex).size();
            usedValues[keyIndex] = new BitSet(numValues[keyIndex]);
        }
        tagKeysInTreeOrder = tagKeysInTreeOrder(numValues);

        root = new Node<>(null, ROOT_PLACEHOLDER_INDEX, ROOT_PLACEHOLDER_INDEX, false, null);
        queuedByProcess = Utils.newHashMap(processes.size());
        // The children of each inner node by their value index, only needed to build the tree.
        final Map<Node<P>, Map<Integer, Node<P>>> childrenByValueIndex = new HashMap<>();
        int processIndex = 0;
        for (final P process : processes) {
            insert(
                process,
                processIndex,
                valueIndexesOfProcesses.get(processIndex),
                childrenByValueIndex
            );
            processIndex++;
        }
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
            final int keyIndex = tagKeysInTreeOrder[level];
            final int valueIndex = valueIndexes[keyIndex];
            final Map<Integer, Node<P>> children = childrenByValueIndex.computeIfAbsent(node, __ -> new HashMap<>());
            Node<P> child = children.get(valueIndex);
            if (child == null) {
                child = new Node<>(node, keyIndex, valueIndex, level == treeHeight - 1, valueIndexes);
                children.put(valueIndex, child);
            }
            node = child;
        }
        final QueuedProcess<P> queued = new QueuedProcess<>(process, processIndex, node, processLoad.applyAsDouble(process));
        if (processHasRoom.test(process)) {
            node.processesByLoad.add(queued);
            repairPath(node);
        }
        queuedByProcess.put(process, queued);
    }

    /** The key indexes as the levels from the top, fewest values first; the order affects speed, not results. */
    private static int[] tagKeysInTreeOrder(final int[] numValues) {
        return IntStream.range(0, numValues.length)
            .boxed()
            .sorted(Comparator.comparingInt(keyIndex -> numValues[keyIndex]))
            .mapToInt(Integer::intValue)
            .toArray();
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
            // A missing value has no bit: isUsed counts it as used.
            if (valueIndexes[keyIndex] != MISSING_VALUE_INDEX) {
                usedValues[keyIndex].set(valueIndexes[keyIndex]);
            }
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

    /**
     * The conditions for the next standby and the least-loaded candidate under them, or null once no process with
     * room can make the task more diverse.
     */
    Pick<P> pick() {
        // No process has room for the standby.
        if (root.leastLoaded == null) {
            return null;
        }
        // Only if a tag key has an unused value is it possible to choose a process that makes the task more diverse
        // in this tag key.
        final List<TagKey> tagKeysWithUnusedValue = new ArrayList<>();
        for (final TagKey tagKey : keys) {
            if (hasUnusedValue(tagKey)) {
                tagKeysWithUnusedValue.add(tagKey);
            }
        }
        if (tagKeysWithUnusedValue.isEmpty()) {
            return null;
        }

        // Most picks find a process with room that meets the condition of every such tag key.
        final QueuedProcess<P> meetingAll = leastLoadedProcessMeeting(tagKeysWithUnusedValue);
        if (meetingAll != null) {
            return new Pick<>(tagKeysWithUnusedValue, meetingAll.process);
        }
        // Otherwise the tag keys are added in priority order; a tag key is given up when no process with room
        // meets it together with the tag keys kept so far.
        List<TagKey> conditions = new ArrayList<>();
        QueuedProcess<P> leastLoaded = null;
        for (final TagKey tagKey : tagKeysWithUnusedValue) {
            final List<TagKey> withTagKey = new ArrayList<>(conditions);
            withTagKey.add(tagKey);
            // If withTagKey holds every tag key, the query above already found no process for it.
            if (withTagKey.size() < tagKeysWithUnusedValue.size()) {
                final QueuedProcess<P> meeting = leastLoadedProcessMeeting(withTagKey);
                if (meeting != null) {
                    conditions = withTagKey;
                    leastLoaded = meeting;
                }
            }
        }
        return leastLoaded == null ? null : new Pick<>(conditions, leastLoaded.process);
    }

    /** Whether a process is a candidate under the conditions of a pick: it meets them and has room. */
    boolean isCandidate(final P process, final List<TagKey> conditions) {
        return meets(process, conditions) && processHasRoom.test(process);
    }

    /** Whether some value of the key is not used yet. */
    boolean hasUnusedValue(final TagKey key) {
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

    /** Whether the value is used: a holder of the task carries it, or it is missing. */
    private boolean isUsed(final int keyIndex, final int valueIndex) {
        return valueIndex == MISSING_VALUE_INDEX || usedValues[keyIndex].get(valueIndex);
    }

    /** The least-loaded process with room meeting {@code conditions}, or null if none. */
    QueuedProcess<P> leastLoadedProcessMeeting(final List<TagKey> conditions) {
        return leastLoadedProcessBelowMeeting(root, keyBits(conditions), null);
    }

    /** The keys as a BitSet with bit k set for the key with index k. */
    private static BitSet keyBits(final List<TagKey> keys) {
        final BitSet keyBits = new BitSet();
        for (final TagKey key : keys) {
            keyBits.set(key.index);
        }
        return keyBits;
    }

    /**
     * The least-loaded process with room below the node meeting {@code conditionBits} if lighter than {@code bound},
     * the least-loaded process meeting them found so far or null, else bound.
     */
    private QueuedProcess<P> leastLoadedProcessBelowMeeting(
        final Node<P> node,
        final BitSet conditionBits,
        final QueuedProcess<P> bound
    ) {
        // Every process of a leaf meets the conditions: the own values on its path were checked on the way down. The
        // leaf was walked into for being lighter than bound, so its least-loaded process is never heavier than bound.
        if (node.isLeaf) {
            return node.leastLoaded;
        }
        // Walk into the children in order until the next one cannot beat the best found.
        QueuedProcess<P> leastLoaded = bound;
        for (final Node<P> child : node.childrenByLeastLoaded) {
            if (leastLoaded != null && QueuedProcess.ORDER.compare(child.leastLoaded, leastLoaded) >= 0) {
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

    /**
     * Queues the process with its current load while it has room and repairs its path; returns the entry it is
     * queued with, a new one if its load or room changed.
     */
    private QueuedProcess<P> requeue(final QueuedProcess<P> queued) {
        final double load = processLoad.applyAsDouble(queued.process);
        final boolean hasRoom = processHasRoom.test(queued.process);
        final TreeSet<QueuedProcess<P>> processesByLoad = queued.leaf.processesByLoad;
        if (Double.compare(load, queued.load) == 0 && hasRoom == processesByLoad.contains(queued)) {
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

    /** The conditions of a pick and the least-loaded candidate under them. */
    record Pick<P>(List<TagKey> conditions, P leastLoaded) { }

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
        private final int processIndex;
        private final Node<P> leaf;
        private final double load;

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
        // The key index and value index of the node's own value, ROOT_PLACEHOLDER_INDEX for the root.
        private final int keyIndex;
        private final int valueIndex;
        private final boolean isLeaf;
        // Inner nodes only: the children with room below, by their least-loaded process.
        private final TreeSet<Node<P>> childrenByLeastLoaded;
        // Leaves only: the processes with room, by load.
        private final TreeSet<QueuedProcess<P>> processesByLoad;
        // Leaves only: the value index of the processes for each key.
        private final int[] valueIndexes;
        // The least-loaded process with room below this node, or null if none.
        private QueuedProcess<P> leastLoaded;

        private Node(final Node<P> parent, final int keyIndex, final int valueIndex, final boolean isLeaf, final int[] valueIndexes) {
            this.parent = parent;
            this.keyIndex = keyIndex;
            this.valueIndex = valueIndex;
            this.isLeaf = isLeaf;
            this.childrenByLeastLoaded = isLeaf ? null : new TreeSet<>(BY_LEAST_LOADED);
            this.processesByLoad = isLeaf ? new TreeSet<>(QueuedProcess.ORDER) : null;
            this.valueIndexes = isLeaf ? valueIndexes : null;
        }

        /** The least-loaded process with room below this node, from the head of its queue. */
        private QueuedProcess<P> leastLoadedInQueue() {
            if (isLeaf) {
                return processesByLoad.isEmpty() ? null : processesByLoad.first();
            }
            return childrenByLeastLoaded.isEmpty() ? null : childrenByLeastLoaded.first().leastLoaded;
        }
    }
}
