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
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

/**
 * Picks the conditions for the next standby of one task: the most tag keys that a process with room can meet,
 * preferring the keys earlier in {@code rack.aware.assignment.tags}. A process meets a tag key when it has a value
 * for it that no holder of the task has yet. It can then tell whether a process meets the conditions and return the
 * least-loaded process with room among those that do. It assumes that loads only grow and room only shrinks.
 * <p>
 * The processes sit in a tree with one level per tag key, those with the same values sharing a leaf. Scanning every
 * leaf per standby is too slow once there are many, e.g. one per process for a host tag. A query skips a subtree
 * whose own value is used and takes a subtree with no used value without looking inside.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    // No conditions, for the least-loaded process below a node. Never modified.
    private static final BitSet NO_CONDITIONS = new BitSet();

    private final ToDoubleFunction<P> processLoad;
    private final Predicate<P> processHasRoom;
    // The keys in the order of rack.aware.assignment.tags.
    private final List<TagKey> keys;
    // Per key index, the number of values.
    private final int[] numValues;
    // Per level from the top, the index of its key. The priority of a key is still its order in
    // rack.aware.assignment.tags, not this one.
    private final int[] tagKeysInTreeOrder;
    private final Map<P, Node<P>> leafByProcess;
    private final Node<P> root;

    // Per key index, one bit per value index, set once a holder of the task carries that value.
    // Cleared by startTask.
    private final BitSet[] usedValues;
    // The nodes with a cached least-loaded process, to clear on clearCache: loads do not change during a pick.
    private final List<Node<P>> cachedNodes = new ArrayList<>();
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

        root = new Node<>(-1, -1, treeHeight, false);
        leafByProcess = Utils.newHashMap(processes.size());
        // The children of each inner node by their value index, only needed to build the tree.
        final Map<Node<P>, Map<Integer, Node<P>>> childrenByValueIndex = new HashMap<>();
        int processIndex = 0;
        for (final P process : processes) {
            insert(
                new QueuedProcess<>(process, processIndex++, load.applyAsDouble(process)),
                valueIndexes(clientTags.apply(process), tagKeys, indexOfValues),
                childrenByValueIndex
            );
        }
        buildInnerNodes(root, childrenByValueIndex);
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
        final QueuedProcess<P> process,
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
                child = new Node<>(keyIndex, valueIndex, treeHeight, level == treeHeight - 1);
                children.put(valueIndex, child);
            }
            node = child;
        }
        node.valueIndexes = valueIndexes;
        node.processesByLoad.add(process);
        leafByProcess.put(process.process, node);
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

    /** Fills in the values below and the child queues of the node and of every inner node under it. */
    private void buildInnerNodes(final Node<P> node, final Map<Node<P>, Map<Integer, Node<P>>> childrenByValueIndex) {
        if (node.isLeaf) {
            return;
        }
        for (final Node<P> child : childrenByValueIndex.get(node).values()) {
            buildInnerNodes(child, childrenByValueIndex);
            // The child's own value, then the values below it; a leaf has none below.
            node.valuesBelowOf(child.keyIndex).set(child.valueIndex);
            if (!child.isLeaf) {
                for (int keyIndex = 0; keyIndex < child.valuesBelow.length; keyIndex++) {
                    if (child.valuesBelow[keyIndex] != null) {
                        node.valuesBelowOf(keyIndex).or(child.valuesBelow[keyIndex]);
                    }
                }
            }
            // A child without room below never gets room again, so it is not queued.
            final QueuedProcess<P> childLeastLoaded = leastLoadedProcessBelow(child);
            if (childLeastLoaded != null) {
                child.queuedLoad = childLeastLoaded.load;
                child.queuedProcessIndex = childLeastLoaded.processIndex;
                node.childrenByLeastLoaded.add(child);
            }
        }
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
        final int[] valueIndexes = leafByProcess.get(holder).valueIndexes;
        for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
            usedValues[keyIndex].set(valueIndexes[keyIndex]);
        }
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
        clearCache();
        // No process has room for the standby.
        if (leastLoadedProcessBelow(root) == null) {
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
        final int[] valueIndexes = leafByProcess.get(process).valueIndexes;
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

    /** Clears the cache, since loads and room may have changed since the last pick. */
    void clearCache() {
        for (final Node<P> node : cachedNodes) {
            node.isCached = false;
        }
        cachedNodes.clear();
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
            return leastLoadedProcessBelow(node);
        }
        // Walk into the children in queue order until one has a process meeting the conditions.
        QueuedProcess<P> found = null;
        while (found == null && nextChildMeeting(node, conditionBits, null) != null) {
            found = anyProcessBelowMeeting(setAsideHead(node), conditionBits);
        }
        putBackSetAsideChildren(node);
        return found;
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
            return leastLoadedProcessBelow(node);
        }
        // Walk into the children in queue order until the next one cannot beat the best found.
        QueuedProcess<P> leastLoaded = bound;
        while (nextChildMeeting(node, conditionBits, leastLoaded) != null) {
            leastLoaded = leastLoadedProcessBelowMeeting(setAsideHead(node), conditionBits, leastLoaded);
        }
        putBackSetAsideChildren(node);
        return leastLoaded;
    }

    /** Takes the child at the head of the node's queue out of it until {@link #putBackSetAsideChildren(Node)}. */
    private Node<P> setAsideHead(final Node<P> node) {
        final Node<P> head = node.childrenByLeastLoaded.poll();
        node.setAsideChildren.add(head);
        return head;
    }

    /** Puts the children that a query set aside back in the node's queue. */
    private void putBackSetAsideChildren(final Node<P> node) {
        node.childrenByLeastLoaded.addAll(node.setAsideChildren);
        node.setAsideChildren.clear();
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

    /** The least-loaded process with room below the node, or null if none; computed once per pick. */
    private QueuedProcess<P> leastLoadedProcessBelow(final Node<P> node) {
        if (!node.isCached) {
            if (node.isLeaf) {
                node.cachedLeastLoaded = leastLoadedInLeaf(node);
            } else {
                // With no conditions, the next child is the one with the least-loaded process below it.
                final Node<P> head = nextChildMeeting(node, NO_CONDITIONS, null);
                node.cachedLeastLoaded = head == null ? null : leastLoadedProcessBelow(head);
            }
            node.isCached = true;
            cachedNodes.add(node);
        }
        return node.cachedLeastLoaded;
    }

    /** The least-loaded process with room of the leaf, or null if none. A stale head is queued again, one without room dropped. */
    private QueuedProcess<P> leastLoadedInLeaf(final Node<P> leaf) {
        while (!leaf.processesByLoad.isEmpty()) {
            final QueuedProcess<P> head = leaf.processesByLoad.peek();
            final double currentLoad = processLoad.applyAsDouble(head.process);
            if (head.load != currentLoad) {
                leaf.processesByLoad.poll();
                head.load = currentLoad;
                leaf.processesByLoad.add(head);
            } else if (!processHasRoom.test(head.process)) {
                leaf.processesByLoad.poll();
            } else {
                return head;
            }
        }
        return null;
    }

    /**
     * Brings the next child whose own value meets {@code conditionBits} to the head of the queue and returns it, or
     * null if none left can beat {@code bound}.
     */
    private Node<P> nextChildMeeting(final Node<P> node, final BitSet conditionBits, final QueuedProcess<P> bound) {
        while (!node.childrenByLeastLoaded.isEmpty()) {
            final Node<P> child = node.childrenByLeastLoaded.peek();
            // The process a child is queued with may be stale, but a stale one is only lighter than its current one.
            if (bound != null && QueuedProcess.compare(child.queuedLoad, child.queuedProcessIndex, bound.load, bound.processIndex) >= 0) {
                return null;
            }
            // Every process below carries its value, so none meets the conditions: set it aside for this query.
            if (conditionBits.get(child.keyIndex) && isUsed(child.keyIndex, child.valueIndex)) {
                setAsideHead(node);
                continue;
            }
            final QueuedProcess<P> childLeastLoaded = leastLoadedProcessBelow(child);
            if (childLeastLoaded == null) {
                // No room below, and room only shrinks: drop it for good.
                node.childrenByLeastLoaded.poll();
            } else if (childLeastLoaded.load != child.queuedLoad || childLeastLoaded.processIndex != child.queuedProcessIndex) {
                // Queued with a stale process: requeue it with the current one.
                node.childrenByLeastLoaded.poll();
                child.queuedLoad = childLeastLoaded.load;
                child.queuedProcessIndex = childLeastLoaded.processIndex;
                node.childrenByLeastLoaded.add(child);
            } else {
                return child;
            }
        }
        return null;
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

    /** A process with its index in {@code processes} and the load it was queued with. */
    static final class QueuedProcess<P> {
        static final Comparator<QueuedProcess<?>> ORDER = (process1, process2) ->
            compare(process1.load, process1.processIndex, process2.load, process2.processIndex);

        final P process;
        final int processIndex;
        double load;

        private QueuedProcess(final P process, final int processIndex, final double load) {
            this.process = process;
            this.processIndex = processIndex;
            this.load = load;
        }

        /** Orders by load, then by process index. */
        static int compare(final double load1, final int processIndex1, final double load2, final int processIndex2) {
            final int byLoad = Double.compare(load1, load2);
            return byLoad != 0 ? byLoad : Integer.compare(processIndex1, processIndex2);
        }
    }

    private static final class Node<P> {
        private static final Comparator<Node<?>> BY_LEAST_LOADED = (node1, node2) ->
            QueuedProcess.compare(node1.queuedLoad, node1.queuedProcessIndex, node2.queuedLoad, node2.queuedProcessIndex);

        // The key index and value index of the node's own value, -1 for the root.
        private final int keyIndex;
        private final int valueIndex;
        private final boolean isLeaf;
        // Inner nodes only: per key index, the value indexes of the nodes below this one, not its own; null for a key
        // with none.
        private final BitSet[] valuesBelow;
        // Inner nodes only: the children with room, by the least-loaded process they are queued with.
        private final PriorityQueue<Node<P>> childrenByLeastLoaded;
        // Inner nodes only: the children a query took out of the queue, put back before the query leaves this node.
        private final List<Node<P>> setAsideChildren;
        // Leaves only: the processes that may still have room, by the load each was queued with.
        private final PriorityQueue<QueuedProcess<P>> processesByLoad;
        // Leaves only: the value index of the processes for each key.
        private int[] valueIndexes;
        // The least-loaded process below this node when it was last queued in its parent; a stale one is only lighter.
        private double queuedLoad;
        private int queuedProcessIndex;
        // Set once the head of the queue is current: cachedLeastLoaded is then the least-loaded process with room below
        // this node, or null. Cleared by clearCache.
        private boolean isCached;
        private QueuedProcess<P> cachedLeastLoaded;

        private Node(final int keyIndex, final int valueIndex, final int numKeys, final boolean isLeaf) {
            this.keyIndex = keyIndex;
            this.valueIndex = valueIndex;
            this.isLeaf = isLeaf;
            this.valuesBelow = isLeaf ? null : new BitSet[numKeys];
            this.childrenByLeastLoaded = isLeaf ? null : new PriorityQueue<>(BY_LEAST_LOADED);
            this.setAsideChildren = isLeaf ? null : new ArrayList<>();
            this.processesByLoad = isLeaf ? new PriorityQueue<>(QueuedProcess.ORDER) : null;
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
