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

import org.apache.kafka.coordinator.group.streams.assignor.IdenticalTagGroups.QueuedProcess;
import org.apache.kafka.coordinator.group.streams.assignor.IdenticalTagGroups.TagGroup;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * The tag groups of a streams group, each a group of the processes with the same values for
 * {@code rack.aware.assignment.tags}, as the leaves of a tree. A key is a tag of {@code rack.aware.assignment.tags},
 * such as {@code cluster}, and its values are the values it takes on the processes, such as {@code c1}. The tree has
 * one level per key, ordered by the number of values of the key, fewest on top.
 * <p>
 * The tree records the values that the holders of the task being placed carry, and a missing value always counts as
 * used. Every inner node queues its children by the least-loaded process with room below them and knows the values
 * below it, so a query for the tag groups whose values for some keys are all unused takes the children in queue order:
 * it skips a child whose own value is used, takes the least-loaded process of one with no used value below, walks into
 * the others, and stops once the next child cannot beat the best found, or at the first found when any such tag group
 * will do. Loads must only grow and room only shrink.
 * Each assignor sets what room is when it builds the {@link IdenticalTagGroups}; without a limit, every process has
 * room.
 * <p>
 * Inside the tree, the index of a key is its position in {@code rack.aware.assignment.tags}, the index of a value
 * comes from a map per key, and the index after the last value stands for a missing value. A set of keys is an int
 * whose bit k stands for the key with index k. Callers only see a key as a {@link TagKey}.
 *
 * @param <P> The assignor's process type.
 */
final class TagTree<P> {

    // The keys in the order of rack.aware.assignment.tags.
    private final List<TagKey> keys;
    // Per key index, the number of values.
    private final int[] numValues;
    private final Map<TagGroup<P>, Node<P>> leafByTagGroup;
    private final Node<P> root;

    // Per key index, the value indexes the holders of the task carry, cleared by clearUsedValues.
    private final BitSet[] usedValues;
    // The nodes with a cached least-loaded process, to clear on startPick: loads do not change during a pick.
    private final List<Node<P>> cachedNodes = new ArrayList<>();

    /**
     * @param tagKeys   The keys of {@code rack.aware.assignment.tags}.
     * @param tagGroups All tag groups of the streams group.
     */
    TagTree(final List<String> tagKeys, final Collection<TagGroup<P>> tagGroups) {
        // One level per tag key, so this is also the number of tag keys.
        final int treeHeight = tagKeys.size();
        // The value indexes of each tag group, in the order the values first appear; -1 for a missing value for now.
        final List<Map<String, Integer>> valueIndexOf = new ArrayList<>(treeHeight);
        for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
            valueIndexOf.add(new HashMap<>());
        }
        final List<int[]> valueIndexesOfTagGroups = new ArrayList<>(tagGroups.size());
        for (final TagGroup<P> tagGroup : tagGroups) {
            final int[] valueIndexes = new int[treeHeight];
            for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
                final String value = tagGroup.clientTags().get(tagKeys.get(keyIndex));
                final Map<String, Integer> indexOfValue = valueIndexOf.get(keyIndex);
                valueIndexes[keyIndex] = value == null ? -1 : indexOfValue.computeIfAbsent(value, v -> indexOfValue.size());
            }
            valueIndexesOfTagGroups.add(valueIndexes);
        }

        // The number of values of each key.
        numValues = new int[treeHeight];
        for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
            numValues[keyIndex] = valueIndexOf.get(keyIndex).size();
        }
        // The keys, with no value used yet.
        keys = new ArrayList<>(treeHeight);
        usedValues = new BitSet[treeHeight];
        for (int keyIndex = 0; keyIndex < treeHeight; keyIndex++) {
            keys.add(new TagKey(tagKeys.get(keyIndex), keyIndex));
            usedValues[keyIndex] = new BitSet(numValues[keyIndex] + 1);
        }
        // The tag keys from the top level down. Their priority is their order in rack.aware.assignment.tags, not this one.
        final int[] tagKeysInTreeOrder = tagKeysInTreeOrder(numValues);

        root = new Node<>(-1, -1, treeHeight, false);
        leafByTagGroup = new HashMap<>();
        int index = 0;
        for (final TagGroup<P> tagGroup : tagGroups) {
            final int[] valueIndexes = valueIndexesOfTagGroups.get(index++);
            Node<P> node = root;
            for (int level = 0; level < treeHeight; level++) {
                final int keyIndex = tagKeysInTreeOrder[level];
                if (valueIndexes[keyIndex] < 0) {
                    valueIndexes[keyIndex] = missingValueIndex(keyIndex);
                }
                final int valueIndex = valueIndexes[keyIndex];
                Node<P> child = node.childrenByValueIndex.get(valueIndex);
                if (child == null) {
                    child = new Node<>(keyIndex, valueIndex, treeHeight, level == treeHeight - 1);
                    node.childrenByValueIndex.put(valueIndex, child);
                }
                node = child;
            }
            node.tagGroup = tagGroup;
            node.valueIndexes = valueIndexes;
            leafByTagGroup.put(tagGroup, node);
        }
        buildQueues(root);
        collectValuesBelow(root);
    }

    /**
     * The tag keys by their number of values, fewest first, as the levels from the top: a used value near the top then
     * rules out a large subtree at once, and a query that leaves out the top key still walks into few branches. The
     * order only decides which nodes a query walks, not its result.
     */
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

    /** Queues the children of the node and of every inner node below it by their least-loaded process with room. */
    private void buildQueues(final Node<P> node) {
        if (node.tagGroup != null) {
            return;
        }
        for (final Node<P> child : node.childrenByValueIndex.values()) {
            buildQueues(child);
            final QueuedProcess<P> childLeastLoaded = leastLoadedBelow(child);
            if (childLeastLoaded != null) {
                child.queuedLoad = childLeastLoaded.load;
                child.queuedProcessIndex = childLeastLoaded.processIndex;
                node.childrenByLeastLoaded.add(child);
            }
        }
    }

    /** Collects the values below the node and below every inner node under it. */
    private void collectValuesBelow(final Node<P> node) {
        if (node.tagGroup != null) {
            return;
        }
        for (final Node<P> child : node.childrenByValueIndex.values()) {
            collectValuesBelow(child);
            node.valuesBelowOf(child.keyIndex).set(child.valueIndex);
            for (int keyIndex = 0; keyIndex < child.valuesBelow.length; keyIndex++) {
                if (child.valuesBelow[keyIndex] != null) {
                    node.valuesBelowOf(keyIndex).or(child.valuesBelow[keyIndex]);
                }
            }
        }
    }

    /** The keys of {@code rack.aware.assignment.tags}, in their order. */
    List<TagKey> keys() {
        return keys;
    }

    /** Forgets the values of the holders of the previous task; a missing value stays used. */
    void clearUsedValues() {
        for (final BitSet used : usedValues) {
            used.clear();
        }
    }

    /** Records the values of a holder of the task as used. */
    void markUsed(final TagGroup<P> holder) {
        final int[] valueIndexes = leafByTagGroup.get(holder).valueIndexes;
        for (int keyIndex = 0; keyIndex < keys.size(); keyIndex++) {
            usedValues[keyIndex].set(valueIndexes[keyIndex]);
        }
    }

    /** Whether some value of the key is not used yet. */
    boolean hasUnusedValue(final TagKey key) {
        // The first unused value index is a value, not the missing one after the last value.
        return usedValues[key.index].nextClearBit(0) < numValues[key.index];
    }

    /** Whether the values of the tag group for {@code conditionKeys} are all unused. */
    boolean meetsConditions(final TagGroup<P> tagGroup, final List<TagKey> conditionKeys) {
        final int[] valueIndexes = leafByTagGroup.get(tagGroup).valueIndexes;
        for (final TagKey key : conditionKeys) {
            if (valueUsed(key.index, valueIndexes[key.index])) {
                return false;
            }
        }
        return true;
    }

    /** The value index of a missing value, after the last value. */
    private int missingValueIndex(final int keyIndex) {
        return numValues[keyIndex];
    }

    /** Whether the value is used: a holder of the task carries it, or it is missing, which never makes a task more diverse. */
    private boolean valueUsed(final int keyIndex, final int valueIndex) {
        return valueIndex == missingValueIndex(keyIndex) || usedValues[keyIndex].get(valueIndex);
    }

    /** Starts a pick: loads and room may have changed since the last one, but do not change during it. */
    void startPick() {
        for (final Node<P> node : cachedNodes) {
            node.cached = false;
        }
        cachedNodes.clear();
    }

    /** Whether a process of some tag group has room; called after startPick. */
    boolean hasRoom() {
        return leastLoadedBelow(root) != null;
    }

    /** Whether a tag group with room has values for {@code conditionKeys} that are all unused. */
    boolean hasTagGroupMeeting(final List<TagKey> conditionKeys) {
        return hasTagGroupMeeting(root, keyBits(conditionKeys));
    }

    /**
     * The least-loaded process with room in a tag group whose values for {@code conditionKeys} are all unused, or null
     * if none.
     */
    QueuedProcess<P> leastLoadedMeeting(final List<TagKey> conditionKeys) {
        return leastLoadedMeeting(root, keyBits(conditionKeys));
    }

    private static int keyBits(final List<TagKey> keys) {
        int keyBits = 0;
        for (final TagKey key : keys) {
            keyBits |= 1 << key.index;
        }
        return keyBits;
    }

    /**
     * Whether a tag group with room below the node has values for {@code conditionKeys} that are all unused; the node's
     * own value and those above it are unused.
     */
    private boolean hasTagGroupMeeting(final Node<P> node, final int conditionKeys) {
        if (!hasUsedValueBelow(node, conditionKeys)) {
            return leastLoadedBelow(node) != null;
        }
        // The children in queue order, until one has such a tag group.
        boolean found = false;
        while (!found && leastLoadedOfChildren(node, conditionKeys, null) != null) {
            final Node<P> child = node.childrenByLeastLoaded.poll();
            node.setAsideChildren.add(child);
            found = hasTagGroupMeeting(child, conditionKeys);
        }
        putBackSetAsideChildren(node);
        return found;
    }

    /**
     * The least-loaded process with room below the node in a tag group whose values for {@code conditionKeys} are all
     * unused; the node's own value and those above it are unused.
     */
    private QueuedProcess<P> leastLoadedMeeting(final Node<P> node, final int conditionKeys) {
        if (!hasUsedValueBelow(node, conditionKeys)) {
            return leastLoadedBelow(node);
        }
        // The children in queue order, until the next one cannot beat the best found.
        QueuedProcess<P> leastLoaded = null;
        while (leastLoadedOfChildren(node, conditionKeys, leastLoaded) != null) {
            final Node<P> child = node.childrenByLeastLoaded.poll();
            node.setAsideChildren.add(child);
            leastLoaded = lessLoaded(leastLoaded, leastLoadedMeeting(child, conditionKeys));
        }
        putBackSetAsideChildren(node);
        return leastLoaded;
    }

    /**
     * Puts the children that the query set aside back in the node's queue, with the processes they were queued with, so
     * the node's cache stays valid.
     */
    private void putBackSetAsideChildren(final Node<P> node) {
        node.childrenByLeastLoaded.addAll(node.setAsideChildren);
        node.setAsideChildren.clear();
    }

    /** Whether the node's own value is used for one of {@code conditionKeys}. */
    private boolean ownValueUsed(final Node<P> node, final int conditionKeys) {
        return (conditionKeys & 1 << node.keyIndex) != 0 && valueUsed(node.keyIndex, node.valueIndex);
    }

    /** Whether a node below the node has a used value for one of {@code conditionKeys}, a missing one included. */
    private boolean hasUsedValueBelow(final Node<P> node, final int conditionKeys) {
        // One key of conditionKeys at a time, lowest bit first.
        for (int keyBits = conditionKeys; keyBits != 0; keyBits &= keyBits - 1) {
            final int keyIndex = Integer.numberOfTrailingZeros(keyBits);
            final BitSet valuesBelow = node.valuesBelow[keyIndex];
            if (valuesBelow == null) {
                continue;
            }
            if (valuesBelow.get(missingValueIndex(keyIndex)) || valuesBelow.intersects(usedValues[keyIndex])) {
                return true;
            }
        }
        return false;
    }

    /** The least-loaded process with room below the node, or null if none; computed once per pick. */
    private QueuedProcess<P> leastLoadedBelow(final Node<P> node) {
        if (!node.cached) {
            node.cachedLeastLoaded = node.tagGroup != null
                ? node.tagGroup.leastLoadedWithRoom()
                : leastLoadedOfChildren(node, 0, null);
            node.cached = true;
            cachedNodes.add(node);
        }
        return node.cachedLeastLoaded;
    }

    /**
     * The least-loaded process with room below the child at the head of the node's queue, or null if no child has one,
     * or once the head is queued with a process no lighter than {@code bound}: a child is never queued with a process
     * heavier than its current one, so no child can then beat {@code bound}. A child whose own value is used for
     * {@code conditionKeys} is set aside first, without looking below it; a stale child is queued again, one without
     * room dropped.
     */
    private QueuedProcess<P> leastLoadedOfChildren(final Node<P> node, final int conditionKeys, final QueuedProcess<P> bound) {
        while (!node.childrenByLeastLoaded.isEmpty()) {
            final Node<P> child = node.childrenByLeastLoaded.peek();
            if (bound != null && !queuedLighter(child, bound)) {
                return null;
            }
            if (ownValueUsed(child, conditionKeys)) {
                node.setAsideChildren.add(node.childrenByLeastLoaded.poll());
                continue;
            }
            final QueuedProcess<P> childLeastLoaded = leastLoadedBelow(child);
            if (childLeastLoaded == null) {
                node.childrenByLeastLoaded.poll();
            } else if (childLeastLoaded.load != child.queuedLoad || childLeastLoaded.processIndex != child.queuedProcessIndex) {
                node.childrenByLeastLoaded.poll();
                child.queuedLoad = childLeastLoaded.load;
                child.queuedProcessIndex = childLeastLoaded.processIndex;
                node.childrenByLeastLoaded.add(child);
            } else {
                return childLeastLoaded;
            }
        }
        return null;
    }

    /** Whether the child is queued with a process lighter than {@code process}, by {@link QueuedProcess#ORDER}. */
    private static boolean queuedLighter(final Node<?> child, final QueuedProcess<?> process) {
        final int byLoad = Double.compare(child.queuedLoad, process.load);
        return byLoad != 0 ? byLoad < 0 : child.queuedProcessIndex < process.processIndex;
    }

    private static <P> QueuedProcess<P> lessLoaded(final QueuedProcess<P> process1, final QueuedProcess<P> process2) {
        if (process1 == null) {
            return process2;
        }
        return process2 == null || QueuedProcess.ORDER.compare(process1, process2) <= 0 ? process1 : process2;
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

    private static final class Node<P> {
        private static final Comparator<Node<?>> BY_LEAST_LOADED = (node1, node2) -> {
            final int byLoad = Double.compare(node1.queuedLoad, node2.queuedLoad);
            return byLoad != 0 ? byLoad : Integer.compare(node1.queuedProcessIndex, node2.queuedProcessIndex);
        };

        // The key index and value index of the node's own value, -1 for the root.
        private final int keyIndex;
        private final int valueIndex;
        // Per key index, the value indexes of the nodes below this one, null for a key with none.
        private final BitSet[] valuesBelow;
        // Inner nodes only: the children by their value index, the children with room by the least-loaded process they
        // are queued with, and the ones a query took out of that queue, put back before the query leaves this node.
        private final Map<Integer, Node<P>> childrenByValueIndex;
        private final PriorityQueue<Node<P>> childrenByLeastLoaded;
        private final List<Node<P>> setAsideChildren;
        // Leaves only.
        private TagGroup<P> tagGroup;
        private int[] valueIndexes;
        // The least-loaded process this node is queued with in its parent.
        private double queuedLoad;
        private int queuedProcessIndex;
        // Set once the head of the queue is current: cachedLeastLoaded is then the least-loaded process with room below
        // this node, or null. Cleared by startPick.
        private boolean cached;
        private QueuedProcess<P> cachedLeastLoaded;

        private Node(final int keyIndex, final int valueIndex, final int numKeys, final boolean leaf) {
            this.keyIndex = keyIndex;
            this.valueIndex = valueIndex;
            this.valuesBelow = new BitSet[numKeys];
            this.childrenByValueIndex = leaf ? null : new HashMap<>();
            this.childrenByLeastLoaded = leaf ? null : new PriorityQueue<>(BY_LEAST_LOADED);
            this.setAsideChildren = leaf ? null : new ArrayList<>();
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
