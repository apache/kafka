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
 * The tree records the values that the holders of the task being placed carry; a missing value always counts as used.
 * Every inner node knows the values below it and queues its children by the least-loaded process with room below them.
 * A query for the tag groups whose values for some keys, the conditions, are all unused takes the children in queue
 * order: it skips a child whose own value fails a condition, takes the least-loaded process of a child whose values
 * below all meet them, and walks into the others. It stops at the first such tag group when any will do, or once the
 * next child cannot beat the best found. Loads must only grow and room only shrink: a queued process may then be
 * stale, but only lighter than the current one, and a child without room can be dropped for good.
 * Each assignor sets what room is when it builds the {@link IdenticalTagGroups}; without a limit, every process has
 * room.
 * <p>
 * Inside the tree, the index of a key is its position in {@code rack.aware.assignment.tags}, the index of a value
 * comes from a map per key, and the index after the last value stands for a missing value. A set of keys, such as the
 * conditions of a query, is an int with bit k set for the key with index k. Callers only see a key as a
 * {@link TagKey}.
 *
 * @param <P> The assignor's process type.
 */
final class TagTree<P> {

    // The keys in the order of rack.aware.assignment.tags.
    private final List<TagKey> keys;
    // Per key index, the number of values.
    private final int[] numValues;
    // Per level from the top, the index of its key. The priority of a key is still its order in
    // rack.aware.assignment.tags, not this one.
    private final int[] tagKeysInTreeOrder;
    private final Map<TagGroup<P>, Node<P>> leafByTagGroup;
    private final Node<P> root;

    // Per key index, one bit per value index, set once a holder of the task carries that value.
    // Cleared by clearUsedValues.
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
        final List<Map<String, Integer>> indexOfValues = indexValues(tagKeys, tagGroups);

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
        leafByTagGroup = new HashMap<>();
        for (final TagGroup<P> tagGroup : tagGroups) {
            insert(tagGroup, valueIndexes(tagGroup, tagKeys, indexOfValues));
        }
        buildInnerNodes(root);
    }

    /** Per key, the index of each of its values, in the order the values first appear on the tag groups. */
    private static <P> List<Map<String, Integer>> indexValues(final List<String> tagKeys, final Collection<TagGroup<P>> tagGroups) {
        final List<Map<String, Integer>> indexOfValues = new ArrayList<>(tagKeys.size());
        for (final String tagKey : tagKeys) {
            final Map<String, Integer> indexOfValue = new HashMap<>();
            for (final TagGroup<P> tagGroup : tagGroups) {
                final String value = tagGroup.clientTags().get(tagKey);
                if (value != null) {
                    indexOfValue.putIfAbsent(value, indexOfValue.size());
                }
            }
            indexOfValues.add(indexOfValue);
        }
        return indexOfValues;
    }

    /** The value index of the tag group for each key, the missing value's index for a key it has no value for. */
    private int[] valueIndexes(final TagGroup<P> tagGroup, final List<String> tagKeys, final List<Map<String, Integer>> indexOfValues) {
        final int[] valueIndexes = new int[tagKeys.size()];
        for (int keyIndex = 0; keyIndex < valueIndexes.length; keyIndex++) {
            final String value = tagGroup.clientTags().get(tagKeys.get(keyIndex));
            valueIndexes[keyIndex] = indexOfValues.get(keyIndex).getOrDefault(value, missingValueIndex(keyIndex));
        }
        return valueIndexes;
    }

    /** Adds the tag group as a leaf, creating the inner nodes on its path. */
    private void insert(final TagGroup<P> tagGroup, final int[] valueIndexes) {
        final int treeHeight = tagKeysInTreeOrder.length;
        Node<P> node = root;
        for (int level = 0; level < treeHeight; level++) {
            final int keyIndex = tagKeysInTreeOrder[level];
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
    private void buildInnerNodes(final Node<P> node) {
        if (node.tagGroup != null) {
            return;
        }
        for (final Node<P> child : node.childrenByValueIndex.values()) {
            buildInnerNodes(child);
            // The child's own value, then the values below it.
            node.valuesBelowOf(child.keyIndex).set(child.valueIndex);
            for (int keyIndex = 0; keyIndex < child.valuesBelow.length; keyIndex++) {
                if (child.valuesBelow[keyIndex] != null) {
                    node.valuesBelowOf(keyIndex).or(child.valuesBelow[keyIndex]);
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
    boolean tagGroupMeeting(final TagGroup<P> tagGroup, final List<TagKey> conditionKeys) {
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

    /** Whether the value is used: a holder of the task carries it, or it is missing. */
    private boolean valueUsed(final int keyIndex, final int valueIndex) {
        return valueIndex == missingValueIndex(keyIndex) || usedValues[keyIndex].get(valueIndex);
    }

    /** Clears the cache, since loads and room may have changed since the last pick. */
    void startPick() {
        for (final Node<P> node : cachedNodes) {
            node.cached = false;
        }
        cachedNodes.clear();
    }

    /** Whether a tag group with room has values for {@code conditionKeys} that are all unused. */
    boolean hasTagGroupMeeting(final List<TagKey> conditionKeys) {
        return hasTagGroupBelowMeeting(root, keyBits(conditionKeys));
    }

    /** The least-loaded process with room in a tag group meeting {@code conditionKeys}, or null if none. */
    QueuedProcess<P> leastLoadedProcessMeeting(final List<TagKey> conditionKeys) {
        return leastLoadedProcessBelowMeeting(root, keyBits(conditionKeys));
    }

    /** The keys as an int with bit k set for the key with index k. */
    private static int keyBits(final List<TagKey> keys) {
        int keyBits = 0;
        for (final TagKey key : keys) {
            keyBits |= 1 << key.index;
        }
        return keyBits;
    }

    /** Whether a tag group with room below the node meets {@code conditionKeys}. */
    private boolean hasTagGroupBelowMeeting(final Node<P> node, final int conditionKeys) {
        if (valuesBelowAllMeeting(node, conditionKeys)) {
            return leastLoadedProcessBelow(node) != null;
        }
        // Walk into the children in queue order until one has a tag group meeting the conditions.
        boolean found = false;
        while (!found && nextChildMeeting(node, conditionKeys, null) != null) {
            found = hasTagGroupBelowMeeting(setAsideHead(node), conditionKeys);
        }
        putBackSetAsideChildren(node);
        return found;
    }

    /**
     * The least-loaded process with room below the node in a tag group meeting {@code conditionKeys}, or null if none.
     */
    private QueuedProcess<P> leastLoadedProcessBelowMeeting(final Node<P> node, final int conditionKeys) {
        if (valuesBelowAllMeeting(node, conditionKeys)) {
            return leastLoadedProcessBelow(node);
        }
        // Walk into the children in queue order until the next one cannot beat the best found.
        QueuedProcess<P> leastLoaded = null;
        while (nextChildMeeting(node, conditionKeys, leastLoaded) != null) {
            final QueuedProcess<P> found = leastLoadedProcessBelowMeeting(setAsideHead(node), conditionKeys);
            if (found != null && (leastLoaded == null || QueuedProcess.ORDER.compare(found, leastLoaded) < 0)) {
                leastLoaded = found;
            }
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

    /** Whether every value below the node meets {@code conditionKeys}. */
    private boolean valuesBelowAllMeeting(final Node<P> node, final int conditionKeys) {
        // One key of conditionKeys at a time, lowest bit first.
        for (int keyBits = conditionKeys; keyBits != 0; keyBits &= keyBits - 1) {
            final int keyIndex = Integer.numberOfTrailingZeros(keyBits);
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
        if (!node.cached) {
            if (node.tagGroup != null) {
                node.cachedLeastLoaded = node.tagGroup.leastLoadedWithRoom();
            } else {
                // With no conditions, the next child is the one with the least-loaded process below it.
                final Node<P> head = nextChildMeeting(node, 0, null);
                node.cachedLeastLoaded = head == null ? null : leastLoadedProcessBelow(head);
            }
            node.cached = true;
            cachedNodes.add(node);
        }
        return node.cachedLeastLoaded;
    }

    /**
     * Brings the next child whose own value meets {@code conditionKeys} to the head of the queue and returns it, or
     * null if none left can beat {@code bound}.
     */
    private Node<P> nextChildMeeting(final Node<P> node, final int conditionKeys, final QueuedProcess<P> bound) {
        while (!node.childrenByLeastLoaded.isEmpty()) {
            final Node<P> child = node.childrenByLeastLoaded.peek();
            // The process a child is queued with may be stale, but a stale one is only lighter than its current one.
            if (bound != null) {
                // Lighter by QueuedProcess.ORDER: by load, then by process index.
                final int byLoad = Double.compare(child.queuedLoad, bound.load);
                final boolean queuedLighter = byLoad != 0 ? byLoad < 0 : child.queuedProcessIndex < bound.processIndex;
                if (!queuedLighter) {
                    return null;
                }
            }
            // Every tag group below carries its value, so none meets the conditions: set it aside for this query.
            if ((conditionKeys & 1 << child.keyIndex) != 0 && valueUsed(child.keyIndex, child.valueIndex)) {
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

    private static final class Node<P> {
        private static final Comparator<Node<?>> BY_LEAST_LOADED = (node1, node2) -> {
            final int byLoad = Double.compare(node1.queuedLoad, node2.queuedLoad);
            return byLoad != 0 ? byLoad : Integer.compare(node1.queuedProcessIndex, node2.queuedProcessIndex);
        };

        // The key index and value index of the node's own value, -1 for the root.
        private final int keyIndex;
        private final int valueIndex;
        // Per key index, the value indexes of the nodes below this one, not its own; null for a key with none.
        private final BitSet[] valuesBelow;
        // Inner nodes only: the children by their value index.
        private final Map<Integer, Node<P>> childrenByValueIndex;
        // Inner nodes only: the children with room, by the least-loaded process they are queued with.
        private final PriorityQueue<Node<P>> childrenByLeastLoaded;
        // Inner nodes only: the children a query took out of the queue, put back before the query leaves this node.
        private final List<Node<P>> setAsideChildren;
        // Leaves only.
        private TagGroup<P> tagGroup;
        private int[] valueIndexes;
        // The least-loaded process below this node when it was last queued in its parent; a stale one is only lighter.
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
