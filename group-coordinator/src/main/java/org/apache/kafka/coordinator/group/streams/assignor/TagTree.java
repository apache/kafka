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
 * A tree built from {@code rack.aware.assignment.tags} with every tag group of {@link IdenticalTagGroups} as a leaf.
 * Given some tag keys as conditions, it tells whether a tag group with room has values for them that no holder of the
 * task has yet, and returns the least-loaded process with room among such tag groups. It assumes that loads only grow
 * and room only shrinks.
 * <p>
 * Scanning every tag group per standby is too slow once there are many tag groups, e.g. one per process for a host
 * tag. A query skips a subtree whose own value is used and takes a subtree with no used value without looking inside.
 *
 * @param <P> The assignor's process type.
 */
final class TagTree<P> {

    // No conditions, for the least-loaded process below a node. Never modified.
    private static final BitSet NO_CONDITIONS = new BitSet();

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
        for (int keyIndex = 0; keyIndex < tagKeys.size(); keyIndex++) {
            final Map<String, Integer> indexOfValue = new HashMap<>();
            for (final TagGroup<P> tagGroup : tagGroups) {
                final String value = tagGroup.tagValues().get(keyIndex);
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
            final String value = tagGroup.tagValues().get(keyIndex);
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

    /** The first process found with room in a tag group meeting {@code conditionKeys}, or null if none. */
    QueuedProcess<P> processMeeting(final List<TagKey> conditionKeys) {
        return processBelowMeeting(root, keyBits(conditionKeys));
    }

    /**
     * The least-loaded process with room in a tag group meeting {@code conditionKeys}, starting from {@code bound}, a
     * process meeting them found in this pick.
     */
    QueuedProcess<P> leastLoadedProcessMeeting(final List<TagKey> conditionKeys, final QueuedProcess<P> bound) {
        return leastLoadedProcessBelowMeeting(root, keyBits(conditionKeys), bound);
    }

    /** The keys as a BitSet with bit k set for the key with index k. */
    private static BitSet keyBits(final List<TagKey> keys) {
        final BitSet keyBits = new BitSet();
        for (final TagKey key : keys) {
            keyBits.set(key.index);
        }
        return keyBits;
    }

    /** The first process found with room below the node in a tag group meeting {@code conditionKeys}, or null if none. */
    private QueuedProcess<P> processBelowMeeting(final Node<P> node, final BitSet conditionKeys) {
        if (valuesBelowAllMeeting(node, conditionKeys)) {
            return leastLoadedProcessBelow(node);
        }
        // Walk into the children in queue order until one has a tag group meeting the conditions.
        QueuedProcess<P> found = null;
        while (found == null && nextChildMeeting(node, conditionKeys, null) != null) {
            found = processBelowMeeting(setAsideHead(node), conditionKeys);
        }
        putBackSetAsideChildren(node);
        return found;
    }

    /**
     * The least-loaded process with room below the node in a tag group meeting {@code conditionKeys} if lighter than
     * {@code bound}, the least-loaded process meeting them found so far, else bound.
     */
    private QueuedProcess<P> leastLoadedProcessBelowMeeting(
        final Node<P> node,
        final BitSet conditionKeys,
        final QueuedProcess<P> bound
    ) {
        // Every tag group below the node meets the conditions. The node is the root, with bound below it, or a child
        // walked into for being lighter than bound, so its least-loaded process is never heavier than bound.
        if (valuesBelowAllMeeting(node, conditionKeys)) {
            return leastLoadedProcessBelow(node);
        }
        // Walk into the children in queue order until the next one cannot beat the best found.
        QueuedProcess<P> leastLoaded = bound;
        while (nextChildMeeting(node, conditionKeys, leastLoaded) != null) {
            leastLoaded = leastLoadedProcessBelowMeeting(setAsideHead(node), conditionKeys, leastLoaded);
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
    private boolean valuesBelowAllMeeting(final Node<P> node, final BitSet conditionKeys) {
        // One key of conditionKeys at a time, lowest index first.
        for (int keyIndex = conditionKeys.nextSetBit(0); keyIndex >= 0; keyIndex = conditionKeys.nextSetBit(keyIndex + 1)) {
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
                final Node<P> head = nextChildMeeting(node, NO_CONDITIONS, null);
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
    private Node<P> nextChildMeeting(final Node<P> node, final BitSet conditionKeys, final QueuedProcess<P> bound) {
        while (!node.childrenByLeastLoaded.isEmpty()) {
            final Node<P> child = node.childrenByLeastLoaded.peek();
            // The process a child is queued with may be stale, but a stale one is only lighter than its current one.
            if (bound != null && QueuedProcess.compare(child.queuedLoad, child.queuedProcessIndex, bound.load, bound.processIndex) >= 0) {
                return null;
            }
            // Every tag group below carries its value, so none meets the conditions: set it aside for this query.
            if (conditionKeys.get(child.keyIndex) && valueUsed(child.keyIndex, child.valueIndex)) {
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
        private static final Comparator<Node<?>> BY_LEAST_LOADED = (node1, node2) ->
            QueuedProcess.compare(node1.queuedLoad, node1.queuedProcessIndex, node2.queuedLoad, node2.queuedProcessIndex);

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
