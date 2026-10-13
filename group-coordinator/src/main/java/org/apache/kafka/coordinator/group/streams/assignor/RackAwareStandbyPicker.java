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
import org.apache.kafka.coordinator.group.streams.assignor.TagTree.TagKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Picks the conditions for the next standby of one task: the most tag keys that a tag group with room can meet,
 * preferring the keys earlier in {@code rack.aware.assignment.tags}. A tag group meets a tag key when it has a value
 * for it that no holder of the task has yet. It can then tell whether a tag group meets the conditions and return the
 * least-loaded process with room among those that do.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    private final TagTree<P> tree;
    // A process meeting the conditions the last pick returned, which leastLoaded starts from.
    private QueuedProcess<P> found;

    /**
     * @param tree    The tag tree of the assignment, whose used values the picker resets for this task.
     * @param holders The holders of the task: its active owner and the standbys placed so far.
     */
    RackAwareStandbyPicker(final TagTree<P> tree, final Collection<TagGroup<P>> holders) {
        this.tree = tree;
        tree.clearUsedValues();
        for (final TagGroup<P> holder : holders) {
            markUsed(holder);
        }
    }

    /** Records the tag values of a new holder of the task as used. */
    void markUsed(final TagGroup<P> holder) {
        tree.markUsed(holder);
    }

    /** The conditions for the next standby, or empty once no tag group with room can make the task more diverse. */
    List<TagKey> pick() {
        // Only if a tag key has an unused value is it possible to choose a process that makes the task more diverse
        // in this tag key.
        final List<TagKey> tagKeysWithUnusedValue = new ArrayList<>();
        for (final TagKey tagKey : tree.tagKeys()) {
            if (tree.hasUnusedValue(tagKey)) {
                tagKeysWithUnusedValue.add(tagKey);
            }
        }
        if (tagKeysWithUnusedValue.isEmpty()) {
            return List.of();
        }
        tree.startPick();
        // No tag group has room for the standby; with no conditions, only room counts.
        if (tree.processMeeting(List.of()) == null) {
            return List.of();
        }

        // Most picks find a tag group with room that meets the condition of every such tag key.
        List<TagKey> conditions = tagKeysWithUnusedValue;
        found = tree.processMeeting(conditions);
        if (found == null) {
            // Otherwise the tag keys are added in priority order; a tag key is given up when no tag group with room
            // meets it together with the tag keys kept so far.
            conditions = new ArrayList<>();
            for (final TagKey tagKey : tagKeysWithUnusedValue) {
                final List<TagKey> withTagKey = new ArrayList<>(conditions);
                withTagKey.add(tagKey);
                // If withTagKey holds every tag key, the query above already found no tag group for it.
                if (withTagKey.size() < tagKeysWithUnusedValue.size()) {
                    final QueuedProcess<P> foundWithTagKey = tree.processMeeting(withTagKey);
                    if (foundWithTagKey != null) {
                        conditions = withTagKey;
                        found = foundWithTagKey;
                    }
                }
            }
        }
        return conditions;
    }

    /** Whether a tag group is a candidate under the conditions of a pick: it meets them and has room. */
    boolean isCandidate(final TagGroup<P> tagGroup, final List<TagKey> conditions) {
        return tree.tagGroupMeeting(tagGroup, conditions) && tagGroup.hasRoom();
    }

    /**
     * The least-loaded process with room of the candidates; call it before placing the standby. Only valid for the
     * non-empty conditions {@link #pick()} returned: without conditions, nothing keeps out the holders of the task.
     */
    P leastLoaded(final List<TagKey> conditions) {
        if (conditions.isEmpty()) {
            throw new IllegalArgumentException("Conditions must be the non-empty result of pick()");
        }
        return tree.leastLoadedProcessMeeting(conditions, found).process;
    }
}
