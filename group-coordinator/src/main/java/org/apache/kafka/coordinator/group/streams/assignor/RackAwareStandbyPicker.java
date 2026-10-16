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

import org.apache.kafka.coordinator.group.streams.assignor.IdenticalTagGroups.TagGroup;
import org.apache.kafka.coordinator.group.streams.assignor.TagTree.TagKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Picks the candidate tag groups for each standby of one task. A tag group meets the condition of a tag key of
 * {@code rack.aware.assignment.tags} when its value for the tag key is new to the task. Starting from the tag groups
 * with room, each tag key in priority order narrows the candidates to those that meet its condition, or is given up if
 * none would be left; the assignor's tie-break then decides among them. Once every tag key is given up, the assignor's
 * tag-blind pass places the remaining standbys.
 * <p>
 * A {@link TagTree} records the values that the holders of the task carry and finds, without testing every tag group,
 * whether a tag group with room meets a set of conditions and the least-loaded process with room of those that do.
 * <p>
 * One picker per task, on the tree of the assignment: build it with the holders of the task, then per standby call
 * {@link #pick()}, stopping once it returns no conditions; let the assignor choose among the candidates under them, by
 * {@link #isCandidate(TagGroup, List)} or {@link #leastLoaded(List)}; and call {@link #markUsed(TagGroup)} for its
 * choice.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    private final TagTree<P> tree;

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
        if (!tree.hasTagGroupMeeting(List.of())) {
            return List.of();
        }

        // Most picks find a tag group with room that meets the condition of every such tag key.
        List<TagKey> conditions = tagKeysWithUnusedValue;
        if (!tree.hasTagGroupMeeting(conditions)) {
            // Otherwise the tag keys are added in priority order; a tag key is given up when no tag group with room
            // meets it together with the tag keys kept so far.
            conditions = new ArrayList<>();
            for (final TagKey tagKey : tagKeysWithUnusedValue) {
                final List<TagKey> withTagKey = new ArrayList<>(conditions);
                withTagKey.add(tagKey);
                // If withTagKey holds every tag key, the query above already found no tag group for it.
                if (withTagKey.size() < tagKeysWithUnusedValue.size() && tree.hasTagGroupMeeting(withTagKey)) {
                    conditions = withTagKey;
                }
            }
        }
        return conditions;
    }

    /** Whether a tag group is a candidate under the conditions of a pick: it meets them and has room. */
    boolean isCandidate(final TagGroup<P> tagGroup, final List<TagKey> conditions) {
        return tree.tagGroupMeeting(tagGroup, conditions) && tagGroup.hasRoom();
    }

    /** The least-loaded process with room of the candidates; call it before placing the standby. */
    P leastLoaded(final List<TagKey> conditions) {
        return tree.leastLoadedProcessMeeting(conditions).process;
    }
}
