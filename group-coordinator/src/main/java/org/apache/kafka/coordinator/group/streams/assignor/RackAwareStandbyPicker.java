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
 * Picks the candidate tag groups for each standby of one task. A tag group meets the condition of a key of
 * {@code rack.aware.assignment.tags} when its value for the key is new to the task. Starting from the tag groups with
 * room, each key in list order narrows the candidates to those that meet its condition, or is given up if none would
 * be left; the assignor's tie-break then decides among them. Once every key is given up, the assignor's tag-blind
 * pass places the remaining standbys.
 * <p>
 * A {@link TagTree} records the values that the holders of the task carry and finds the least-loaded tag group with
 * room that meets a set of conditions without testing every tag group.
 * <p>
 * One picker per task, on the tree of the assignment: build it with the holders of the task, then per standby
 * {@link #pick()}, the assignor's choice by {@link #isCandidate(TagGroup)} or {@link #leastLoaded()}, and
 * {@link #markUsed(TagGroup)} for it.
 *
 * @param <P> The assignor's process type.
 */
final class RackAwareStandbyPicker<P> {

    private final TagTree<P> tree;

    // The keys whose condition holds in the last pick.
    private List<TagKey> pickedConditions;

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

    /** Records the tag values of a new holder of the task, so that no later standby lands on them while a new value exists. */
    void markUsed(final TagGroup<P> holder) {
        tree.markUsed(holder);
    }

    /** Picks the candidates for the next standby, or returns false once no tag group can make the task more diverse. */
    boolean pick() {
        // A key whose values the holders all carry cannot make the task more diverse.
        final List<TagKey> keysWithNewValue = new ArrayList<>();
        for (final TagKey key : tree.keys()) {
            if (tree.hasUnusedValue(key)) {
                keysWithNewValue.add(key);
            }
        }
        if (keysWithNewValue.isEmpty()) {
            return false;
        }
        tree.startPick();
        if (!tree.hasRoom()) {
            return false;
        }

        // Most picks find a tag group with room that meets the condition of every such key.
        List<TagKey> conditions = keysWithNewValue;
        if (!tree.hasTagGroupMeeting(conditions)) {
            // Otherwise the keys are added in priority order, giving up each one that would leave no tag group.
            conditions = new ArrayList<>();
            for (final TagKey key : keysWithNewValue) {
                final List<TagKey> withKey = new ArrayList<>(conditions);
                withKey.add(key);
                // With every key, the query above already found no tag group.
                if (withKey.size() < keysWithNewValue.size() && tree.hasTagGroupMeeting(withKey)) {
                    conditions = withKey;
                }
            }
            if (conditions.isEmpty()) {
                return false;
            }
        }
        pickedConditions = conditions;
        return true;
    }

    /** Whether a tag group is one of the candidates of the last pick. */
    boolean isCandidate(final TagGroup<P> tagGroup) {
        return tree.meetsConditions(tagGroup, pickedConditions) && tagGroup.hasRoom();
    }

    /** The least-loaded process with room of the candidates of the last pick; call it before placing the standby. */
    P leastLoaded() {
        return tree.leastLoadedMeeting(pickedConditions).process;
    }
}
