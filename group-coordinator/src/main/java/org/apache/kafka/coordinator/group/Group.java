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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.message.ListGroupsResponseData;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Interface common for all groups.
 */
public interface Group {
    enum GroupType {
        CONSUMER("consumer"),
        CLASSIC("classic"),
        UNKNOWN("unknown");

        private final String name;

        GroupType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        private final static Map<String, GroupType> NAME_TO_ENUM = Arrays.stream(values())
            .collect(Collectors.toMap(type -> type.name.toLowerCase(Locale.ROOT), Function.identity()));

        /**
         * Parse a string into the corresponding {@code GroupType} enum value, in a case-insensitive manner.
         *
         * @return The {{@link GroupType}} according to the string passed. Unknown group type is returned if
         * the string doesn't correspond to a valid group type.
         */
        public static GroupType parse(String name) {
            if (name == null) {
                return UNKNOWN;
            }
            GroupType type = NAME_TO_ENUM.get(name.toLowerCase(Locale.ROOT));

            return type == null ? UNKNOWN : type;
        }
    }

    /**
     * @return The {{@link GroupType}}.
     */
    GroupType type();

    /**
     * @return The {{@link GroupType}}'s String representation.
     */
    String stateAsString();

    /**
     * @return The {{@link GroupType}}'s String representation based on the committed offset.
     */
    String stateAsString(long committedOffset);

    /**
     * @return the group formatted as a list group response based on the committed offset.
     */
    ListGroupsResponseData.ListedGroup asListedGroup(long committedOffset);

    /**
     * @return The group id.
     */
    String groupId();

    /**
     * Validates the OffsetCommit request.
     *
     * @param memberId                  The member id.
     * @param groupInstanceId           The group instance id.
     * @param generationIdOrMemberEpoch The generation id for genetic groups or the member epoch
     *                                  for consumer groups.
     * @param isTransactional           Whether the offset commit is transactional or not.
     */
    void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int generationIdOrMemberEpoch,
        boolean isTransactional
    ) throws KafkaException;

    /**
     * Validates the OffsetFetch request.
     *
     * @param memberId              The member id for consumer groups.
     * @param memberEpoch           The member epoch for consumer groups.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     */
    void validateOffsetFetch(
        String memberId,
        int memberEpoch,
        long lastCommittedOffset
    ) throws KafkaException;

    /**
     * Validates the OffsetDelete request.
     */
    void validateOffsetDelete() throws KafkaException;

    /**
     * Validates the DeleteGroups request.
     */
    void validateDeleteGroup() throws KafkaException;

    /**
     * Returns true if the group is actively subscribed to the topic.
     *
     * @param topic  The topic name.
     *
     * @return Whether the group is subscribed to the topic.
     */
    boolean isSubscribedToTopic(String topic);

    /**
     * Populates the list of records with tombstone(s) for deleting the group.
     *
     * @param records The list of records.
     */
    void createGroupTombstoneRecords(List<Record> records);

    /**
     * @return Whether the group is in Empty state.
     */
    boolean isEmpty();

    /**
     * See {@link OffsetExpirationCondition}
     *
     * @return The offset expiration condition for the group or Empty if no such condition exists.
     */
    Optional<OffsetExpirationCondition> offsetExpirationCondition();

    /**
     * Returns true if the statesFilter contains the current state with given committedOffset.
     *
     * @param statesFilter The states to filter, which must be lowercase.
     * @return true if the state includes, false otherwise.
     */
    boolean isInStates(Set<String> statesFilter, long committedOffset);
}
