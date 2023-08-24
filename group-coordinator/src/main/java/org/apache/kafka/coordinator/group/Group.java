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

/**
 * Interface common for all groups.
 */
public interface Group {
    enum GroupType {
        CONSUMER("consumer"),
        GENERIC("generic");

        private final String name;

        GroupType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
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
     */
    void validateOffsetCommit(
        String memberId,
        String groupInstanceId,
        int generationIdOrMemberEpoch
    ) throws KafkaException;
}
