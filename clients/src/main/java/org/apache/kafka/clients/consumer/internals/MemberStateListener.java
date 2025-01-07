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

package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.TopicPartition;

import java.util.Optional;
import java.util.Set;

/**
 * Listener for getting notified of membership state changes.
 */
public interface MemberStateListener {

    /**
     * Called whenever epoch changes with new values received from the broker or
     * cleared if the member is not part of the group anymore (when it gets fenced, leaves the
     * group or fails).
     *
     * @param memberEpoch New member epoch received from the broker. Empty if the member is
     *                    not part of the group anymore.
     * @param memberId    Current member ID. It won't change until the process is terminated.
     */
    void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId);

    /**
     * This callback is invoked when a group member's assigned set of partitions changes. Assignments can change via
     * group coordinator partition assignment changes, unsubscribing, and when leaving the group.
     *
     * @param partitions New assignment, can be empty, but not {@code null}
     */
    default void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {

    }
}
