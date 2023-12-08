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

import java.util.Optional;

/**
 * Listener for getting notified of member ID and epoch changes.
 */
public interface MemberStateListener {

    /**
     * Called whenever member ID or epoch change with new values received from the broker or
     * cleared if the member is not part of the group anymore (when it gets fenced, leaves the
     * group or fails).
     *
     * @param memberEpoch New member epoch received from the broker. Empty if the member is
     *                    not part of the group anymore.
     * @param memberId    Current member ID. Empty if the member is not part of the group.
     */
    void onMemberEpochUpdated(Optional<Integer> memberEpoch, Optional<String> memberId);
}
