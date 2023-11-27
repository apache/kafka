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

/**
 * Listener to register for getting notified when the member state changes, or new member ID or
 * epoch are received.
 */
public interface MemberStateListener {

    /**
     * Called when the member transitions to a new state.
     *
     * @param state New state.
     */
    void onStateChange(MemberState state);

    /**
     * Called when the member receives a new member ID.
     *
     * @param memberId New member ID.
     * @param epoch    Latest member epoch received.
     */
    void onMemberIdUpdated(String memberId, int epoch);

    /**
     * Called when a member receives a new member epoch.
     *
     * @param epoch    New member epoch.
     * @param memberId Current member ID.
     */
    void onMemberEpochUpdated(int epoch, String memberId);
}
