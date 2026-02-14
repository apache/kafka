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
package org.apache.kafka.server.share;

/**
 * The ShareGroupListener is used to notify when there is a change in the share group members.
 */
public interface ShareGroupListener {

    /**
     * Called when member leaves the group.
     *
     * @param groupId  The id of the group.
     * @param memberId The id of the member.
     */
    void onMemberLeave(String groupId, String memberId);

    /**
     * Called when the group is empty.
     *
     * @param groupId The id of the group.
     */
    void onGroupEmpty(String groupId);
}
