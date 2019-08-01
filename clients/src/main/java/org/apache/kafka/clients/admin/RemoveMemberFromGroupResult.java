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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.LeaveGroupResponse;

import java.lang.reflect.Member;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of a batch member removal operation.
 */
public class RemoveMemberFromGroupResult {

    private final Errors error;
    private final Map<MemberIdentity, KafkaFuture<Void>> memberFutures;

    private final List<MemberIdentity> membersToRemove;
    private final List<MemberIdentity> succeedMembers;
    private final List<MemberResponse> failedMembers;

    RemoveMemberFromGroupResult(Errors error,
                                List<MemberIdentity> membersToRemove,
                                List<MemberResponse> memberResponses) {
        this.error = error;
        this.membersToRemove = membersToRemove;
        this.memberFutures = new HashMap<>(membersToRemove.size());

        if (!hasError()) {
            for (MemberIdentity memberIdentity : membersToRemove) {
                memberFutures.put(memberIdentity, new KafkaFuture.Function<>())

            }
//            succeedMembers = membersToRemove;
//            failedMembers = Collections.emptyList();
        } else {
            succeedMembers = new ArrayList<>();
            failedMembers = new ArrayList<>();

            for (MemberResponse memberResponse : memberResponses) {
                if (Errors.forCode(memberResponse.errorCode()) == Errors.NONE) {
                    succeedMembers.add(new MemberIdentity()
                                           .setMemberId(memberResponse.memberId())
                                           .setGroupInstanceId(memberResponse.groupInstanceId()));
                } else {
                    failedMembers.add(memberResponse);
                }
            }
        }
    }

    public Errors error() {
        return error;
    }

    public boolean hasError() {
        return error != Errors.NONE;
    }

    /**
     * Members set for removal in the original request.
     *
     * @return list of members to be removed.
     */
    public List<MemberIdentity> membersToRemove() {
        return membersToRemove;
    }

    /**
     * Individual members who failed to leave the group with corresponding errors. If
     * the response error is a top level error for the entire request
     * such as {@link Errors#GROUP_AUTHORIZATION_FAILED}, the list will be empty.
     * See comments on {@link LeaveGroupResponse} for more details.
     *
     * @return list of members who failed to be removed
     */
    public List<MemberResponse> failedMembers() {
        return failedMembers;
    }

    /**
     * Members who are removed successfully from the group.
     *
     * @return list of members who succeed for removal
     */
    public List<MemberIdentity> succeedMembers() {
        return succeedMembers;
    }
}
