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
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.Errors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Result of a batch member removal operation.
 */
public class RemoveMemberFromGroupResult {

    private final Errors populatedError;
    private final Map<MemberIdentity, KafkaFuture<Void>> memberFutures;

    RemoveMemberFromGroupResult(Errors populatedError,
                                List<MemberIdentity> membersToRemove,
                                List<MemberResponse> memberResponses) {
        this.populatedError = populatedError;
        this.memberFutures = new HashMap<>(membersToRemove.size());

        if (!hasError()) {
            for (MemberIdentity memberIdentity : membersToRemove) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.complete(null);
                memberFutures.put(memberIdentity, future);
            }
        } else if (!isMemberLevelError(this.populatedError)) {
            // If the populated error is a top-level error, fail every member's future.
            for (MemberIdentity memberIdentity : membersToRemove) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(populatedError.exception());
                memberFutures.put(memberIdentity, future);
            }
        } else {
            for (MemberResponse memberResponse : memberResponses) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                Errors memberError = Errors.forCode(memberResponse.errorCode());
                if (memberError != Errors.NONE) {
                    future.completeExceptionally(memberError.exception());
                } else {
                    future.complete(null);
                }
                memberFutures.put(new MemberIdentity()
                                      .setMemberId(memberResponse.memberId())
                                      .setGroupInstanceId(memberResponse.groupInstanceId()), future);
            }
        }
    }

    public Errors error() {
        return populatedError;
    }

    public boolean hasError() {
        return populatedError != Errors.NONE;
    }

    private static boolean isMemberLevelError(Errors error) {
        return error == Errors.UNKNOWN_MEMBER_ID || error == Errors.FENCED_INSTANCE_ID;
    }

    /**
     * Futures of members with corresponding errors when they leave the group.
     *
     * @return list of members who failed to be removed
     */
    public Map<MemberIdentity, KafkaFuture<Void>> memberFutures() {
        return memberFutures;
    }
}
