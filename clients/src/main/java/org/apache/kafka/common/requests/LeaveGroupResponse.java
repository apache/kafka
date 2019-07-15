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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Possible error codes.
 *
 * Top level errors:
 * - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 * - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 * - {@link Errors#NOT_COORDINATOR}
 * - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 * - {@link Errors#UNKNOWN_MEMBER_ID} returned when the group is not found
 *
 * Member level errors:
 * - {@link Errors#FENCED_INSTANCE_ID}
 * - {@link Errors#UNKNOWN_MEMBER_ID}
 */
public class LeaveGroupResponse extends AbstractResponse {

    public final LeaveGroupResponseData data;

    public LeaveGroupResponse(LeaveGroupResponseData data) {
        this.data = data;
    }

    public LeaveGroupResponse(List<MemberResponse> memberResponses,
                              Errors topLevelError,
                              int throttleTimeMs,
                              short version) {
        if (version <= 2) {
            if (memberResponses.size() != 1) {
                throw new IllegalStateException("Singleton leave group request shouldn't have more than one " +
                                                    "response, while actually get " + memberResponses.size());
            }

            short errorCode = topLevelError != Errors.NONE ? topLevelError.code() :
                                  memberResponses.get(0).errorCode();

            this.data = new LeaveGroupResponseData()
                            .setErrorCode(errorCode)
                            .setThrottleTimeMs(throttleTimeMs);
        } else {
            this.data = new LeaveGroupResponseData()
                            .setErrorCode(topLevelError.code())
                            .setThrottleTimeMs(throttleTimeMs)
                            .setMembers(memberResponses);
        }
    }

    public LeaveGroupResponse(Struct struct) {
        short latestVersion = (short) (LeaveGroupResponseData.SCHEMAS.length - 1);
        this.data = new LeaveGroupResponseData(struct, latestVersion);
    }

    public LeaveGroupResponse(Struct struct, short version) {
        this.data = new LeaveGroupResponseData(struct, version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public List<MemberResponse> memberResponses() {
        if (data.members().isEmpty()) {
            return Collections.singletonList(
                new MemberResponse().setErrorCode(data.errorCode())
            );
        } else {
            return data.members();
        }
    }

    public Errors error() {
        Errors topLevelError = Errors.forCode(data.errorCode());
        if (topLevelError != Errors.NONE) {
            return topLevelError;
        } else {
            for (MemberResponse memberResponse : data.members()) {
                Errors memberError = Errors.forCode(memberResponse.errorCode());
                if (memberError != Errors.NONE) {
                    return memberError;
                }
            }
            return Errors.NONE;
        }
    }

    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> combinedErrorCounts = new HashMap<>();
        // Top level error.
        Errors topLevelError = Errors.forCode(data.errorCode());
        if (topLevelError != Errors.NONE) {
            combinedErrorCounts.put(topLevelError, 1);
        }

        // Member level error.
        Map<String, Errors> errorMap = new HashMap<>();

        for (MemberResponse memberResponse : data.members()) {
            Errors memberError = Errors.forCode(memberResponse.errorCode());
            if (memberError != Errors.NONE) {
                errorMap.put(memberResponse.memberId(), memberError);
            }
        }

        combinedErrorCounts.putAll(errorCounts(errorMap));
        return combinedErrorCounts;
    }

    @Override
    public Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static LeaveGroupResponse parse(ByteBuffer buffer, short versionId) {
        return new LeaveGroupResponse(ApiKeys.LEAVE_GROUP.parseResponse(versionId, buffer), versionId);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LeaveGroupResponse &&
                   ((LeaveGroupResponse) other).data.equals(this.data);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(data);
    }
}
