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

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
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
 *
 * Member level errors:
 * - {@link Errors#FENCED_INSTANCE_ID}
 * - {@link Errors#UNKNOWN_MEMBER_ID}
 *
 * If the top level error code is set, normally this indicates that broker early stops the request
 * handling due to some severe global error, so it is expected to see the member level errors to be empty.
 * For older version response, we may populate member level error towards top level because older client
 * couldn't parse member level.
 */
public class LeaveGroupResponse extends AbstractResponse {

    private final LeaveGroupResponseData data;

    public LeaveGroupResponse(LeaveGroupResponseData data) {
        super(ApiKeys.LEAVE_GROUP);
        this.data = data;
    }

    public LeaveGroupResponse(LeaveGroupResponseData data, short version) {
        super(ApiKeys.LEAVE_GROUP);

        if (version >= 3) {
            this.data = data;
        } else {
            if (data.members().size() != 1) {
                throw new UnsupportedVersionException("LeaveGroup response version " + version +
                    " can only contain one member, got " + data.members().size() + " members.");
            }

            Errors topLevelError = Errors.forCode(data.errorCode());
            short errorCode = getError(topLevelError, data.members()).code();
            this.data = new LeaveGroupResponseData().setErrorCode(errorCode);
        }
    }

    public LeaveGroupResponse(List<MemberResponse> memberResponses,
                              Errors topLevelError,
                              final int throttleTimeMs,
                              final short version) {
        super(ApiKeys.LEAVE_GROUP);
        if (version <= 2) {
            // Populate member level error.
            final short errorCode = getError(topLevelError, memberResponses).code();

            this.data = new LeaveGroupResponseData()
                            .setErrorCode(errorCode);
        } else {
            this.data = new LeaveGroupResponseData()
                            .setErrorCode(topLevelError.code())
                            .setMembers(memberResponses);
        }

        if (version >= 1) {
            this.data.setThrottleTimeMs(throttleTimeMs);
        }
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public List<MemberResponse> memberResponses() {
        return data.members();
    }

    public Errors error() {
        return getError(Errors.forCode(data.errorCode()), data.members());
    }

    public Errors topLevelError() {
        return Errors.forCode(data.errorCode());
    }

    private static Errors getError(Errors topLevelError, List<MemberResponse> memberResponses) {
        if (topLevelError != Errors.NONE) {
            return topLevelError;
        } else {
            for (MemberResponse memberResponse : memberResponses) {
                Errors memberError = Errors.forCode(memberResponse.errorCode());
                if (memberError != Errors.NONE) {
                    return memberError;
                }
            }
            return Errors.NONE;
        }
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> combinedErrorCounts = new HashMap<>();
        // Top level error.
        updateErrorCounts(combinedErrorCounts, Errors.forCode(data.errorCode()));

        // Member level error.
        data.members().forEach(memberResponse -> {
            updateErrorCounts(combinedErrorCounts, Errors.forCode(memberResponse.errorCode()));
        });
        return combinedErrorCounts;
    }

    @Override
    public LeaveGroupResponseData data() {
        return data;
    }

    public static LeaveGroupResponse parse(ByteBuffer buffer, short version) {
        return new LeaveGroupResponse(new LeaveGroupResponseData(new ByteBufferAccessor(buffer), version));
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

    @Override
    public String toString() {
        return data.toString();
    }
}
