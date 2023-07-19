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

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;

public class JoinGroupRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<JoinGroupRequest> {

        private final JoinGroupRequestData data;

        public Builder(JoinGroupRequestData data) {
            super(ApiKeys.JOIN_GROUP);
            this.data = data;
        }

        @Override
        public JoinGroupRequest build(short version) {
            if (data.groupInstanceId() != null && version < 5) {
                throw new UnsupportedVersionException("The broker join group protocol version " +
                        version + " does not support usage of config group.instance.id.");
            }
            return new JoinGroupRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final JoinGroupRequestData data;

    public static final String UNKNOWN_MEMBER_ID = "";
    public static final int UNKNOWN_GENERATION_ID = -1;
    public static final String UNKNOWN_PROTOCOL_NAME = "";

    /**
     * Ported from class Topic in {@link org.apache.kafka.common.internals} to restrict the charset for
     * static member id.
     */
    public static void validateGroupInstanceId(String id) {
        Topic.validate(id, "Group instance id", message -> {
            throw new InvalidConfigurationException(message);
        });
    }

    /**
     * Ensures that the provided {@code reason} remains within a range of 255 chars.
     * @param reason This is the reason that is sent to the broker over the wire
     *               as a part of {@code JoinGroupRequest} or {@code LeaveGroupRequest} messages.
     * @return a provided reason as is or truncated reason if it exceeds the 255 chars threshold.
     */
    public static String maybeTruncateReason(final String reason) {
        if (reason.length() > 255) {
            return reason.substring(0, 255);
        } else {
            return reason;
        }
    }

    /**
     * Since JoinGroupRequest version 4, a client that sends a join group request with
     * {@link UNKNOWN_MEMBER_ID} needs to rejoin with a new member id generated
     * by the server. Once the second join group request is complete, the client is
     * added as a new member of the group.
     *
     * Prior to version 4, a client is immediately added as a new member if it sends a
     * join group request with UNKNOWN_MEMBER_ID.
     *
     * @param apiVersion The JoinGroupRequest api version.
     *
     * @return whether a known member id is required or not.
     */
    public static boolean requiresKnownMemberId(short apiVersion) {
        return apiVersion >= 4;
    }

    /**
     * Starting from version 9 of the JoinGroup API, static members are able to
     * skip running the assignor based on the `SkipAssignment` field. We leverage
     * this to tell the leader that it is the leader of the group but by skipping
     * running the assignor while the group is in stable state.
     * Notes:
     * 1) This allows the leader to continue monitoring metadata changes for the
     * group. Note that any metadata changes happening while the static leader is
     * down won't be noticed.
     * 2) The assignors are not idempotent nor free from side effects. This is why
     * we skip entirely the assignment step as it could generate a different group
     * assignment which would be ignored by the group coordinator because the group
     * is the stable state.
     *
     * Prior to version 9 of the JoinGroup API, we wanted to avoid current leader
     * performing trivial assignment while the group is in stable stage, because
     * the new assignment in leader's next sync call won't be broadcast by a stable group.
     * This could be guaranteed by always returning the old leader id so that the current
     * leader won't assume itself as a leader based on the returned message, since the new
     * member.id won't match returned leader id, therefore no assignment will be performed.
     *
     * @param apiVersion The JoinGroupRequest api version.
     *
     * @return whether the version supports skipping assignment.
     */

    public static boolean supportsSkippingAssignment(short apiVersion) {
        return apiVersion >= 9;
    }

    /**
     * Get the client's join reason.
     *
     * @param request The JoinGroupRequest.
     *
     * @return The join reason.
     */
    public static String joinReason(JoinGroupRequestData request) {
        String joinReason = request.reason();
        if (joinReason == null || joinReason.isEmpty()) {
            joinReason = "not provided";
        }
        return joinReason;
    }

    public JoinGroupRequest(JoinGroupRequestData data, short version) {
        super(ApiKeys.JOIN_GROUP, version);
        this.data = data;
        maybeOverrideRebalanceTimeout(version);
    }

    private void maybeOverrideRebalanceTimeout(short version) {
        if (version == 0) {
            // Version 0 has no rebalance timeout, so we use the session timeout
            // to be consistent with the original behavior of the API.
            data.setRebalanceTimeoutMs(data.sessionTimeoutMs());
        }
    }

    @Override
    public JoinGroupRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        JoinGroupResponseData data = new JoinGroupResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(Errors.forException(e).code())
            .setGenerationId(UNKNOWN_GENERATION_ID)
            .setProtocolName(UNKNOWN_PROTOCOL_NAME)
            .setLeader(UNKNOWN_MEMBER_ID)
            .setMemberId(UNKNOWN_MEMBER_ID)
            .setMembers(Collections.emptyList());

        if (version() >= 7)
            data.setProtocolName(null);
        else
            data.setProtocolName(UNKNOWN_PROTOCOL_NAME);

        return new JoinGroupResponse(data, version());
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, short version) {
        return new JoinGroupRequest(new JoinGroupRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
