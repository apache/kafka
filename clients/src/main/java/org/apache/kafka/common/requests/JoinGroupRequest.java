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

    private static final int MAX_GROUP_INSTANCE_ID_LENGTH = 249;

    /**
     * Ported from class Topic in {@link org.apache.kafka.common.internals} to restrict the charset for
     * static member id.
     */
    public static void validateGroupInstanceId(String id) {
        if (id.equals(""))
            throw new InvalidConfigurationException("Group instance id must be non-empty string");
        if (id.equals(".") || id.equals(".."))
            throw new InvalidConfigurationException("Group instance id cannot be \".\" or \"..\"");
        if (id.length() > MAX_GROUP_INSTANCE_ID_LENGTH)
            throw new InvalidConfigurationException("Group instance id can't be longer than " + MAX_GROUP_INSTANCE_ID_LENGTH +
                    " characters: " + id);
        if (!containsValidPattern(id))
            throw new InvalidConfigurationException("Group instance id \"" + id + "\" is illegal, it contains a character other than " +
                    "ASCII alphanumerics, '.', '_' and '-'");
    }

    /**
     * Valid characters for Consumer group.instance.id are the ASCII alphanumerics, '.', '_', and '-'
     */
    static boolean containsValidPattern(String topic) {
        for (int i = 0; i < topic.length(); ++i) {
            char c = topic.charAt(i);

            boolean validChar = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || c == '.' ||
                    c == '_' || c == '-';
            if (!validChar)
                return false;
        }
        return true;
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

        return new JoinGroupResponse(data);
    }

    public static JoinGroupRequest parse(ByteBuffer buffer, short version) {
        return new JoinGroupRequest(new JoinGroupRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
