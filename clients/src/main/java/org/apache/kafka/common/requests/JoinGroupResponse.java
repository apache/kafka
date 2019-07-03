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

import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class JoinGroupResponse extends AbstractResponse {

    private final JoinGroupResponseData data;

    public static final String UNKNOWN_PROTOCOL = "";
    public static final int UNKNOWN_GENERATION_ID = -1;
    public static final String UNKNOWN_MEMBER_ID = "";

    public JoinGroupResponse(JoinGroupResponseData data) {
        this.data = data;
    }

    public JoinGroupResponse(Struct struct) {
        short latestVersion = (short) (JoinGroupResponseData.SCHEMAS.length - 1);
        this.data = new JoinGroupResponseData(struct, latestVersion);
    }

    public JoinGroupResponse(Struct struct, short version) {
        this.data = new JoinGroupResponseData(struct, version);
    }

    public JoinGroupResponseData data() {
        return data;
    }

    public boolean isLeader() {
        return data.memberId().equals(data.leader());
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
    }

    public static JoinGroupResponse parse(ByteBuffer buffer, short versionId) {
        return new JoinGroupResponse(ApiKeys.JOIN_GROUP.parseResponse(versionId, buffer), versionId);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }
}
