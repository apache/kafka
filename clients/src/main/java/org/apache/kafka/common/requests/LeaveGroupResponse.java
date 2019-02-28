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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class LeaveGroupResponse extends AbstractResponse {

    private final LeaveGroupResponseData data;

    public LeaveGroupResponse(LeaveGroupResponseData data) {
        this.data = data;
    }

    public LeaveGroupResponse(Struct struct) {
        short latestVersion = (short) (LeaveGroupResponseData.SCHEMAS.length - 1);
        this.data = new LeaveGroupResponseData(struct, latestVersion);
    }
    public LeaveGroupResponse(Struct struct, short version) {
        this.data = new LeaveGroupResponseData(struct, version);
    }

    public LeaveGroupResponseData data() {
        return data;
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
}
