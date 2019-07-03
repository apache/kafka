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

import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

public class SyncGroupResponse extends AbstractResponse {

    public final SyncGroupResponseData data;

    public SyncGroupResponse(SyncGroupResponseData data) {
        this.data = data;
    }

    public SyncGroupResponse(Struct struct) {
        short latestVersion = (short) (SyncGroupResponseData.SCHEMAS.length - 1);
        this.data = new SyncGroupResponseData(struct, latestVersion);
    }

    public SyncGroupResponse(Struct struct, short version) {
        this.data = new SyncGroupResponseData(struct, version);
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
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    public static SyncGroupResponse parse(ByteBuffer buffer, short version) {
        return new SyncGroupResponse(ApiKeys.SYNC_GROUP.parseResponse(version, buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
