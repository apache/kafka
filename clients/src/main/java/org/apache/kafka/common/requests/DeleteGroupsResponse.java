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

import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes:
 *
 * COORDINATOR_LOAD_IN_PROGRESS (14)
 * COORDINATOR_NOT_AVAILABLE(15)
 * NOT_COORDINATOR (16)
 * INVALID_GROUP_ID(24)
 * GROUP_AUTHORIZATION_FAILED(30)
 * NON_EMPTY_GROUP(68)
 * GROUP_ID_NOT_FOUND(69)
 */
public class DeleteGroupsResponse extends AbstractResponse {

    private final DeleteGroupsResponseData data;

    public DeleteGroupsResponse(DeleteGroupsResponseData data) {
        super(ApiKeys.DELETE_GROUPS);
        this.data = data;
    }

    @Override
    public DeleteGroupsResponseData data() {
        return data;
    }

    public Map<String, Errors> errors() {
        Map<String, Errors> errorMap = new HashMap<>();
        for (DeletableGroupResult result : data.results()) {
            errorMap.put(result.groupId(), Errors.forCode(result.errorCode()));
        }
        return errorMap;
    }

    public Errors get(String group) throws IllegalArgumentException {
        DeletableGroupResult result = data.results().find(group);
        if (result == null) {
            throw new IllegalArgumentException("could not find group " + group + " in the delete group response");
        }
        return Errors.forCode(result.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        data.results().forEach(result ->
            updateErrorCounts(counts, Errors.forCode(result.errorCode()))
        );
        return counts;
    }

    public static DeleteGroupsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteGroupsResponse(new DeleteGroupsResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
