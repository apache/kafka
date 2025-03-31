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

import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DescribeShareGroupOffsetsResponse extends AbstractResponse {
    private final DescribeShareGroupOffsetsResponseData data;
    private final Map<String, Throwable> groupLevelErrors = new HashMap<>();

    public DescribeShareGroupOffsetsResponse(DescribeShareGroupOffsetsResponseData data) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS);
        this.data = data;
        for (DescribeShareGroupOffsetsResponseGroup group : data.groups()) {
            if (group.errorCode() != Errors.NONE.code()) {
                this.groupLevelErrors.put(group.groupId(), Errors.forCode(group.errorCode()).exception(group.errorMessage()));
            }
        }
    }

    // Builds a response with the same group-level error for all groups and empty topics lists for all groups
    public DescribeShareGroupOffsetsResponse(int throttleTimeMs,
                                             List<String> groupIds,
                                             Throwable allGroupsException) {
        super(ApiKeys.DESCRIBE_SHARE_GROUP_OFFSETS);
        short errorCode = Errors.forException(allGroupsException).code();
        List<DescribeShareGroupOffsetsResponseGroup> groupList = new ArrayList<>();
        groupIds.forEach(groupId -> {
            groupList.add(new DescribeShareGroupOffsetsResponseGroup()
                .setGroupId(groupId)
                .setErrorCode(errorCode)
                .setErrorMessage(errorCode == Errors.UNKNOWN_SERVER_ERROR.code() ? Errors.forCode(errorCode).message() : allGroupsException.getMessage()));
            groupLevelErrors.put(groupId, allGroupsException);
        });
        this.data = new DescribeShareGroupOffsetsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setGroups(groupList);
    }

    public boolean hasGroupError(String groupId) {
        return groupLevelErrors.containsKey(groupId);
    }

    public Throwable groupError(String groupId) {
        return groupLevelErrors.get(groupId);
    }

    @Override
    public DescribeShareGroupOffsetsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        groupLevelErrors.values().forEach(exception -> updateErrorCounts(counts, Errors.forException(exception)));
        for (DescribeShareGroupOffsetsResponseGroup group : data.groups()) {
            group.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    updateErrorCounts(counts, Errors.forCode(partition.errorCode()))));
        }
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static DescribeShareGroupOffsetsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeShareGroupOffsetsResponse(new DescribeShareGroupOffsetsResponseData(new ByteBufferAccessor(buffer), version));
    }
}
