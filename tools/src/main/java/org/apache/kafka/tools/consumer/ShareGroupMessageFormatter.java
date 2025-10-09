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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.group.GroupCoordinatorRecordSerde;
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordJsonConverters;
import org.apache.kafka.coordinator.group.generated.CoordinatorRecordType;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Set;

/**
 * Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
 * Formats the records on the __consumer_offsets topic which pertain to share groups.
 */
public class ShareGroupMessageFormatter extends CoordinatorRecordMessageFormatter {
    private static final Set<Short> ALLOWED_RECORDS = Set.of(
        CoordinatorRecordType.SHARE_GROUP_MEMBER_METADATA.id(),
        CoordinatorRecordType.SHARE_GROUP_METADATA.id(),
        CoordinatorRecordType.SHARE_GROUP_TARGET_ASSIGNMENT_METADATA.id(),
        CoordinatorRecordType.SHARE_GROUP_TARGET_ASSIGNMENT_MEMBER.id(),
        CoordinatorRecordType.SHARE_GROUP_CURRENT_MEMBER_ASSIGNMENT.id(),
        CoordinatorRecordType.SHARE_GROUP_STATE_PARTITION_METADATA.id()
    );

    public ShareGroupMessageFormatter() {
        super(new GroupCoordinatorRecordSerde());
    }

    @Override
    protected boolean isRecordTypeAllowed(short recordType) {
        return ALLOWED_RECORDS.contains(recordType);
    }

    @Override
    protected JsonNode keyAsJson(ApiMessage message) {
        return CoordinatorRecordJsonConverters.writeRecordKeyAsJson(message);
    }

    @Override
    protected JsonNode valueAsJson(ApiMessage message, short version) {
        return CoordinatorRecordJsonConverters.writeRecordValueAsJson(message, version);
    }
}
