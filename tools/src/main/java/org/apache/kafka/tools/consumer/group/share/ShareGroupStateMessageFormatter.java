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
package org.apache.kafka.tools.consumer.group.share;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.coordinator.share.ShareCoordinatorRecordSerde;
import org.apache.kafka.coordinator.share.generated.CoordinatorRecordJsonConverters;
import org.apache.kafka.tools.consumer.CoordinatorRecordMessageFormatter;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Formatter for records of in __share_group_state topic.
 */
public class ShareGroupStateMessageFormatter extends CoordinatorRecordMessageFormatter {

    public ShareGroupStateMessageFormatter() {
        super(new ShareCoordinatorRecordSerde());
    }

    @Override
    protected boolean isRecordTypeAllowed(short recordType) {
        return true;
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
