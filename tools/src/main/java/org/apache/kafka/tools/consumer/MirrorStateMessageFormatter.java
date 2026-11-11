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
import org.apache.kafka.coordinator.mirror.MirrorCoordinatorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.CoordinatorRecordJsonConverters;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateValue;
import org.apache.kafka.server.mirror.MirrorPartitionState;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Formats records from the {@code __mirror_state} topic for the console consumer.
 */
public class MirrorStateMessageFormatter extends CoordinatorRecordMessageFormatter {
    public MirrorStateMessageFormatter() {
        super(new MirrorCoordinatorRecordSerde());
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
        JsonNode json = CoordinatorRecordJsonConverters.writeRecordValueAsJson(message, version);
        if (message instanceof MirrorPartitionStateValue state) {
            ((ObjectNode) json).put("state", MirrorPartitionState.nameOf(state.state()));
            ((ObjectNode) json).put("previousState", MirrorPartitionState.nameOf(state.previousState()));
        }
        return json;
    }
}
