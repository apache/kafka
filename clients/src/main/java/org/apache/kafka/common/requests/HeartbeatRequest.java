/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class HeartbeatRequest extends AbstractRequestResponse {
    public static Schema curSchema = ProtoUtils.currentRequestSchema(ApiKeys.HEARTBEAT.id);
    private static String GROUP_ID_KEY_NAME = "group_id";
    private static String GROUP_GENERATION_ID_KEY_NAME = "group_generation_id";
    private static String CONSUMER_ID_KEY_NAME = "consumer_id";

    private final String groupId;
    private final int groupGenerationId;
    private final String consumerId;

    public HeartbeatRequest(String groupId, int groupGenerationId, String consumerId) {
        super(new Struct(curSchema));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(GROUP_GENERATION_ID_KEY_NAME, groupGenerationId);
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        this.groupId = groupId;
        this.groupGenerationId = groupGenerationId;
        this.consumerId = consumerId;
    }

    public HeartbeatRequest(Struct struct) {
        super(struct);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        groupGenerationId = struct.getInt(GROUP_GENERATION_ID_KEY_NAME);
        consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
    }

    public String groupId() {
        return groupId;
    }

    public int groupGenerationId() {
        return groupGenerationId;
    }

    public String consumerId() {
        return consumerId;
    }

    public static HeartbeatRequest parse(ByteBuffer buffer) {
        return new HeartbeatRequest(((Struct) curSchema.read(buffer)));
    }
}