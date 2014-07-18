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
import java.util.ArrayList;
import java.util.List;

public class JoinGroupRequest extends AbstractRequestResponse {
    public static Schema curSchema = ProtoUtils.currentRequestSchema(ApiKeys.JOIN_GROUP.id);
    private static String GROUP_ID_KEY_NAME = "group_id";
    private static String SESSION_TIMEOUT_KEY_NAME = "session_timeout";
    private static String TOPICS_KEY_NAME = "topics";
    private static String CONSUMER_ID_KEY_NAME = "consumer_id";
    private static String STRATEGY_KEY_NAME = "partition_assignment_strategy";

    private final String groupId;
    private final int sessionTimeout;
    private final List<String> topics;
    private final String consumerId;
    private final String strategy;

    public JoinGroupRequest(String groupId, int sessionTimeout, List<String> topics, String consumerId, String strategy) {
        super(new Struct(curSchema));
        struct.set(GROUP_ID_KEY_NAME, groupId);
        struct.set(SESSION_TIMEOUT_KEY_NAME, sessionTimeout);
        struct.set(TOPICS_KEY_NAME, topics.toArray());
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        struct.set(STRATEGY_KEY_NAME, strategy);
        this.groupId = groupId;
        this.sessionTimeout = sessionTimeout;
        this.topics = topics;
        this.consumerId = consumerId;
        this.strategy = strategy;
    }

    public JoinGroupRequest(Struct struct) {
        super(struct);
        groupId = struct.getString(GROUP_ID_KEY_NAME);
        sessionTimeout = struct.getInt(SESSION_TIMEOUT_KEY_NAME);
        Object[] topicsArray = struct.getArray(TOPICS_KEY_NAME);
        topics = new ArrayList<String>();
        for (Object topic: topicsArray)
            topics.add((String) topic);
        consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
        strategy = struct.getString(STRATEGY_KEY_NAME);
    }

    public String groupId() {
        return groupId;
    }

    public int sessionTimeout() {
        return sessionTimeout;
    }

    public List<String> topics() {
        return topics;
    }

    public String consumerId() {
        return consumerId;
    }

    public String strategy() {
        return strategy;
    }

    public static JoinGroupRequest parse(ByteBuffer buffer) {
        return new JoinGroupRequest(((Struct) curSchema.read(buffer)));
    }
}
