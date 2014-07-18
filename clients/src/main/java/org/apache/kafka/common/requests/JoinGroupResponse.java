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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.*;

public class JoinGroupResponse extends AbstractRequestResponse {
    public static Schema curSchema = ProtoUtils.currentResponseSchema(ApiKeys.JOIN_GROUP.id);
    private static String ERROR_CODE_KEY_NAME = "error_code";
    private static String GENERATION_ID_KEY_NAME = "group_generation_id";
    private static String CONSUMER_ID_KEY_NAME = "consumer_id";
    private static String ASSIGNED_PARTITIONS_KEY_NAME = "assigned_partitions";
    private static String TOPIC_KEY_NAME = "topic";
    private static String PARTITIONS_KEY_NAME = "partitions";

    public static int UNKNOWN_GENERATION_ID = -1;
    public static String UNKNOWN_CONSUMER_ID = "";

    private final short errorCode;
    private final int generationId;
    private final String consumerId;
    private final List<TopicPartition> assignedPartitions;

    public JoinGroupResponse(short errorCode, int generationId, String consumerId, List<TopicPartition> assignedPartitions) {
        super(new Struct(curSchema));

        Map<String, List<Integer>> partitionsByTopic = CollectionUtils.groupDataByTopic(assignedPartitions);

        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        struct.set(GENERATION_ID_KEY_NAME, generationId);
        struct.set(CONSUMER_ID_KEY_NAME, consumerId);
        List<Struct> topicArray = new ArrayList<Struct>();
        for (Map.Entry<String, List<Integer>> entries: partitionsByTopic.entrySet()) {
            Struct topicData = struct.instance(ASSIGNED_PARTITIONS_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, entries.getKey());
            topicData.set(PARTITIONS_KEY_NAME, entries.getValue().toArray());
            topicArray.add(topicData);
        }
        struct.set(ASSIGNED_PARTITIONS_KEY_NAME, topicArray.toArray());

        this.errorCode = errorCode;
        this.generationId = generationId;
        this.consumerId = consumerId;
        this.assignedPartitions = assignedPartitions;
    }

    public JoinGroupResponse(short errorCode) {
        this(errorCode, UNKNOWN_GENERATION_ID, UNKNOWN_CONSUMER_ID, Collections.<TopicPartition>emptyList());
    }

    public JoinGroupResponse(Struct struct) {
        super(struct);
        assignedPartitions = new ArrayList<TopicPartition>();
        for (Object topicDataObj : struct.getArray(ASSIGNED_PARTITIONS_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionObj : topicData.getArray(PARTITIONS_KEY_NAME))
                assignedPartitions.add(new TopicPartition(topic, (Integer) partitionObj));
        }
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        generationId = struct.getInt(GENERATION_ID_KEY_NAME);
        consumerId = struct.getString(CONSUMER_ID_KEY_NAME);
    }

    public short errorCode() {
        return errorCode;
    }

    public int generationId() {
        return generationId;
    }

    public String consumerId() {
        return consumerId;
    }

    public List<TopicPartition> assignedPartitions() {
        return assignedPartitions;
    }

    public static JoinGroupResponse parse(ByteBuffer buffer) {
        return new JoinGroupResponse(((Struct) curSchema.read(buffer)));
    }
}