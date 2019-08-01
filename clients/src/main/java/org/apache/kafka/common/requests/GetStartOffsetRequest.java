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

import org.apache.kafka.common.NewOffsetMetaData;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GetStartOffsetRequest extends AbstractRequest {
    private static final String TOPIC_DATA_KEY_NAME = "topic_data";

    // topic level field names
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String PARTITION_DATA_KEY_NAME = "data";

    // partition level field names
    private static final String PARTITION_KEY_NAME = "partition";

    // NewOffsetMetaData
    private static final String LEO_KEY_NAME = "leo";
    private static final String LSO_KEY_NAME = "lso";

    private static final String BROKERID_KEY_NAME = "brokerid";
    private static final String LST_KEY_NAME = "lst";
    private static final String LET_KEY_NAME = "let";



    private final LinkedHashMap<TopicPartition, NewOffsetMetaData> partitionOffsetMetaDatas;


    public static class Builder extends AbstractRequest.Builder<GetStartOffsetRequest> {
        private LinkedHashMap<TopicPartition, NewOffsetMetaData> partitionOffsetMetaDatas;

        public Builder(LinkedHashMap<TopicPartition, NewOffsetMetaData> partitionOffsetMetaDatas) {
            super(ApiKeys.GET_START_OFFSET);
            this.partitionOffsetMetaDatas = partitionOffsetMetaDatas;
        }

        @Override
        public GetStartOffsetRequest build() {
            short version = version();
            return new GetStartOffsetRequest(this.partitionOffsetMetaDatas, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=GetStartOffsetRequest").
                    append(", partitionOffsetMetaDatas=");
            if (partitionOffsetMetaDatas == null) {
                bld.append("<NULL>");
            } else {
                bld.append(", partitionRecords=(").append(Utils.mkString(partitionOffsetMetaDatas));
            }
            bld.append(")");
            return bld.toString();
        }
    }

    /**
     * In v0 null is not allowed and and empty list indicates requesting all topics.
     * Note: modern clients do not support sending v0 requests.
     * In v1 null indicates requesting all topics, and an empty list indicates requesting no topics.
     */

    public GetStartOffsetRequest(Struct struct, short version) {
        super(struct, version);
        partitionOffsetMetaDatas = new LinkedHashMap<>();
        for (Object topicDataObj : struct.getArray(TOPIC_DATA_KEY_NAME)) {
            Struct topicData = (Struct) topicDataObj;
            String topic = topicData.getString(TOPIC_KEY_NAME);
            for (Object partitionResponseObj : topicData.getArray(PARTITION_DATA_KEY_NAME)) {
                Struct partitionResponse = (Struct) partitionResponseObj;
                int partition = partitionResponse.getInt(PARTITION_KEY_NAME);
                int brokerid = partitionResponse.getInt(BROKERID_KEY_NAME);
                long leo = partitionResponse.getLong(LEO_KEY_NAME);
                long lst = partitionResponse.getLong(LST_KEY_NAME);
                long let = partitionResponse.getLong(LET_KEY_NAME);
                long lso = partitionResponse.getLong(LSO_KEY_NAME);
                NewOffsetMetaData newOffsetMetaData = new NewOffsetMetaData(brokerid, leo, lst, let, lso);
                partitionOffsetMetaDatas.put(new TopicPartition(topic, partition), newOffsetMetaData);
            }
        }
    }

    public GetStartOffsetRequest(LinkedHashMap<TopicPartition, NewOffsetMetaData> partitionOffsetMetaDatas, short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.GET_START_OFFSET.id, version)), version);
        List<FetchRequest.TopicAndPartitionData<NewOffsetMetaData>> topicsData = FetchRequest.TopicAndPartitionData.batchByTopic(partitionOffsetMetaDatas);

        List<Struct> topicArray = new ArrayList<>();
        for (FetchRequest.TopicAndPartitionData<NewOffsetMetaData> topicEntry : topicsData) {
            Struct topicData = struct.instance(TOPIC_DATA_KEY_NAME);
            topicData.set(TOPIC_KEY_NAME, topicEntry.topic);
            List<Struct> partitionArray = new ArrayList<>();
            for (Map.Entry<Integer, NewOffsetMetaData> partitionEntry : topicEntry.partitions.entrySet()) {
                NewOffsetMetaData newOffsetMetaData = partitionEntry.getValue();
                Struct partitionData = topicData.instance(PARTITION_DATA_KEY_NAME);
                partitionData.set(PARTITION_KEY_NAME, partitionEntry.getKey());
                partitionData.set(LEO_KEY_NAME, newOffsetMetaData.leo);
                partitionData.set(BROKERID_KEY_NAME, newOffsetMetaData.brokerid);
                partitionData.set(LST_KEY_NAME, newOffsetMetaData.lst);
                partitionData.set(LET_KEY_NAME, newOffsetMetaData.let);
                partitionData.set(LSO_KEY_NAME, newOffsetMetaData.lso);

                partitionArray.add(partitionData);
            }
            topicData.set(PARTITION_DATA_KEY_NAME, partitionArray.toArray());
            topicArray.add(topicData);
        }
        struct.set(TOPIC_DATA_KEY_NAME, topicArray.toArray());

        this.partitionOffsetMetaDatas = partitionOffsetMetaDatas;
    }

    @Override
    public String toString() {
        // Use the same format as `Struct.toString()`
        StringBuilder bld = new StringBuilder();
        bld.append(",partitionOffsetMetaDatas=")
                .append(Utils.mkString(partitionOffsetMetaDatas, "[", "]", "=", ","))
                .append("}");
        return bld.toString();
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        LinkedHashMap<TopicPartition, GetStartOffsetResponse.StartOffsetResponse> responseData = new LinkedHashMap<>();

        for (Map.Entry<TopicPartition, NewOffsetMetaData> entry: partitionOffsetMetaDatas.entrySet()) {
            GetStartOffsetResponse.StartOffsetResponse partitionResponse = new GetStartOffsetResponse.StartOffsetResponse(Errors.UNKNOWN, -1);
            responseData.put(entry.getKey(), partitionResponse);
        }
        short versionId = version();
        return new GetStartOffsetResponse(responseData, versionId);
    }

    public int getBrokerId() {
        if (!this.partitionOffsetMetaDatas.isEmpty()) {
            return this.partitionOffsetMetaDatas.entrySet().iterator().next().getValue().brokerid;
        } else {
            return -1;
        }
    }

    public Map<TopicPartition, NewOffsetMetaData> partitionRecordsOrFail() {
        // Store it in a local variable to protect against concurrent updates
        Map<TopicPartition, NewOffsetMetaData> partitionOffsetMetaDatas = this.partitionOffsetMetaDatas;
        if (partitionOffsetMetaDatas == null) {
            throw new IllegalStateException("The partition records are no longer available because");
        }
        return partitionOffsetMetaDatas;
    }

    public static GetStartOffsetRequest parse(ByteBuffer buffer, int versionId) {
        return new GetStartOffsetRequest(ProtoUtils.parseRequest(ApiKeys.GET_START_OFFSET.id, versionId, buffer), (short) versionId);
    }

    public static GetStartOffsetRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.GET_START_OFFSET.id));
    }
}
