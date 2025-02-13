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

import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class AlterShareGroupOffsetsRequest extends AbstractRequest {

    private final AlterShareGroupOffsetsRequestData data;

    public AlterShareGroupOffsetsRequest(AlterShareGroupOffsetsRequestData data, short version) {
        super(ApiKeys.ALTER_SHARE_GROUP_OFFSETS, version);
        this.data = data;
    }

    public static class Builder extends AbstractRequest.Builder<AlterShareGroupOffsetsRequest> {

        private final AlterShareGroupOffsetsRequestData data;

        public Builder(AlterShareGroupOffsetsRequestData data) {
            this(data, true);
        }

        public Builder(AlterShareGroupOffsetsRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.ALTER_SHARE_GROUP_OFFSETS, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public AlterShareGroupOffsetsRequest build(short version) {
            return new AlterShareGroupOffsetsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        List<AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic> results = new ArrayList<>();
        data.topics().forEach(
            topicResult -> results.add(new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponseTopic()
                .setTopicName(topicResult.topicName())
                .setPartitions(topicResult.partitions().stream()
                    .map(partitionData -> new AlterShareGroupOffsetsResponseData.AlterShareGroupOffsetsResponsePartition()
                        .setPartitionIndex(partitionData.partitionIndex())
                        .setErrorCode(Errors.forException(e).code()))
                    .collect(Collectors.toList()))));
        return new AlterShareGroupOffsetsResponse(new AlterShareGroupOffsetsResponseData()
            .setResponses(results));
    }

    public static AlterShareGroupOffsetsRequest parse(ByteBuffer buffer, short version) {
        return new AlterShareGroupOffsetsRequest(
            new AlterShareGroupOffsetsRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }

    @Override
    public AlterShareGroupOffsetsRequestData data() {
        return data;
    }
}
