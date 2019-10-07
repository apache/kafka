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

import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;
import static org.apache.kafka.common.protocol.types.Type.BOOLEAN;
import static org.apache.kafka.common.protocol.types.Type.INT32;

public class CreatePartitionsRequest extends AbstractRequest {

    private static final String TOPIC_PARTITIONS_KEY_NAME = "topic_partitions";
    private static final String NEW_PARTITIONS_KEY_NAME = "new_partitions";
    private static final String COUNT_KEY_NAME = "count";
    private static final String ASSIGNMENT_KEY_NAME = "assignment";
    private static final String TIMEOUT_KEY_NAME = "timeout";
    private static final String VALIDATE_ONLY_KEY_NAME = "validate_only";

    private static final Schema CREATE_PARTITIONS_REQUEST_V0 = new Schema(
            new Field(TOPIC_PARTITIONS_KEY_NAME, new ArrayOf(
                    new Schema(
                            TOPIC_NAME,
                            new Field(NEW_PARTITIONS_KEY_NAME, new Schema(
                                    new Field(COUNT_KEY_NAME, INT32, "The new partition count."),
                                    new Field(ASSIGNMENT_KEY_NAME, ArrayOf.nullable(new ArrayOf(INT32)),
                                            "The assigned brokers.")
                            )))),
                    "List of topic and the corresponding new partitions."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for the partitions to be created."),
            new Field(VALIDATE_ONLY_KEY_NAME, BOOLEAN,
                    "If true then validate the request, but don't actually increase the number of partitions."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema CREATE_PARTITIONS_REQUEST_V1 = CREATE_PARTITIONS_REQUEST_V0;

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_PARTITIONS_REQUEST_V0, CREATE_PARTITIONS_REQUEST_V1};
    }

    private final CreatePartitionsRequestData data;
    private final short version;

    public static class Builder extends AbstractRequest.Builder<CreatePartitionsRequest> {

        private final CreatePartitionsRequestData data;

        public Builder(CreatePartitionsRequestData data) {
            super(ApiKeys.CREATE_PARTITIONS);
            this.data = data;
        }

        @Override
        public CreatePartitionsRequest build(short version) {
            return new CreatePartitionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    CreatePartitionsRequest(CreatePartitionsRequestData data, short apiVersion) {
        super(ApiKeys.CREATE_PARTITIONS, apiVersion);
        this.data = data;
        this.version = apiVersion;
    }

    public CreatePartitionsRequest(Struct struct, short apiVersion) {
        super(ApiKeys.CREATE_PARTITIONS, apiVersion);

        this.data = new CreatePartitionsRequestData(struct, apiVersion);
        this.version = apiVersion;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    public CreatePartitionsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        CreatePartitionsResponseData response = new CreatePartitionsResponseData();
        response.setThrottleTimeMs(throttleTimeMs);

        ApiError apiError = ApiError.fromThrowable(e);
        for (CreatePartitionsTopic topic : data.topics()) {
            response.results().add(new CreatePartitionsTopicResult()
                    .setName(topic.name())
                    .setErrorCode(apiError.error().code())
                    .setErrorMessage(apiError.message())
            );
        }
        return new CreatePartitionsResponse(response);
    }

    public static CreatePartitionsRequest parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsRequest(ApiKeys.CREATE_PARTITIONS.parseRequest(version, buffer), version);
    }
}
