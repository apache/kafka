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

import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class DeleteTopicsRequest extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String TIMEOUT_KEY_NAME = "timeout";

    /* DeleteTopic api */
    private static final Schema DELETE_TOPICS_REQUEST_V0 = new Schema(
            new Field(TOPICS_KEY_NAME, new ArrayOf(STRING), "An array of topics to be deleted."),
            new Field(TIMEOUT_KEY_NAME, INT32, "The time in ms to wait for a topic to be completely deleted on the " +
                    "controller node. Values <= 0 will trigger topic deletion and return immediately"));

    /* v1 request is the same as v0. Throttle time has been added to the response */
    private static final Schema DELETE_TOPICS_REQUEST_V1 = DELETE_TOPICS_REQUEST_V0;

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DELETE_TOPICS_REQUEST_V2 = DELETE_TOPICS_REQUEST_V1;

    /**
     * v3 request is the same that as v2. The response is different based on the request version.
     * In v3 version a TopicDeletionDisabledException is returned
     */
    private static final Schema DELETE_TOPICS_REQUEST_V3 = DELETE_TOPICS_REQUEST_V2;

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_TOPICS_REQUEST_V0, DELETE_TOPICS_REQUEST_V1,
            DELETE_TOPICS_REQUEST_V2, DELETE_TOPICS_REQUEST_V3};
    }

    private DeleteTopicsRequestData data;
    private final short version;

    public static class Builder extends AbstractRequest.Builder<DeleteTopicsRequest> {
        private DeleteTopicsRequestData data;

        public Builder(DeleteTopicsRequestData data) {
            super(ApiKeys.DELETE_TOPICS);
            this.data = data;
        }

        @Override
        public DeleteTopicsRequest build(short version) {
            return new DeleteTopicsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private DeleteTopicsRequest(DeleteTopicsRequestData data, short version) {
        super(ApiKeys.DELETE_TOPICS, version);
        this.data = data;
        this.version = version;
    }

    public DeleteTopicsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_TOPICS, version);
        this.data = new DeleteTopicsRequestData(struct, version);
        this.version = version;
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version);
    }

    public DeleteTopicsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeleteTopicsResponseData response = new DeleteTopicsResponseData();
        if (version >= 1) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        ApiError apiError = ApiError.fromThrowable(e);
        for (String topic : data.topicNames()) {
            response.responses().add(new DeletableTopicResult()
                    .setName(topic)
                    .setErrorCode(apiError.error().code()));
        }
        return new DeleteTopicsResponse(response);
    }

    public static DeleteTopicsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsRequest(ApiKeys.DELETE_TOPICS.parseRequest(version, buffer), version);
    }

}
