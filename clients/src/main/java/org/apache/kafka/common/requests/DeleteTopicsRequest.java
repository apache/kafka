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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

    private final Set<String> topics;
    private final Integer timeout;

    public static class Builder extends AbstractRequest.Builder<DeleteTopicsRequest> {
        private final Set<String> topics;
        private final Integer timeout;

        public Builder(Set<String> topics, Integer timeout) {
            super(ApiKeys.DELETE_TOPICS);
            this.topics = topics;
            this.timeout = timeout;
        }

        @Override
        public DeleteTopicsRequest build(short version) {
            return new DeleteTopicsRequest(topics, timeout, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=DeleteTopicsRequest").
                append(", topics=(").append(Utils.join(topics, ", ")).append(")").
                append(", timeout=").append(timeout).
                append(")");
            return bld.toString();
        }
    }

    private DeleteTopicsRequest(Set<String> topics, Integer timeout, short version) {
        super(ApiKeys.DELETE_TOPICS, version);
        this.topics = topics;
        this.timeout = timeout;
    }

    public DeleteTopicsRequest(Struct struct, short version) {
        super(ApiKeys.DELETE_TOPICS, version);
        Object[] topicsArray = struct.getArray(TOPICS_KEY_NAME);
        Set<String> topics = new HashSet<>(topicsArray.length);
        for (Object topic : topicsArray)
            topics.add((String) topic);

        this.topics = topics;
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.DELETE_TOPICS.requestSchema(version()));
        struct.set(TOPICS_KEY_NAME, topics.toArray());
        struct.set(TIMEOUT_KEY_NAME, timeout);
        return struct;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : topics)
            topicErrors.put(topic, Errors.forException(e));

        switch (version()) {
            case 0:
                return new DeleteTopicsResponse(topicErrors);
            case 1:
            case 2:
            case 3:
                return new DeleteTopicsResponse(throttleTimeMs, topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    version(), this.getClass().getSimpleName(), ApiKeys.DELETE_TOPICS.latestVersion()));
        }
    }

    public Set<String> topics() {
        return topics;
    }

    public Integer timeout() {
        return this.timeout;
    }

    public static DeleteTopicsRequest parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsRequest(ApiKeys.DELETE_TOPICS.parseRequest(version, buffer), version);
    }

}
