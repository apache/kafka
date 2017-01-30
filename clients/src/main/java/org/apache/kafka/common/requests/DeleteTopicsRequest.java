/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DeleteTopicsRequest extends AbstractRequest {
    private static final String TOPICS_KEY_NAME = "topics";
    private static final String TIMEOUT_KEY_NAME = "timeout";

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
        public DeleteTopicsRequest build() {
            return new DeleteTopicsRequest(topics, timeout, version());
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
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.DELETE_TOPICS.id, version)), version);

        struct.set(TOPICS_KEY_NAME, topics.toArray());
        struct.set(TIMEOUT_KEY_NAME, timeout);

        this.topics = topics;
        this.timeout = timeout;
    }

    public DeleteTopicsRequest(Struct struct, short version) {
        super(struct, version);
        Object[] topicsArray = struct.getArray(TOPICS_KEY_NAME);
        Set<String> topics = new HashSet<>(topicsArray.length);
        for (Object topic : topicsArray)
            topics.add((String) topic);

        this.topics = topics;
        this.timeout = struct.getInt(TIMEOUT_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        Map<String, Errors> topicErrors = new HashMap<>();
        for (String topic : topics)
            topicErrors.put(topic, Errors.forException(e));

        switch (version()) {
            case 0:
                return new DeleteTopicsResponse(topicErrors);
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    version(), this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.DELETE_TOPICS.id)));
        }
    }

    public Set<String> topics() {
        return topics;
    }

    public Integer timeout() {
        return this.timeout;
    }

    public static DeleteTopicsRequest parse(ByteBuffer buffer, int versionId) {
        return new DeleteTopicsRequest(ProtoUtils.parseRequest(ApiKeys.DELETE_TOPICS.id, versionId, buffer),
                (short) versionId);
    }

    public static DeleteTopicsRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.DELETE_TOPICS.id));
    }
}
