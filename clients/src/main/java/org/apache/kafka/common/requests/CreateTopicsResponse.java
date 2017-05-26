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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTopicsResponse extends AbstractResponse {
    private static final String TOPIC_ERRORS_KEY_NAME = "topic_errors";
    private static final String TOPIC_KEY_NAME = "topic";

    /**
     * Possible error codes:
     *
     * REQUEST_TIMED_OUT(7)
     * INVALID_TOPIC_EXCEPTION(17)
     * CLUSTER_AUTHORIZATION_FAILED(31)
     * TOPIC_ALREADY_EXISTS(36)
     * INVALID_PARTITIONS(37)
     * INVALID_REPLICATION_FACTOR(38)
     * INVALID_REPLICA_ASSIGNMENT(39)
     * INVALID_CONFIG(40)
     * NOT_CONTROLLER(41)
     * INVALID_REQUEST(42)
     */

    private final Map<String, ApiError> errors;
    private final int throttleTimeMs;

    public CreateTopicsResponse(Map<String, ApiError> errors) {
        this(DEFAULT_THROTTLE_TIME, errors);
    }

    public CreateTopicsResponse(int throttleTimeMs, Map<String, ApiError> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public CreateTopicsResponse(Struct struct) {
        Object[] topicErrorStructs = struct.getArray(TOPIC_ERRORS_KEY_NAME);
        Map<String, ApiError> errors = new HashMap<>();
        for (Object topicErrorStructObj : topicErrorStructs) {
            Struct topicErrorStruct = (Struct) topicErrorStructObj;
            String topic = topicErrorStruct.getString(TOPIC_KEY_NAME);
            errors.put(topic, new ApiError(topicErrorStruct));
        }

        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_TOPICS.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);

        List<Struct> topicErrorsStructs = new ArrayList<>(errors.size());
        for (Map.Entry<String, ApiError> topicError : errors.entrySet()) {
            Struct topicErrorsStruct = struct.instance(TOPIC_ERRORS_KEY_NAME);
            topicErrorsStruct.set(TOPIC_KEY_NAME, topicError.getKey());
            topicError.getValue().write(topicErrorsStruct);
            topicErrorsStructs.add(topicErrorsStruct);
        }
        struct.set(TOPIC_ERRORS_KEY_NAME, topicErrorsStructs.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<String, ApiError> errors() {
        return errors;
    }

    public static CreateTopicsResponse parse(ByteBuffer buffer, short version) {
        return new CreateTopicsResponse(ApiKeys.CREATE_TOPICS.responseSchema(version).read(buffer));
    }
}
