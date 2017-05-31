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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeleteTopicsResponse extends AbstractResponse {
    private static final String TOPIC_ERROR_CODES_KEY_NAME = "topic_error_codes";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String ERROR_CODE_KEY_NAME = "error_code";

    /**
     * Possible error codes:
     *
     * REQUEST_TIMED_OUT(7)
     * INVALID_TOPIC_EXCEPTION(17)
     * TOPIC_AUTHORIZATION_FAILED(29)
     * NOT_CONTROLLER(41)
     */
    private final Map<String, Errors> errors;
    private final int throttleTimeMs;

    public DeleteTopicsResponse(Map<String, Errors> errors) {
        this(DEFAULT_THROTTLE_TIME, errors);
    }

    public DeleteTopicsResponse(int throttleTimeMs, Map<String, Errors> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public DeleteTopicsResponse(Struct struct) {
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        Object[] topicErrorCodesStructs = struct.getArray(TOPIC_ERROR_CODES_KEY_NAME);
        Map<String, Errors> errors = new HashMap<>();
        for (Object topicErrorCodeStructObj : topicErrorCodesStructs) {
            Struct topicErrorCodeStruct = (Struct) topicErrorCodeStructObj;
            String topic = topicErrorCodeStruct.getString(TOPIC_KEY_NAME);
            Errors error = Errors.forCode(topicErrorCodeStruct.getShort(ERROR_CODE_KEY_NAME));
            errors.put(topic, error);
        }

        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DELETE_TOPICS.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        List<Struct> topicErrorCodeStructs = new ArrayList<>(errors.size());
        for (Map.Entry<String, Errors> topicError : errors.entrySet()) {
            Struct topicErrorCodeStruct = struct.instance(TOPIC_ERROR_CODES_KEY_NAME);
            topicErrorCodeStruct.set(TOPIC_KEY_NAME, topicError.getKey());
            topicErrorCodeStruct.set(ERROR_CODE_KEY_NAME, topicError.getValue().code());
            topicErrorCodeStructs.add(topicErrorCodeStruct);
        }
        struct.set(TOPIC_ERROR_CODES_KEY_NAME, topicErrorCodeStructs.toArray());
        return struct;
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<String, Errors> errors() {
        return errors;
    }

    public static DeleteTopicsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsResponse(ApiKeys.DELETE_TOPICS.responseSchema(version).read(buffer));
    }
}
