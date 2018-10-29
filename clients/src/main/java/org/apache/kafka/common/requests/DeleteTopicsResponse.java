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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class DeleteTopicsResponse extends AbstractResponse {
    private static final String TOPIC_ERROR_CODES_KEY_NAME = "topic_error_codes";

    private static final Schema TOPIC_ERROR_CODE = new Schema(
            TOPIC_NAME,
            ERROR_CODE);

    private static final Schema DELETE_TOPICS_RESPONSE_V0 = new Schema(
            new Field(TOPIC_ERROR_CODES_KEY_NAME,
                    new ArrayOf(TOPIC_ERROR_CODE), "An array of per topic error codes."));

    private static final Schema DELETE_TOPICS_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPIC_ERROR_CODES_KEY_NAME, new ArrayOf(TOPIC_ERROR_CODE), "An array of per topic error codes."));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema DELETE_TOPICS_RESPONSE_V2 = DELETE_TOPICS_RESPONSE_V1;

    /**
     * v3 request is the same that as v2. The response is different based on the request version.
     * In v3 version a TopicDeletionDisabledException is returned
     */
    private static final Schema DELETE_TOPICS_RESPONSE_V3 = DELETE_TOPICS_RESPONSE_V2;

    public static Schema[] schemaVersions() {
        return new Schema[]{DELETE_TOPICS_RESPONSE_V0, DELETE_TOPICS_RESPONSE_V1,
            DELETE_TOPICS_RESPONSE_V2, DELETE_TOPICS_RESPONSE_V3};
    }


    /**
     * Possible error codes:
     *
     * REQUEST_TIMED_OUT(7)
     * INVALID_TOPIC_EXCEPTION(17)
     * TOPIC_AUTHORIZATION_FAILED(29)
     * NOT_CONTROLLER(41)
     * INVALID_REQUEST(42)
     * TOPIC_DELETION_DISABLED(73)
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
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        Object[] topicErrorCodesStructs = struct.getArray(TOPIC_ERROR_CODES_KEY_NAME);
        Map<String, Errors> errors = new HashMap<>();
        for (Object topicErrorCodeStructObj : topicErrorCodesStructs) {
            Struct topicErrorCodeStruct = (Struct) topicErrorCodeStructObj;
            String topic = topicErrorCodeStruct.get(TOPIC_NAME);
            Errors error = Errors.forCode(topicErrorCodeStruct.get(ERROR_CODE));
            errors.put(topic, error);
        }

        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.DELETE_TOPICS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        List<Struct> topicErrorCodeStructs = new ArrayList<>(errors.size());
        for (Map.Entry<String, Errors> topicError : errors.entrySet()) {
            Struct topicErrorCodeStruct = struct.instance(TOPIC_ERROR_CODES_KEY_NAME);
            topicErrorCodeStruct.set(TOPIC_NAME, topicError.getKey());
            topicErrorCodeStruct.set(ERROR_CODE, topicError.getValue().code());
            topicErrorCodeStructs.add(topicErrorCodeStruct);
        }
        struct.set(TOPIC_ERROR_CODES_KEY_NAME, topicErrorCodeStructs.toArray());
        return struct;
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Map<String, Errors> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(errors);
    }

    public static DeleteTopicsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsResponse(ApiKeys.DELETE_TOPICS.responseSchema(version).read(buffer));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
