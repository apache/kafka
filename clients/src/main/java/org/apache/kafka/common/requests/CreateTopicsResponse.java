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
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;

public class CreateTopicsResponse extends AbstractResponse {
    private static final String TOPIC_ERRORS_KEY_NAME = "topic_errors";

    private static final Schema TOPIC_ERROR_CODE = new Schema(
            TOPIC_NAME,
            ERROR_CODE);

    // Improves on TOPIC_ERROR_CODE by adding an error_message to complement the error_code
    private static final Schema TOPIC_ERROR = new Schema(
            TOPIC_NAME,
            ERROR_CODE,
            ERROR_MESSAGE);

    private static final Schema CREATE_TOPICS_RESPONSE_V0 = new Schema(
            new Field(TOPIC_ERRORS_KEY_NAME, new ArrayOf(TOPIC_ERROR_CODE), "An array of per topic error codes."));

    private static final Schema CREATE_TOPICS_RESPONSE_V1 = new Schema(
            new Field(TOPIC_ERRORS_KEY_NAME, new ArrayOf(TOPIC_ERROR), "An array of per topic errors."));

    private static final Schema CREATE_TOPICS_RESPONSE_V2 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPIC_ERRORS_KEY_NAME, new ArrayOf(TOPIC_ERROR), "An array of per topic errors."));

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_TOPICS_RESPONSE_V0, CREATE_TOPICS_RESPONSE_V1, CREATE_TOPICS_RESPONSE_V2};
    }

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
            String topic = topicErrorStruct.get(TOPIC_NAME);
            errors.put(topic, new ApiError(topicErrorStruct));
        }

        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_TOPICS.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);

        List<Struct> topicErrorsStructs = new ArrayList<>(errors.size());
        for (Map.Entry<String, ApiError> topicError : errors.entrySet()) {
            Struct topicErrorsStruct = struct.instance(TOPIC_ERRORS_KEY_NAME);
            topicErrorsStruct.set(TOPIC_NAME, topicError.getKey());
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

    @Override
    public Map<Errors, Integer> errorCounts() {
        return apiErrorCounts(errors);
    }

    public static CreateTopicsResponse parse(ByteBuffer buffer, short version) {
        return new CreateTopicsResponse(ApiKeys.CREATE_TOPICS.responseSchema(version).read(buffer));
    }
}
