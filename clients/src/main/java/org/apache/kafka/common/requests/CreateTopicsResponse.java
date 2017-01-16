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
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTopicsResponse extends AbstractResponse {
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.CREATE_TOPICS.id);

    private static final String TOPIC_ERRORS_KEY_NAME = "topic_errors";
    private static final String TOPIC_KEY_NAME = "topic";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String ERROR_MESSAGE_KEY_NAME = "error_message";

    public static class Error {
        private final Errors error;
        private final String message; // introduced in V1

        public Error(Errors error, String message) {
            this.error = error;
            this.message = message;
        }

        public boolean is(Errors error) {
            return this.error == error;
        }

        public Errors error() {
            return error;
        }

        public String message() {
            return message;
        }

        public String messageWithFallback() {
            if (message == null)
                return error.message();
            return message;
        }

        @Override
        public String toString() {
            return "Error(error=" + error + ", message=" + message + ")";
        }
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

    private final Map<String, Error> errors;

    public CreateTopicsResponse(Map<String, Error> errors, short version) {
        super(new Struct(ProtoUtils.responseSchema(ApiKeys.CREATE_TOPICS.id, version)));

        List<Struct> topicErrorsStructs = new ArrayList<>(errors.size());
        for (Map.Entry<String, Error> topicError : errors.entrySet()) {
            Struct topicErrorsStruct = struct.instance(TOPIC_ERRORS_KEY_NAME);
            topicErrorsStruct.set(TOPIC_KEY_NAME, topicError.getKey());
            Error error = topicError.getValue();
            topicErrorsStruct.set(ERROR_CODE_KEY_NAME, error.error.code());
            if (version >= 1)
                topicErrorsStruct.set(ERROR_MESSAGE_KEY_NAME, error.message());
            topicErrorsStructs.add(topicErrorsStruct);
        }
        struct.set(TOPIC_ERRORS_KEY_NAME, topicErrorsStructs.toArray());

        this.errors = errors;
    }

    public CreateTopicsResponse(Struct struct) {
        super(struct);

        Object[] topicErrorStructs = struct.getArray(TOPIC_ERRORS_KEY_NAME);
        Map<String, Error> errors = new HashMap<>();
        for (Object topicErrorStructObj : topicErrorStructs) {
            Struct topicErrorCodeStruct = (Struct) topicErrorStructObj;
            String topic = topicErrorCodeStruct.getString(TOPIC_KEY_NAME);
            short errorCode = topicErrorCodeStruct.getShort(ERROR_CODE_KEY_NAME);
            String errorMessage = null;
            if (topicErrorCodeStruct.hasField(ERROR_MESSAGE_KEY_NAME))
                errorMessage = topicErrorCodeStruct.getString(ERROR_MESSAGE_KEY_NAME);
            errors.put(topic, new Error(Errors.forCode(errorCode), errorMessage));
        }

        this.errors = errors;
    }

    public Map<String, Error> errors() {
        return errors;
    }

    public static CreateTopicsResponse parse(ByteBuffer buffer) {
        return new CreateTopicsResponse(CURRENT_SCHEMA.read(buffer));
    }

    public static CreateTopicsResponse parse(ByteBuffer buffer, int version) {
        return new CreateTopicsResponse(ProtoUtils.responseSchema(ApiKeys.CREATE_TOPICS.id, version).read(buffer));
    }
}
