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
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.CommonFields.TOPIC_NAME;


public class CreatePartitionsResponse extends AbstractResponse {

    private static final String TOPIC_ERRORS_KEY_NAME = "topic_errors";

    private static final Schema CREATE_PARTITIONS_RESPONSE_V0 = new Schema(
            THROTTLE_TIME_MS,
            new Field(TOPIC_ERRORS_KEY_NAME, new ArrayOf(
                    new Schema(
                            TOPIC_NAME,
                            ERROR_CODE,
                            ERROR_MESSAGE
                    )), "Per topic results for the create partitions request")
    );

    public static Schema[] schemaVersions() {
        return new Schema[]{CREATE_PARTITIONS_RESPONSE_V0};
    }

    private final int throttleTimeMs;
    private final Map<String, ApiError> errors;

    public CreatePartitionsResponse(int throttleTimeMs, Map<String, ApiError> errors) {
        this.throttleTimeMs = throttleTimeMs;
        this.errors = errors;
    }

    public CreatePartitionsResponse(Struct struct) {
        super();
        Object[] topicErrorsArray = struct.getArray(TOPIC_ERRORS_KEY_NAME);
        Map<String, ApiError> errors = new HashMap<>(topicErrorsArray.length);
        for (Object topicErrorObj : topicErrorsArray) {
            Struct topicErrorStruct = (Struct) topicErrorObj;
            String topic = topicErrorStruct.get(TOPIC_NAME);
            ApiError error = new ApiError(topicErrorStruct);
            errors.put(topic, error);
        }
        this.throttleTimeMs = struct.get(THROTTLE_TIME_MS);
        this.errors = errors;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.CREATE_PARTITIONS.responseSchema(version));
        List<Struct> topicErrors = new ArrayList<>(errors.size());
        for (Map.Entry<String, ApiError> error : errors.entrySet()) {
            Struct errorStruct = struct.instance(TOPIC_ERRORS_KEY_NAME);
            errorStruct.set(TOPIC_NAME, error.getKey());
            error.getValue().write(errorStruct);
            topicErrors.add(errorStruct);
        }
        struct.set(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(TOPIC_ERRORS_KEY_NAME, topicErrors.toArray(new Object[topicErrors.size()]));
        return struct;
    }

    public Map<String, ApiError> errors() {
        return errors;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return apiErrorCounts(errors);
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public static CreatePartitionsResponse parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsResponse(ApiKeys.CREATE_PARTITIONS.parseResponse(version, buffer));
    }

}
