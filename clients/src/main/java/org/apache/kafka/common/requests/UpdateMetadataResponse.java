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
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;

public class UpdateMetadataResponse extends AbstractResponse {
    private static final Schema UPDATE_METADATA_RESPONSE_V0 = new Schema(ERROR_CODE);
    private static final Schema UPDATE_METADATA_RESPONSE_V1 = UPDATE_METADATA_RESPONSE_V0;
    private static final Schema UPDATE_METADATA_RESPONSE_V2 = UPDATE_METADATA_RESPONSE_V1;
    private static final Schema UPDATE_METADATA_RESPONSE_V3 = UPDATE_METADATA_RESPONSE_V2;
    private static final Schema UPDATE_METADATA_RESPONSE_V4 = UPDATE_METADATA_RESPONSE_V3;

    public static Schema[] schemaVersions() {
        return new Schema[]{UPDATE_METADATA_RESPONSE_V0, UPDATE_METADATA_RESPONSE_V1, UPDATE_METADATA_RESPONSE_V2,
            UPDATE_METADATA_RESPONSE_V3, UPDATE_METADATA_RESPONSE_V4};
    }

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     */
    private final Errors error;

    public UpdateMetadataResponse(Errors error) {
        this.error = error;
    }

    public UpdateMetadataResponse(Struct struct) {
        error = Errors.forCode(struct.get(ERROR_CODE));
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public static UpdateMetadataResponse parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataResponse(ApiKeys.UPDATE_METADATA.parseResponse(version, buffer));
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.UPDATE_METADATA.responseSchema(version));
        struct.set(ERROR_CODE, error.code());
        return struct;
    }
}
