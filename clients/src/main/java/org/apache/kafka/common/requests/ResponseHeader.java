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

import org.apache.kafka.common.protocol.types.BoundField;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.INT32;

/**
 * A response header in the kafka protocol.
 */
public class ResponseHeader extends AbstractRequestResponse {
    public static final Schema SCHEMA = new Schema(
            new Field("correlation_id", INT32, "The user-supplied value passed in with the request"));
    private static final BoundField CORRELATION_KEY_FIELD = SCHEMA.get("correlation_id");

    private final int correlationId;

    public ResponseHeader(Struct struct) {
        correlationId = struct.getInt(CORRELATION_KEY_FIELD);
    }

    public ResponseHeader(int correlationId) {
        this.correlationId = correlationId;
    }

    public int sizeOf() {
        return toStruct().sizeOf();
    }

    public Struct toStruct() {
        Struct struct = new Struct(SCHEMA);
        struct.set(CORRELATION_KEY_FIELD, correlationId);
        return struct;
    }

    public int correlationId() {
        return correlationId;
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(SCHEMA.read(buffer));
    }

}
