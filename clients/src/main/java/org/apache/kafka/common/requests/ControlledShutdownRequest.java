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
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.INT64;

public class ControlledShutdownRequest extends AbstractRequest {
    private static final String BROKER_ID_KEY_NAME = "broker_id";
    private static final String BROKER_EPOCH_KEY_NAME = "broker_epoch";

    private static final Schema CONTROLLED_SHUTDOWN_REQUEST_V0 = new Schema(
            new Field(BROKER_ID_KEY_NAME, INT32, "The id of the broker for which controlled shutdown has been requested."));
    private static final Schema CONTROLLED_SHUTDOWN_REQUEST_V1 = CONTROLLED_SHUTDOWN_REQUEST_V0;
    // Introduce broker_epoch to allow controller to reject stale ControlledShutdownRequest
    private static final Schema CONTROLLED_SHUTDOWN_REQUEST_V2 = new Schema(
            new Field(BROKER_ID_KEY_NAME, INT32, "The id of the broker for which controlled shutdown has been requested."),
            new Field(BROKER_EPOCH_KEY_NAME, INT64, "The broker epoch"));

    public static Schema[] schemaVersions() {
        return new Schema[] {CONTROLLED_SHUTDOWN_REQUEST_V0, CONTROLLED_SHUTDOWN_REQUEST_V1, CONTROLLED_SHUTDOWN_REQUEST_V2};
    }

    public static class Builder extends AbstractRequest.Builder<ControlledShutdownRequest> {
        private final int brokerId;
        private final long brokerEpoch;

        public Builder(int brokerId, long brokerEpoch, short desiredVersion) {
            super(ApiKeys.CONTROLLED_SHUTDOWN, desiredVersion);
            this.brokerId = brokerId;
            this.brokerEpoch = brokerEpoch;
        }

        @Override
        public ControlledShutdownRequest build(short version) {
            return new ControlledShutdownRequest(brokerId, brokerEpoch, version);
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ControlledShutdownRequest").
                append(", brokerId=").append(brokerId).
                append(", brokerEpoch=").append(brokerEpoch).
                append(")");
            return bld.toString();
        }
    }
    private final int brokerId;
    private final long brokerEpoch;

    private ControlledShutdownRequest(int brokerId, long brokerEpoch, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN, version);
        this.brokerId = brokerId;
        this.brokerEpoch = brokerEpoch;
    }

    public ControlledShutdownRequest(Struct struct, short version) {
        super(ApiKeys.CONTROLLED_SHUTDOWN, version);
        brokerId = struct.getInt(BROKER_ID_KEY_NAME);
        brokerEpoch = struct.hasField(BROKER_EPOCH_KEY_NAME) ? struct.getLong(BROKER_EPOCH_KEY_NAME) :
                AbstractControlRequest.UNKNOWN_BROKER_EPOCH;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
            case 1:
            case 2:
                return new ControlledShutdownResponse(Errors.forException(e), Collections.emptySet());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                    versionId, this.getClass().getSimpleName(), ApiKeys.CONTROLLED_SHUTDOWN.latestVersion()));
        }
    }

    public int brokerId() {
        return brokerId;
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public static ControlledShutdownRequest parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownRequest(
                ApiKeys.CONTROLLED_SHUTDOWN.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.CONTROLLED_SHUTDOWN.requestSchema(version()));
        struct.set(BROKER_ID_KEY_NAME, brokerId);
        struct.setIfExists(BROKER_EPOCH_KEY_NAME, brokerEpoch);
        return struct;
    }
}
