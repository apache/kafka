/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Collections;

public class ControlledShutdownRequest extends AbstractRequest {
    private static final String BROKER_ID_KEY_NAME = "broker_id";

    public static class Builder extends AbstractRequest.Builder<ControlledShutdownRequest> {
        private final int brokerId;

        public Builder(int brokerId) {
            super(ApiKeys.CONTROLLED_SHUTDOWN_KEY);
            this.brokerId = brokerId;
        }

        @Override
        public ControlledShutdownRequest build() {
            return new ControlledShutdownRequest(brokerId, version());
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=ControlledShutdownRequest").
                append(", brokerId=").append(brokerId).
                append(")");
            return bld.toString();
        }
    }
    private int brokerId;

    private ControlledShutdownRequest(int brokerId, short version) {
        super(new Struct(ProtoUtils.requestSchema(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id, version)),
                version);
        struct.set(BROKER_ID_KEY_NAME, brokerId);
        this.brokerId = brokerId;
    }

    public ControlledShutdownRequest(Struct struct, short versionId) {
        super(struct, versionId);
        brokerId = struct.getInt(BROKER_ID_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                throw new IllegalArgumentException("Version 0 is not supported. It is only supported by " +
                        "the Scala request class for controlled shutdown");
            case 1:
                return new ControlledShutdownResponse(Errors.forException(e).code(), Collections.<TopicPartition>emptySet());
            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ProtoUtils.latestVersion(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id)));
        }
    }

    public int brokerId() {
        return brokerId;
    }

    public static ControlledShutdownRequest parse(ByteBuffer buffer, int versionId) {
        return new ControlledShutdownRequest(
                ProtoUtils.parseRequest(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id, versionId, buffer), (short) versionId);
    }

    public static ControlledShutdownRequest parse(ByteBuffer buffer) {
        return parse(buffer, ProtoUtils.latestVersion(ApiKeys.CONTROLLED_SHUTDOWN_KEY.id));
    }
}
