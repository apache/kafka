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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.INT8;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class FindCoordinatorRequest extends AbstractRequest {
    private static final String GROUP_ID_KEY_NAME = "group_id";
    private static final String COORDINATOR_KEY_KEY_NAME = "coordinator_key";
    private static final String COORDINATOR_TYPE_KEY_NAME = "coordinator_type";

    private static final Schema FIND_COORDINATOR_REQUEST_V0 = new Schema(
            new Field("group_id", STRING, "The unique group id."));

    private static final Schema FIND_COORDINATOR_REQUEST_V1 = new Schema(
            new Field("coordinator_key", STRING, "Id to use for finding the coordinator (for groups, this is the groupId, " +
                            "for transactional producers, this is the transactional id)"),
            new Field("coordinator_type", INT8, "The type of coordinator to find (0 = group, 1 = transaction)"));

    public static Schema[] schemaVersions() {
        return new Schema[] {FIND_COORDINATOR_REQUEST_V0, FIND_COORDINATOR_REQUEST_V1};
    }

    public static class Builder extends AbstractRequest.Builder<FindCoordinatorRequest> {
        private final String coordinatorKey;
        private final CoordinatorType coordinatorType;
        private final short minVersion;

        public Builder(CoordinatorType coordinatorType, String coordinatorKey) {
            super(ApiKeys.FIND_COORDINATOR);
            this.coordinatorType = coordinatorType;
            this.coordinatorKey = coordinatorKey;
            this.minVersion = coordinatorType == CoordinatorType.TRANSACTION ? (short) 1 : (short) 0;
        }

        @Override
        public FindCoordinatorRequest build(short version) {
            if (version < minVersion)
                throw new UnsupportedVersionException("Cannot create a v" + version + " FindCoordinator request " +
                        "because we require features supported only in " + minVersion + " or later.");
            return new FindCoordinatorRequest(coordinatorType, coordinatorKey, version);
        }

        public String coordinatorKey() {
            return coordinatorKey;
        }

        public CoordinatorType coordinatorType() {
            return coordinatorType;
        }

        @Override
        public String toString() {
            StringBuilder bld = new StringBuilder();
            bld.append("(type=FindCoordinatorRequest, coordinatorKey=");
            bld.append(coordinatorKey);
            bld.append(", coordinatorType=");
            bld.append(coordinatorType);
            bld.append(")");
            return bld.toString();
        }
    }

    private final String coordinatorKey;
    private final CoordinatorType coordinatorType;

    private FindCoordinatorRequest(CoordinatorType coordinatorType, String coordinatorKey, short version) {
        super(version);
        this.coordinatorType = coordinatorType;
        this.coordinatorKey = coordinatorKey;
    }

    public FindCoordinatorRequest(Struct struct, short version) {
        super(version);

        if (struct.hasField(COORDINATOR_TYPE_KEY_NAME))
            this.coordinatorType = CoordinatorType.forId(struct.getByte(COORDINATOR_TYPE_KEY_NAME));
        else
            this.coordinatorType = CoordinatorType.GROUP;
        if (struct.hasField(GROUP_ID_KEY_NAME))
            this.coordinatorKey = struct.getString(GROUP_ID_KEY_NAME);
        else
            this.coordinatorKey = struct.getString(COORDINATOR_KEY_KEY_NAME);
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        short versionId = version();
        switch (versionId) {
            case 0:
                return new FindCoordinatorResponse(Errors.forException(e), Node.noNode());
            case 1:
                return new FindCoordinatorResponse(throttleTimeMs, Errors.forException(e), Node.noNode());

            default:
                throw new IllegalArgumentException(String.format("Version %d is not valid. Valid versions for %s are 0 to %d",
                        versionId, this.getClass().getSimpleName(), ApiKeys.FIND_COORDINATOR.latestVersion()));
        }
    }

    public String coordinatorKey() {
        return coordinatorKey;
    }

    public CoordinatorType coordinatorType() {
        return coordinatorType;
    }

    public static FindCoordinatorRequest parse(ByteBuffer buffer, short version) {
        return new FindCoordinatorRequest(ApiKeys.FIND_COORDINATOR.parseRequest(version, buffer), version);
    }

    @Override
    protected Struct toStruct() {
        Struct struct = new Struct(ApiKeys.FIND_COORDINATOR.requestSchema(version()));
        if (struct.hasField(GROUP_ID_KEY_NAME))
            struct.set(GROUP_ID_KEY_NAME, coordinatorKey);
        else
            struct.set(COORDINATOR_KEY_KEY_NAME, coordinatorKey);
        if (struct.hasField(COORDINATOR_TYPE_KEY_NAME))
            struct.set(COORDINATOR_TYPE_KEY_NAME, coordinatorType.id);
        return struct;
    }

    public enum CoordinatorType {
        GROUP((byte) 0), TRANSACTION((byte) 1);

        final byte id;

        CoordinatorType(byte id) {
            this.id = id;
        }

        public static CoordinatorType forId(byte id) {
            switch (id) {
                case 0:
                    return GROUP;
                case 1:
                    return TRANSACTION;
                default:
                    throw new IllegalArgumentException("Unknown coordinator type received: " + id);
            }
        }
    }

}
