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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.Map;

import static org.apache.kafka.common.protocol.CommonFields.ERROR_CODE;
import static org.apache.kafka.common.protocol.CommonFields.ERROR_MESSAGE;
import static org.apache.kafka.common.protocol.CommonFields.THROTTLE_TIME_MS;
import static org.apache.kafka.common.protocol.types.Type.INT32;
import static org.apache.kafka.common.protocol.types.Type.STRING;

public class FindCoordinatorResponse extends AbstractResponse {
    private static final String COORDINATOR_KEY_NAME = "coordinator";

    // coordinator level field names
    private static final String NODE_ID_KEY_NAME = "node_id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";

    private static final Schema FIND_COORDINATOR_BROKER_V0 = new Schema(
            new Field(NODE_ID_KEY_NAME, INT32, "The broker id."),
            new Field(HOST_KEY_NAME, STRING, "The hostname of the broker."),
            new Field(PORT_KEY_NAME, INT32, "The port on which the broker accepts requests."));

    private static final Schema FIND_COORDINATOR_RESPONSE_V0 = new Schema(
            ERROR_CODE,
            new Field(COORDINATOR_KEY_NAME, FIND_COORDINATOR_BROKER_V0, "Host and port information for the coordinator " +
                    "for a consumer group."));

    private static final Schema FIND_COORDINATOR_RESPONSE_V1 = new Schema(
            THROTTLE_TIME_MS,
            ERROR_CODE,
            ERROR_MESSAGE,
            new Field(COORDINATOR_KEY_NAME, FIND_COORDINATOR_BROKER_V0, "Host and port information for the coordinator"));

    /**
     * The version number is bumped to indicate that on quota violation brokers send out responses before throttling.
     */
    private static final Schema FIND_COORDINATOR_RESPONSE_V2 = FIND_COORDINATOR_RESPONSE_V1;

    public static Schema[] schemaVersions() {
        return new Schema[] {FIND_COORDINATOR_RESPONSE_V0, FIND_COORDINATOR_RESPONSE_V1, FIND_COORDINATOR_RESPONSE_V2};
    }

    /**
     * Possible error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * COORDINATOR_NOT_AVAILABLE (15)
     * GROUP_AUTHORIZATION_FAILED (30)
     * INVALID_REQUEST (42)
     * TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)
     */


    private final int throttleTimeMs;
    private final String errorMessage;
    private final Errors error;
    private final Node node;

    public FindCoordinatorResponse(Errors error, Node node) {
        this(DEFAULT_THROTTLE_TIME, error, node);
    }

    public FindCoordinatorResponse(int throttleTimeMs, Errors error, Node node) {
        this.throttleTimeMs = throttleTimeMs;
        this.error = error;
        this.node = node;
        this.errorMessage = null;
    }

    public FindCoordinatorResponse(Struct struct) {
        this.throttleTimeMs = struct.getOrElse(THROTTLE_TIME_MS, DEFAULT_THROTTLE_TIME);
        error = Errors.forCode(struct.get(ERROR_CODE));
        errorMessage = struct.getOrElse(ERROR_MESSAGE, null);

        Struct broker = (Struct) struct.get(COORDINATOR_KEY_NAME);
        int nodeId = broker.getInt(NODE_ID_KEY_NAME);
        String host = broker.getString(HOST_KEY_NAME);
        int port = broker.getInt(PORT_KEY_NAME);
        node = new Node(nodeId, host, port);
    }

    @Override
    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public boolean hasError() {
        return this.error != Errors.NONE;
    }

    public Errors error() {
        return error;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error);
    }

    public Node node() {
        return node;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.FIND_COORDINATOR.responseSchema(version));
        struct.setIfExists(THROTTLE_TIME_MS, throttleTimeMs);
        struct.set(ERROR_CODE, error.code());
        struct.setIfExists(ERROR_MESSAGE, errorMessage);

        Struct coordinator = struct.instance(COORDINATOR_KEY_NAME);
        coordinator.set(NODE_ID_KEY_NAME, node.id());
        coordinator.set(HOST_KEY_NAME, node.host());
        coordinator.set(PORT_KEY_NAME, node.port());
        struct.set(COORDINATOR_KEY_NAME, coordinator);
        return struct;
    }

    public static FindCoordinatorResponse parse(ByteBuffer buffer, short version) {
        return new FindCoordinatorResponse(ApiKeys.FIND_COORDINATOR.responseSchema(version).read(buffer));
    }

    @Override
    public String toString() {
        return "FindCoordinatorResponse(" +
                "throttleTimeMs=" + throttleTimeMs +
                ", errorMessage='" + errorMessage + '\'' +
                ", error=" + error +
                ", node=" + node +
                ')';
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
