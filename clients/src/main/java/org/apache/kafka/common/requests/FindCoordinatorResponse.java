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
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class FindCoordinatorResponse extends AbstractResponse {

    private static final String THROTTLE_TIME_KEY_NAME = "throttle_time_ms";
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String ERROR_MESSAGE_KEY_NAME = "error_message";
    private static final String COORDINATOR_KEY_NAME = "coordinator";

    /**
     * Possible error codes:
     *
     * COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR (16)
     * GROUP_AUTHORIZATION_FAILED (30)
     */

    // coordinator level field names
    private static final String NODE_ID_KEY_NAME = "node_id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";

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
        this.throttleTimeMs = struct.hasField(THROTTLE_TIME_KEY_NAME) ? struct.getInt(THROTTLE_TIME_KEY_NAME) : DEFAULT_THROTTLE_TIME;
        error = Errors.forCode(struct.getShort(ERROR_CODE_KEY_NAME));
        if (struct.hasField(ERROR_MESSAGE_KEY_NAME))
            errorMessage = struct.getString(ERROR_MESSAGE_KEY_NAME);
        else
            errorMessage = null;

        Struct broker = (Struct) struct.get(COORDINATOR_KEY_NAME);
        int nodeId = broker.getInt(NODE_ID_KEY_NAME);
        String host = broker.getString(HOST_KEY_NAME);
        int port = broker.getInt(PORT_KEY_NAME);
        node = new Node(nodeId, host, port);
    }

    public int throttleTimeMs() {
        return throttleTimeMs;
    }

    public Errors error() {
        return error;
    }

    public Node node() {
        return node;
    }

    @Override
    protected Struct toStruct(short version) {
        Struct struct = new Struct(ApiKeys.FIND_COORDINATOR.responseSchema(version));
        if (struct.hasField(THROTTLE_TIME_KEY_NAME))
            struct.set(THROTTLE_TIME_KEY_NAME, throttleTimeMs);
        struct.set(ERROR_CODE_KEY_NAME, error.code());
        if (struct.hasField(ERROR_MESSAGE_KEY_NAME))
            struct.set(ERROR_MESSAGE_KEY_NAME, errorMessage);

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
}
