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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

public class GroupCoordinatorResponse extends AbstractRequestResponse {
    
    private static final Schema CURRENT_SCHEMA = ProtoUtils.currentResponseSchema(ApiKeys.GROUP_COORDINATOR.id);
    private static final String ERROR_CODE_KEY_NAME = "error_code";
    private static final String COORDINATOR_KEY_NAME = "coordinator";

    /**
     * Possible error codes:
     *
     * GROUP_COORDINATOR_NOT_AVAILABLE (15)
     * NOT_COORDINATOR_FOR_GROUP (16)
     * GROUP_AUTHORIZATION_FAILED (30)
     */


    // coordinator level field names
    private static final String NODE_ID_KEY_NAME = "node_id";
    private static final String HOST_KEY_NAME = "host";
    private static final String PORT_KEY_NAME = "port";

    private final short errorCode;
    private final Node node;

    public GroupCoordinatorResponse(short errorCode, Node node) {
        super(new Struct(CURRENT_SCHEMA));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        Struct coordinator = struct.instance(COORDINATOR_KEY_NAME);
        coordinator.set(NODE_ID_KEY_NAME, node.id());
        coordinator.set(HOST_KEY_NAME, node.host());
        coordinator.set(PORT_KEY_NAME, node.port());
        struct.set(COORDINATOR_KEY_NAME, coordinator);
        this.errorCode = errorCode;
        this.node = node;
    }

    public GroupCoordinatorResponse(Struct struct) {
        super(struct);
        errorCode = struct.getShort(ERROR_CODE_KEY_NAME);
        Struct broker = (Struct) struct.get(COORDINATOR_KEY_NAME);
        int nodeId = broker.getInt(NODE_ID_KEY_NAME);
        String host = broker.getString(HOST_KEY_NAME);
        int port = broker.getInt(PORT_KEY_NAME);
        node = new Node(nodeId, host, port);
    }

    public short errorCode() {
        return errorCode;
    }

    public Node node() {
        return node;
    }

    public static GroupCoordinatorResponse parse(ByteBuffer buffer) {
        return new GroupCoordinatorResponse(CURRENT_SCHEMA.read(buffer));
    }
}
