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

public class ConsumerMetadataResponse extends AbstractRequestResponse {
    private static Schema curSchema = ProtoUtils.currentResponseSchema(ApiKeys.CONSUMER_METADATA.id);
    private static String ERROR_CODE_KEY_NAME = "error_code";
    private static String COORDINATOR_KEY_NAME = "coordinator";

    // coordinator level field names
    private static String NODE_ID_KEY_NAME = "node_id";
    private static String HOST_KEY_NAME = "host";
    private static String PORT_KEY_NAME = "port";

    private final short errorCode;
    private final Node node;

    public ConsumerMetadataResponse(short errorCode, Node node) {
        super(new Struct(curSchema));
        struct.set(ERROR_CODE_KEY_NAME, errorCode);
        Struct coordinator = struct.instance(COORDINATOR_KEY_NAME);
        coordinator.set(NODE_ID_KEY_NAME, node.id());
        coordinator.set(HOST_KEY_NAME, node.host());
        coordinator.set(PORT_KEY_NAME, node.port());
        struct.set(COORDINATOR_KEY_NAME, coordinator);
        this.errorCode = errorCode;
        this.node = node;
    }

    public ConsumerMetadataResponse(Struct struct) {
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

    public static ConsumerMetadataResponse parse(ByteBuffer buffer) {
        return new ConsumerMetadataResponse(((Struct) curSchema.read(buffer)));
    }
}