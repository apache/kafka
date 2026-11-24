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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.Node;

/**
 * This subclass of {@link Node} is used by the consumer for information about
 * a Kafka node which is a group coordinator. It ensures that the idString differs from
 * a regular node with the same node ID so that the network code can maintain separate
 * network connections to the same node as a regular broker and as a group coordinator.
 * It achieves this by ensuring that the node ID is non-negative (which it must be because
 * negative node IDs are used for bootstrapping) and by prepending a '+' on the node ID to
 * create the idString. This maintains the requirement that the idString can be parsed as
 * an integer to obtain the actual node ID.
 */
public class GroupCoordinatorNode extends Node {
    public GroupCoordinatorNode(int id, String host, int port) {
        super(GroupCoordinatorNode.validateId(id), host, port, null, false, "+" + id);
    }

    private static int validateId(int id) {
        if (id < 0) {
            throw new IllegalArgumentException("Node id for group coordinator node cannot be negative");
        }
        return id;
    }
}