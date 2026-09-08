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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class GroupCoordinatorNodeTest {
    @Test
    public void testValidation() {
        assertDoesNotThrow(() -> new Node(0, "localhost", 9092));
        assertDoesNotThrow(() -> new Node(-1, "localhost", 9092));
        assertDoesNotThrow(() -> new GroupCoordinatorNode(0, "localhost", 9092));
        assertThrows(IllegalArgumentException.class, () -> new GroupCoordinatorNode(-1, "localhost", 9092));
    }

    @Test
    public void testIdString() {
        Node node0 = new Node(0, "localhost", 9092);
        Node gcNode0 = new GroupCoordinatorNode(0, "localhost", 9092);
        Assertions.assertEquals(node0.id(), gcNode0.id());
        assertNotEquals(node0.idString(), gcNode0.idString());
        assertEquals(0, Integer.parseInt(gcNode0.idString()));
        assertEquals(0, Integer.parseInt(node0.idString()));

        Node node1 = new Node(1, "localhost", 9092);
        Node gcNode1 = new GroupCoordinatorNode(1, "localhost", 9092);
        assertEquals(node1.id(), gcNode1.id());
        assertNotEquals(node1.idString(), gcNode1.idString());
        assertEquals(1, Integer.parseInt(node1.idString()));
        assertEquals(1, Integer.parseInt(gcNode1.idString()));
    }
}