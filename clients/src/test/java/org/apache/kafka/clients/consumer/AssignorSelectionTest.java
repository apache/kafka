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

package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.consumer.internals.AssignorSelection;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class AssignorSelectionTest {

    @Test
    public void testClientAssignorCannotBeNullIfSelected() {
        assertThrows(IllegalArgumentException.class,
                () -> AssignorSelection.newClientAssignors(null));
    }

    @Test
    public void testServerAssignorCannotBeNullOrEmptyIfSelected() {
        assertThrows(IllegalArgumentException.class,
                () -> AssignorSelection.newServerAssignor(null));
        assertThrows(IllegalArgumentException.class,
                () -> AssignorSelection.newServerAssignor(""));
    }

    @Test
    public void testEquals() {
        // Server assignors
        AssignorSelection selection1 = AssignorSelection.newServerAssignor("range");
        AssignorSelection selection2 = AssignorSelection.newServerAssignor("range");
        assertEquals(selection1, selection1);
        assertEquals(selection1, selection2);
        AssignorSelection selection3 = AssignorSelection.newServerAssignor("uniform");
        assertNotEquals(selection1, selection3);
        assertNotEquals(selection1, null);

        // Client assignors
        AssignorSelection clientSelection1 =
                AssignorSelection.newClientAssignors(Collections.singletonList(new ConsumerGroupHeartbeatRequestData.Assignor().setName("client-assignor-1")));
        AssignorSelection clientSelection2 =
                AssignorSelection.newClientAssignors(Collections.singletonList(new ConsumerGroupHeartbeatRequestData.Assignor().setName("client-assignor-1")));
        assertEquals(clientSelection1, clientSelection1);
        assertEquals(clientSelection1, clientSelection2);
        AssignorSelection clientSelection3 =
                AssignorSelection.newClientAssignors(Collections.singletonList(new ConsumerGroupHeartbeatRequestData.Assignor().setName("client-assignor-2")));
        assertNotEquals(clientSelection1, clientSelection3);
        assertNotEquals(clientSelection1, null);

        // Mixed server/client
        assertNotEquals(selection1, clientSelection1);
    }

    @Test
    public void testClientAssignorSelection() {
        ConsumerGroupHeartbeatRequestData.Assignor assignor =
                new ConsumerGroupHeartbeatRequestData.Assignor().setName("client-assignor-1");
        AssignorSelection selection =
                AssignorSelection.newClientAssignors(Collections.singletonList(assignor));
        assertEquals(AssignorSelection.Type.CLIENT, selection.type());
        assertNotNull(selection.clientAssignors());
        assertNull(selection.serverAssignor());
        assertEquals(1, selection.clientAssignors().size());
        assertEquals(assignor, selection.clientAssignors().get(0));
    }

    @Test
    public void testServerAssignorSelection() {
        String assignorName = "uniform";
        AssignorSelection selection = AssignorSelection.newServerAssignor(assignorName);
        assertEquals(AssignorSelection.Type.SERVER, selection.type());
        assertEquals(assignorName, selection.serverAssignor());
        assertNull(selection.clientAssignors());
    }
}
