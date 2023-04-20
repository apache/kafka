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
package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.coordinator.group.generated.ConsumerGroupMemberMetadataValue;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ClientAssignorTest {

    @Test
    public void testNameAndMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new ClientAssignor(
            "range",
            (byte) 2,
            (short) 5,
            (short) 10,
            null
        ));

        assertThrows(NullPointerException.class, () -> new ClientAssignor(
            null,
            (byte) 2,
            (short) 5,
            (short) 10,
            new VersionedMetadata(
                (short) 8,
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
            )
        ));
    }

    @Test
    public void testAttributes() {
        ClientAssignor clientAssignor = new ClientAssignor(
            "range",
            (byte) 2,
            (short) 5,
            (short) 10,
            new VersionedMetadata(
                (short) 8,
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
            )
        );

        assertEquals("range", clientAssignor.name());
        assertEquals((byte) 2, clientAssignor.reason());
        assertEquals((short) 5, clientAssignor.minimumVersion());
        assertEquals((short) 10, clientAssignor.maximumVersion());
        assertEquals(new VersionedMetadata(
            (short) 8,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        ), clientAssignor.metadata());
    }

    @Test
    public void testFromRecord() {
        ConsumerGroupMemberMetadataValue.Assignor record = new ConsumerGroupMemberMetadataValue.Assignor()
            .setName("range")
            .setReason((byte) 2)
            .setMinimumVersion((byte) 5)
            .setMaximumVersion((byte) 10)
            .setVersion((byte) 8)
            .setMetadata("hello".getBytes(StandardCharsets.UTF_8));

        ClientAssignor clientAssignor = ClientAssignor.fromRecord(record);

        assertEquals("range", clientAssignor.name());
        assertEquals((byte) 2, clientAssignor.reason());
        assertEquals((short) 5, clientAssignor.minimumVersion());
        assertEquals((short) 10, clientAssignor.maximumVersion());
        assertEquals(new VersionedMetadata(
            (short) 8,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        ), clientAssignor.metadata());
    }

    @Test
    public void testEquals() {
        ClientAssignor clientAssignor = new ClientAssignor(
            "range",
            (byte) 2,
            (short) 5,
            (short) 10,
            new VersionedMetadata(
                (short) 8,
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
            )
        );

        assertEquals(new ClientAssignor(
            "range",
            (byte) 2,
            (short) 5,
            (short) 10,
            new VersionedMetadata(
                (short) 8,
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
            )
        ), clientAssignor);

        assertNotEquals(new ClientAssignor(
            "uniform",
            (byte) 2,
            (short) 5,
            (short) 10,
            new VersionedMetadata(
                (short) 8,
                ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
            )
        ), clientAssignor);
    }
}
