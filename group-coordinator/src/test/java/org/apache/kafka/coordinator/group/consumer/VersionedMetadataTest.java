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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VersionedMetadataTest {
    @Test
    public void testAttributes() {
        VersionedMetadata metadata = new VersionedMetadata(
            (short) 1,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        );

        assertEquals((short) 1, metadata.version());
        assertEquals(ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)), metadata.metadata());
    }

    @Test
    public void testMetadataCannotBeNull() {
        assertThrows(NullPointerException.class, () -> new VersionedMetadata((short) 1, null));
    }

    @Test
    public void testEquals() {
        VersionedMetadata metadata = new VersionedMetadata(
            (short) 1,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        );

        assertEquals(new VersionedMetadata(
            (short) 1,
            ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8))
        ), metadata);

        assertNotEquals(VersionedMetadata.EMPTY, metadata);
    }
}
