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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.query.Position;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PositionSerdeTest {

    @Test
    public void shouldRoundTripPosition() {
        final Position position = Position.emptyPosition()
            .withComponent("topic1", 0, 1L)
            .withComponent("topic1", 1, 5L)
            .withComponent("topic2", 3, 100L);

        assertEquals(position, PositionSerde.deserialize(PositionSerde.serialize(position)));
    }

    @Test
    public void shouldRoundTripEmptyPosition() {
        final Position position = Position.emptyPosition();

        assertEquals(position, PositionSerde.deserialize(PositionSerde.serialize(position)));
    }

    @Test
    public void shouldRejectTopicCountLargerThanRemainingBuffer() {
        final ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + 1 + 4);
        buffer.put((byte) 0);
        buffer.putInt(1 << 30);
        buffer.putInt(1);
        buffer.put((byte) 't');
        buffer.putInt(0);
        buffer.flip();

        assertThrows(IllegalArgumentException.class, () -> PositionSerde.deserialize(buffer));
    }

    @Test
    public void shouldRejectNegativeTopicCount() {
        final ByteBuffer buffer = ByteBuffer.allocate(1 + 4);
        buffer.put((byte) 0);
        buffer.putInt(-1);
        buffer.flip();

        assertThrows(IllegalArgumentException.class, () -> PositionSerde.deserialize(buffer));
    }

    @Test
    public void shouldRejectPartitionCountLargerThanRemainingBuffer() {
        final ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + 1 + 4 + 4 + 8);
        buffer.put((byte) 0);
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.put((byte) 't');
        buffer.putInt(1 << 30);
        buffer.putInt(0);
        buffer.putLong(0L);
        buffer.flip();

        assertThrows(IllegalArgumentException.class, () -> PositionSerde.deserialize(buffer));
    }

    @Test
    public void shouldRejectNegativePartitionCount() {
        final ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + 1 + 4);
        buffer.put((byte) 0);
        buffer.putInt(1);
        buffer.putInt(1);
        buffer.put((byte) 't');
        buffer.putInt(-1);
        buffer.flip();

        assertThrows(IllegalArgumentException.class, () -> PositionSerde.deserialize(buffer));
    }
}
