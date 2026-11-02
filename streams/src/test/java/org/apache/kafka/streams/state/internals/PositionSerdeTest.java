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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PositionSerdeTest {

    private static final Position POSITION = Position.emptyPosition()
        .withComponent("topic1", 0, 1L)
        .withComponent("topic1", 1, 5L)
        .withComponent("topic2", 3, 100L);

    @Test
    public void shouldSerializeToExactSize() {
        final ByteBuffer buffer = PositionSerde.serialize(POSITION);

        assertEquals(1 + 4 + 2 * (4 + 6 + 4) + 3 * (4 + 8), buffer.array().length);
        assertEquals(buffer.limit(), buffer.array().length);
        assertEquals(POSITION, PositionSerde.deserialize(ByteBuffer.wrap(buffer.array())));
    }

    @Test
    public void shouldSerializeEmptyPositionToExactSize() {
        final ByteBuffer buffer = PositionSerde.serialize(Position.emptyPosition());

        assertEquals(1 + 4, buffer.array().length);
        assertEquals(Position.emptyPosition(), PositionSerde.deserialize(ByteBuffer.wrap(buffer.array())));
    }

    @Test
    public void shouldDeserializePositionWithTrailingBytes() {
        final byte[] bytes = PositionSerde.serialize(POSITION).array();
        final byte[] padded = Arrays.copyOf(bytes, bytes.length * 8);

        assertEquals(POSITION, PositionSerde.deserialize(ByteBuffer.wrap(padded)));
    }
}
