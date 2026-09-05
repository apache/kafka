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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Optional;

class TimestampAndOffsetSerde implements Serde<TimestampAndOffset> {

    private static final int SIZE = 8 + 1 + 8; // timestamp + hasOffset flag + offset

    @Override
    public Serializer<TimestampAndOffset> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            final ByteBuffer buf = ByteBuffer.allocate(SIZE);
            buf.putLong(data.timestamp);
            if (data.offset.isPresent()) {
                buf.put((byte) 0x01);
                buf.putLong(data.offset.get());
            } else {
                buf.put((byte) 0x00);
                buf.putLong(0L); // padding
            }
            return buf.array();
        };
    }

    @Override
    public Deserializer<TimestampAndOffset> deserializer() {
        return (topic, data) -> {
            if (data == null) return null;
            final ByteBuffer buf = ByteBuffer.wrap(data);
            final long timestamp = buf.getLong();
            final boolean hasOffset = buf.get() == 0x01;
            final long offsetValue = buf.getLong();
            final Optional<Long> offset = hasOffset ? Optional.of(offsetValue) : Optional.empty();
            return new TimestampAndOffset(timestamp, offset);
        };
    }
}
