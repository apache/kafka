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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class MirrorRecordVersionTest {

    @Test
    public void checkpointDeserializeRejectsUnsupportedValueVersion() {
        Checkpoint checkpoint = new Checkpoint("group", new TopicPartition("topic", 0), 10L, 20L, "metadata");
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "checkpoints",
                0,
                0,
                checkpoint.recordKey(),
                checkpointValueWithVersion((short) (Checkpoint.VERSION + 1))
        );

        assertThrows(UnsupportedVersionException.class, () -> Checkpoint.deserializeRecord(record));
    }

    @Test
    public void heartbeatDeserializeRejectsUnsupportedValueVersion() {
        Heartbeat heartbeat = new Heartbeat("source", "target", 123L);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "heartbeats",
                0,
                0,
                heartbeat.recordKey(),
                heartbeatValueWithVersion((short) (Heartbeat.VERSION + 1))
        );

        assertThrows(UnsupportedVersionException.class, () -> Heartbeat.deserializeRecord(record));
    }

    private static byte[] checkpointValueWithVersion(short version) {
        Struct header = new Struct(Checkpoint.HEADER_SCHEMA);
        header.set(Checkpoint.VERSION_KEY, version);
        Struct value = new Struct(Checkpoint.VALUE_SCHEMA_V0);
        value.set(Checkpoint.UPSTREAM_OFFSET_KEY, 10L);
        value.set(Checkpoint.DOWNSTREAM_OFFSET_KEY, 20L);
        value.set(Checkpoint.METADATA_KEY, "metadata");
        return write(Checkpoint.HEADER_SCHEMA, header, Checkpoint.VALUE_SCHEMA_V0, value);
    }

    private static byte[] heartbeatValueWithVersion(short version) {
        Struct header = new Struct(Heartbeat.HEADER_SCHEMA);
        header.set(Heartbeat.VERSION_KEY, version);
        Struct value = new Struct(Heartbeat.VALUE_SCHEMA_V0);
        value.set(Heartbeat.TIMESTAMP_KEY, 123L);
        return write(Heartbeat.HEADER_SCHEMA, header, Heartbeat.VALUE_SCHEMA_V0, value);
    }

    private static byte[] write(Schema headerSchema, Struct header, Schema valueSchema, Struct value) {
        ByteBuffer buffer = ByteBuffer.allocate(headerSchema.sizeOf(header) + valueSchema.sizeOf(value));
        headerSchema.write(buffer, header);
        valueSchema.write(buffer, value);
        buffer.flip();
        byte[] result = new byte[buffer.remaining()];
        buffer.get(result);
        return result;
    }
}
