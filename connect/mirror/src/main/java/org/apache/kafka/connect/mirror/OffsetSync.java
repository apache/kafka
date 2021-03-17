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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.ByteBuffer;

public class OffsetSync {
    public static final String TOPIC_KEY = "topic";
    public static final String PARTITION_KEY = "partition";
    public static final String UPSTREAM_OFFSET_KEY = "upstreamOffset";
    public static final String DOWNSTREAM_OFFSET_KEY = "offset";

    public static final Schema VALUE_SCHEMA = new Schema(
            new Field(UPSTREAM_OFFSET_KEY, Type.INT64),
            new Field(DOWNSTREAM_OFFSET_KEY, Type.INT64));

    public static final Schema KEY_SCHEMA = new Schema(
            new Field(TOPIC_KEY, Type.STRING),
            new Field(PARTITION_KEY, Type.INT32));

    private TopicPartition topicPartition;
    private long upstreamOffset;
    private long downstreamOffset;

    public OffsetSync(TopicPartition topicPartition, long upstreamOffset, long downstreamOffset) {
        this.topicPartition = topicPartition;
        this.upstreamOffset = upstreamOffset;
        this.downstreamOffset = downstreamOffset;
    }

    public TopicPartition topicPartition() {
        return topicPartition;
    }

    public long upstreamOffset() {
        return upstreamOffset;
    }

    public long downstreamOffset() {
        return downstreamOffset;
    }

    @Override
    public String toString() {
        return String.format("OffsetSync{topicPartition=%s, upstreamOffset=%d, downstreamOffset=%d}",
            topicPartition, upstreamOffset, downstreamOffset);
    }

    ByteBuffer serializeValue() {
        Struct struct = valueStruct();
        ByteBuffer buffer = ByteBuffer.allocate(VALUE_SCHEMA.sizeOf(struct));
        VALUE_SCHEMA.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    ByteBuffer serializeKey() {
        Struct struct = keyStruct();
        ByteBuffer buffer = ByteBuffer.allocate(KEY_SCHEMA.sizeOf(struct));
        KEY_SCHEMA.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    public static OffsetSync deserializeRecord(ConsumerRecord<byte[], byte[]> record) {
        Struct keyStruct = KEY_SCHEMA.read(ByteBuffer.wrap(record.key()));
        String topic = keyStruct.getString(TOPIC_KEY);
        int partition = keyStruct.getInt(PARTITION_KEY);

        Struct valueStruct = VALUE_SCHEMA.read(ByteBuffer.wrap(record.value()));
        long upstreamOffset = valueStruct.getLong(UPSTREAM_OFFSET_KEY);
        long downstreamOffset = valueStruct.getLong(DOWNSTREAM_OFFSET_KEY);

        return new OffsetSync(new TopicPartition(topic, partition), upstreamOffset, downstreamOffset);
    }

    private Struct valueStruct() {
        Struct struct = new Struct(VALUE_SCHEMA);
        struct.set(UPSTREAM_OFFSET_KEY, upstreamOffset);
        struct.set(DOWNSTREAM_OFFSET_KEY, downstreamOffset);
        return struct;
    }

    private Struct keyStruct() {
        Struct struct = new Struct(KEY_SCHEMA);
        struct.set(TOPIC_KEY, topicPartition.topic());
        struct.set(PARTITION_KEY, topicPartition.partition());
        return struct;
    }

    byte[] recordKey() {
        return serializeKey().array();
    }

    byte[] recordValue() {
        return serializeValue().array();
    }
}

