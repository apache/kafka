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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;

/** Checkpoint records emitted from MirrorCheckpointConnector. Encodes remote consumer group state. */
public class Checkpoint {
    public static final String TOPIC_KEY = "topic";
    public static final String PARTITION_KEY = "partition";
    public static final String CONSUMER_GROUP_ID_KEY = "group";
    public static final String UPSTREAM_OFFSET_KEY = "upstreamOffset";
    public static final String DOWNSTREAM_OFFSET_KEY = "offset";
    public static final String METADATA_KEY = "metadata";
    public static final String VERSION_KEY = "version";
    public static final short VERSION = 0;

    public static final Schema VALUE_SCHEMA_V0 = new Schema(
            new Field(UPSTREAM_OFFSET_KEY, Type.INT64),
            new Field(DOWNSTREAM_OFFSET_KEY, Type.INT64),
            new Field(METADATA_KEY, Type.STRING));

    public static final Schema KEY_SCHEMA = new Schema(
            new Field(CONSUMER_GROUP_ID_KEY, Type.STRING),
            new Field(TOPIC_KEY, Type.STRING),
            new Field(PARTITION_KEY, Type.INT32));

    public static final Schema HEADER_SCHEMA = new Schema(
            new Field(VERSION_KEY, Type.INT16));

    private String consumerGroupId;
    private TopicPartition topicPartition;
    private long upstreamOffset;
    private long downstreamOffset;
    private String metadata;

    public Checkpoint(String consumerGroupId, TopicPartition topicPartition, long upstreamOffset,
            long downstreamOffset, String metadata) {
        this.consumerGroupId = consumerGroupId;
        this.topicPartition = topicPartition;
        this.upstreamOffset = upstreamOffset;
        this.downstreamOffset = downstreamOffset;
        this.metadata = metadata;
    }

    public String consumerGroupId() {
        return consumerGroupId;
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

    public String metadata() {
        return metadata;
    }

    public OffsetAndMetadata offsetAndMetadata() {
        return new OffsetAndMetadata(downstreamOffset, metadata);
    }

    @Override
    public String toString() {
        return String.format("Checkpoint{consumerGroupId=%s, topicPartition=%s, "
            + "upstreamOffset=%d, downstreamOffset=%d, metatadata=%s}",
            consumerGroupId, topicPartition, upstreamOffset, downstreamOffset, metadata);
    }

    ByteBuffer serializeValue(short version) {
        Struct header = headerStruct(version);
        Schema valueSchema = valueSchema(version);
        Struct valueStruct = valueStruct(valueSchema);
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SCHEMA.sizeOf(header) + valueSchema.sizeOf(valueStruct));
        HEADER_SCHEMA.write(buffer, header);
        valueSchema.write(buffer, valueStruct);
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

    public static Checkpoint deserializeRecord(ConsumerRecord<byte[], byte[]> record) {
        ByteBuffer value = ByteBuffer.wrap(record.value());
        Struct header = HEADER_SCHEMA.read(value);
        short version = header.getShort(VERSION_KEY);
        Schema valueSchema = valueSchema(version);
        Struct valueStruct = valueSchema.read(value);
        long upstreamOffset = valueStruct.getLong(UPSTREAM_OFFSET_KEY);
        long downstreamOffset = valueStruct.getLong(DOWNSTREAM_OFFSET_KEY);
        String metadata = valueStruct.getString(METADATA_KEY);
        Struct keyStruct = KEY_SCHEMA.read(ByteBuffer.wrap(record.key()));
        String group = keyStruct.getString(CONSUMER_GROUP_ID_KEY);
        String topic = keyStruct.getString(TOPIC_KEY);
        int partition = keyStruct.getInt(PARTITION_KEY);
        return new Checkpoint(group, new TopicPartition(topic, partition), upstreamOffset,
            downstreamOffset, metadata);
    }

    private static Schema valueSchema(short version) {
        assert version == 0;
        return VALUE_SCHEMA_V0;
    }

    private Struct valueStruct(Schema schema) {
        Struct struct = new Struct(schema);
        struct.set(UPSTREAM_OFFSET_KEY, upstreamOffset);
        struct.set(DOWNSTREAM_OFFSET_KEY, downstreamOffset);
        struct.set(METADATA_KEY, metadata);
        return struct;
    }

    private Struct keyStruct() {
        Struct struct = new Struct(KEY_SCHEMA);
        struct.set(CONSUMER_GROUP_ID_KEY, consumerGroupId);
        struct.set(TOPIC_KEY, topicPartition.topic());
        struct.set(PARTITION_KEY, topicPartition.partition());
        return struct;
    }

    private Struct headerStruct(short version) {
        Struct struct = new Struct(HEADER_SCHEMA);
        struct.set(VERSION_KEY, version);
        return struct;
    }

    Map<String, ?> connectPartition() {
        Map<String, Object> partition = new HashMap<>();
        partition.put(CONSUMER_GROUP_ID_KEY, consumerGroupId);
        partition.put(TOPIC_KEY, topicPartition.topic());
        partition.put(PARTITION_KEY, topicPartition.partition());
        return partition;
    }

    static String unwrapGroup(Map<String, ?> connectPartition) {
        return connectPartition.get(CONSUMER_GROUP_ID_KEY).toString();
    }

    byte[] recordKey() {
        return serializeKey().array();
    }

    byte[] recordValue() {
        return serializeValue(VERSION).array();
    }
}

