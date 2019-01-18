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

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.HashMap;
import java.nio.ByteBuffer;

public class Heartbeat {
    public static final String SOURCE_CLUSTER_ALIAS_KEY = "sourceClusterAlias";
    public static final String TARGET_CLUSTER_ALIAS_KEY = "targetClusterAlias";
    public static final String TIMESTAMP_KEY = "timestamp";

    public static final Schema VALUE_SCHEMA = new Schema(
            new Field(TIMESTAMP_KEY, Type.INT64));

    public static final Schema KEY_SCHEMA = new Schema(
            new Field(SOURCE_CLUSTER_ALIAS_KEY, Type.STRING),
            new Field(TARGET_CLUSTER_ALIAS_KEY, Type.STRING));

    private String sourceClusterAlias;
    private String targetClusterAlias;
    private long timestamp;

    public Heartbeat(String sourceClusterAlias, String targetClusterAlias, long timestamp) {
        this.sourceClusterAlias = sourceClusterAlias;
        this.targetClusterAlias = targetClusterAlias;
        this.timestamp = timestamp;
    }

    public String sourceClusterAlias() {
        return sourceClusterAlias;
    }

    public String targetClusterAlias() {
        return targetClusterAlias;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("Heartbeat{sourceClusterAlias=%s, targetClusterAlias=%s, timestamp=%d}",
            sourceClusterAlias, targetClusterAlias, timestamp);
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

    static Heartbeat deserializeRecord(ConsumerRecord<byte[], byte[]> record) {
        Struct keyStruct = KEY_SCHEMA.read(ByteBuffer.wrap(record.key()));
        String sourceClusterAlias = keyStruct.getString(SOURCE_CLUSTER_ALIAS_KEY);
        String targetClusterAlias = keyStruct.getString(TARGET_CLUSTER_ALIAS_KEY);
        
        Struct valueStruct = VALUE_SCHEMA.read(ByteBuffer.wrap(record.value()));
        long timestamp = valueStruct.getLong(TIMESTAMP_KEY);
    
        return new Heartbeat(sourceClusterAlias, targetClusterAlias, timestamp);    
    } 

    protected Struct valueStruct() {
        Struct struct = new Struct(VALUE_SCHEMA);
        struct.set(TIMESTAMP_KEY, timestamp);
        return struct;
    }

    protected Struct keyStruct() {
        Struct struct = new Struct(KEY_SCHEMA);
        struct.set(SOURCE_CLUSTER_ALIAS_KEY, sourceClusterAlias);
        struct.set(TARGET_CLUSTER_ALIAS_KEY, targetClusterAlias);
        return struct;
    }

    Map<String, ?> connectPartition() {
        Map<String, Object> partition = new HashMap<>();
        partition.put(SOURCE_CLUSTER_ALIAS_KEY, sourceClusterAlias);
        partition.put(TARGET_CLUSTER_ALIAS_KEY, targetClusterAlias);
        return partition;
    }

    byte[] recordKey() {
        return serializeKey().array();
    }

    byte[] recordValue() {
        return serializeValue().array();
    }
};

