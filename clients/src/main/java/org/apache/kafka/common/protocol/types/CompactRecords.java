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
package org.apache.kafka.common.protocol.types;

import org.apache.kafka.common.protocol.types.Type.DocumentedType;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.ByteUtils;

import java.nio.ByteBuffer;

public class CompactRecords extends DocumentedType {
    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o instanceof MemoryRecords) {
            MemoryRecords records = (MemoryRecords) o;
            COMPACT_BYTES.write(buffer, records.buffer().duplicate());
        } else {
            throw new IllegalArgumentException("Unexpected record type: " + o.getClass());
        }
    }

    @Override
    public MemoryRecords read(ByteBuffer buffer) {
        ByteBuffer recordsBuffer = (ByteBuffer) COMPACT_BYTES.read(buffer);
        return MemoryRecords.readableRecords(recordsBuffer);
    }

    @Override
    public int sizeOf(Object o) {
        BaseRecords records = (BaseRecords) o;
        int recordsSize = records.sizeInBytes();
        return ByteUtils.sizeOfUnsignedVarint(recordsSize + 1) + recordsSize;
    }

    @Override
    public String typeName() {
        return "COMPACT_RECORDS";
    }

    @Override
    public BaseRecords validate(Object item) {
        if (item instanceof BaseRecords)
            return (BaseRecords) item;

        throw new SchemaException(item + " is not an instance of " + BaseRecords.class.getName());
    }

    @Override
    public String documentation() {
        return "Represents a sequence of Kafka records as " + COMPACT_BYTES + ". " +
            "For a detailed description of records see " +
            "<a href=\"/documentation/#messageformat\">Message Sets</a>.";
    }
}
