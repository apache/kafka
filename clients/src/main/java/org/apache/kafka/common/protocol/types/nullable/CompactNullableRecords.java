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
package org.apache.kafka.common.protocol.types.nullable;

import org.apache.kafka.common.protocol.types.CompactRecords;
import org.apache.kafka.common.record.BaseRecords;
import org.apache.kafka.common.record.MemoryRecords;

import java.nio.ByteBuffer;

public class CompactNullableRecords extends CompactRecords {
    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public void write(ByteBuffer buffer, Object o) {
        if (o == null) {
            COMPACT_NULLABLE_BYTES.write(buffer, null);
            return;
        } 
        super.write(buffer, o);
    }

    @Override
    public MemoryRecords read(ByteBuffer buffer) {
        ByteBuffer recordsBuffer = (ByteBuffer) COMPACT_NULLABLE_BYTES.read(buffer);
        if (recordsBuffer == null)
            return null;
        return MemoryRecords.readableRecords(recordsBuffer);
    }

    @Override
    public int sizeOf(Object o) {
        if (o == null) {
            return 1;
        }

        return super.sizeOf(o);
    }

    @Override
    public String typeName() {
        return "COMPACT_NULLABLE_RECORDS";
    }

    @Override
    public BaseRecords validate(Object item) {
        if (item == null)
            return null;

        return super.validate(item);
    }

    @Override
    public String documentation() {
        return "Represents a sequence of Kafka records as " + COMPACT_NULLABLE_BYTES + ". " +
            "For a detailed description of records see " +
            "<a href=\"/documentation/#messageformat\">Message Sets</a>.";
    }
}
