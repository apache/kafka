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
package org.apache.kafka.raft;

import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RaftUtil {

    public static ByteBuffer serializeRecords(Records records) throws IOException {
        if (records instanceof MemoryRecords) {
            MemoryRecords memoryRecords = (MemoryRecords) records;
            return memoryRecords.buffer();
        } else if (records instanceof FileRecords) {
            FileRecords fileRecords = (FileRecords) records;
            ByteBuffer buffer = ByteBuffer.allocate(fileRecords.sizeInBytes());
            fileRecords.readInto(buffer, 0);
            return buffer;
        } else {
            throw new UnsupportedOperationException("Serialization not yet supported for " + records.getClass());
        }
    }

}
