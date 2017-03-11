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
package org.apache.kafka.common.record;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class EosLogEntryTest {

    @Test
    public void testSizeInBytes() {
        long timestamp = System.currentTimeMillis();
        KafkaRecord[] records = new KafkaRecord[] {
            new KafkaRecord(timestamp, "key".getBytes(), "value".getBytes()),
            new KafkaRecord(timestamp + 30000, null, "value".getBytes()),
            new KafkaRecord(timestamp + 60000, "key".getBytes(), null)
        };
        int actualSize = MemoryRecords.withRecords(CompressionType.NONE, records).sizeInBytes();
        assertEquals(actualSize, EosLogEntry.sizeInBytes(Arrays.asList(records)));
    }

}
