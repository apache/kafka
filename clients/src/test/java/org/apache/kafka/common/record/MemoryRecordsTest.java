/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static org.apache.kafka.common.utils.Utils.toArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.junit.Test;

public class MemoryRecordsTest {

    @Test
    public void testIterator() {
        MemoryRecords recs1 = new MemoryRecords(ByteBuffer.allocate(1024));
        MemoryRecords recs2 = new MemoryRecords(ByteBuffer.allocate(1024));
        List<Record> list = Arrays.asList(new Record("a".getBytes(), "1".getBytes()),
                                          new Record("b".getBytes(), "2".getBytes()),
                                          new Record("c".getBytes(), "3".getBytes()));
        for (int i = 0; i < list.size(); i++) {
            Record r = list.get(i);
            recs1.append(i, r);
            recs2.append(i, toArray(r.key()), toArray(r.value()), r.compressionType());
        }

        for (int iteration = 0; iteration < 2; iteration++) {
            for (MemoryRecords recs : Arrays.asList(recs1, recs2)) {
                Iterator<LogEntry> iter = recs.iterator();
                for (int i = 0; i < list.size(); i++) {
                    assertTrue(iter.hasNext());
                    LogEntry entry = iter.next();
                    assertEquals((long) i, entry.offset());
                    assertEquals(list.get(i), entry.record());
                }
                assertFalse(iter.hasNext());
            }
        }
    }

}
