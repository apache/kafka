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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(value = Parameterized.class)
public class MemoryRecordsTest {

    private CompressionType compression;

    public MemoryRecordsTest(CompressionType compression) {
        this.compression = compression;
    }

    @Test
    public void testIterator() {
        MemoryRecords recs1 = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), compression);
        MemoryRecords recs2 = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), compression);
        List<Record> list = Arrays.asList(new Record(0L, "a".getBytes(), "1".getBytes()),
                                          new Record(0L, "b".getBytes(), "2".getBytes()),
                                          new Record(0L, "c".getBytes(), "3".getBytes()));
        for (int i = 0; i < list.size(); i++) {
            Record r = list.get(i);
            recs1.append(i, r);
            recs2.append(i, 0L, toArray(r.key()), toArray(r.value()));
        }
        recs1.close();
        recs2.close();

        for (int iteration = 0; iteration < 2; iteration++) {
            for (MemoryRecords recs : Arrays.asList(recs1, recs2)) {
                Iterator<LogEntry> iter = recs.iterator();
                for (int i = 0; i < list.size(); i++) {
                    assertTrue(iter.hasNext());
                    LogEntry entry = iter.next();
                    assertEquals((long) i, entry.offset());
                    assertEquals(list.get(i), entry.record());
                    entry.record().ensureValid();
                }
                assertFalse(iter.hasNext());
            }
        }
    }

    @Test
    public void testHasRoomForMethod() {
        MemoryRecords recs1 = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), compression);
        recs1.append(0, new Record(0L, "a".getBytes(), "1".getBytes()));

        assertTrue(recs1.hasRoomFor("b".getBytes(), "2".getBytes()));
        recs1.close();
        assertFalse(recs1.hasRoomFor("b".getBytes(), "2".getBytes()));

    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<Object[]>();
        for (CompressionType type: CompressionType.values())
            values.add(new Object[] {type});
        return values;
    }
}
