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

import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RecordsBuilderTest {

    @Test
    public void testOffsetUpdated() {
        MemoryRecords records = new RecordsBuilder(15L)
                .addBatch(new SimpleRecord("foo".getBytes()))
                .addBatch(new SimpleRecord("bar".getBytes()), new SimpleRecord("baz".getBytes()))
                .build();

        List<Record> list = Utils.toList(records.records().iterator());
        assertEquals(3, list.size());
        assertEquals(15L, list.get(0).offset());
        assertEquals("foo", Utils.utf8(list.get(0).value()));
        assertEquals(16L, list.get(1).offset());
        assertEquals("bar", Utils.utf8(list.get(1).value()));
        assertEquals(17L, list.get(2).offset());
        assertEquals("baz", Utils.utf8(list.get(2).value()));

        List<MutableRecordBatch> batches = Utils.toList(records.batches().iterator());
        assertEquals(2, batches.size());
        assertEquals(15L, batches.get(0).baseOffset());
        assertEquals(15L, batches.get(0).lastOffset());
        assertEquals(16L, batches.get(1).baseOffset());
        assertEquals(17L, batches.get(1).lastOffset());
    }

    @Test
    public void testBatchOverrides() {
        RecordsBuilder recordsBuilder = new RecordsBuilder();

        RecordsBuilder.BatchBuilder batchMagic0 = recordsBuilder.newBatch().withMagic(RecordBatch.MAGIC_VALUE_V0);
        batchMagic0.append(new SimpleRecord("foo".getBytes()));
        batchMagic0.closeBatch();

        RecordsBuilder.BatchBuilder batchMagic1 = recordsBuilder.newBatch().withMagic(RecordBatch.MAGIC_VALUE_V1);
        batchMagic1.append(new SimpleRecord("bar".getBytes()));
        batchMagic1.closeBatch();

        RecordsBuilder.BatchBuilder batchMagic2 = recordsBuilder.newBatch().withMagic(RecordBatch.MAGIC_VALUE_V2);
        batchMagic2.append(new SimpleRecord("baz".getBytes()));
        batchMagic2.closeBatch();

        MemoryRecords records = recordsBuilder.build();
        List<MutableRecordBatch> batches = Utils.toList(records.batches().iterator());
        assertEquals(3, batches.size());
        assertEquals(RecordBatch.MAGIC_VALUE_V0, batches.get(0).magic());
        assertEquals(RecordBatch.MAGIC_VALUE_V1, batches.get(1).magic());
        assertEquals(RecordBatch.MAGIC_VALUE_V2, batches.get(2).magic());
    }

    @Test
    public void testEmptyRecords() {
        MemoryRecords records = new RecordsBuilder().build();
        assertEquals(0, records.sizeInBytes());
    }


}