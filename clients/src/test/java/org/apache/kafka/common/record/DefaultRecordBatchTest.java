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

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class DefaultRecordBatchTest {

    @Test
    public void testSizeInBytes() {
        Header[] headers = new Header[] {
            new Header("foo", "value".getBytes()),
            new Header("bar", Utils.wrapNullable(null))
        };

        long timestamp = System.currentTimeMillis();
        SimpleRecord[] records = new SimpleRecord[] {
            new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes()),
            new SimpleRecord(timestamp + 30000, null, "value".getBytes()),
            new SimpleRecord(timestamp + 60000, "key".getBytes(), null),
            new SimpleRecord(timestamp + 60000, "key".getBytes(), "value".getBytes(), headers)
        };
        int actualSize = MemoryRecords.withRecords(CompressionType.NONE, records).sizeInBytes();
        assertEquals(actualSize, DefaultRecordBatch.sizeInBytes(Arrays.asList(records)));
    }

}
