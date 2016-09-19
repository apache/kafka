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

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleRecordTest {

    /* This scenario can happen if the record size field is corrupt and we end up allocating a buffer that is too small */
    @Test
    public void testIsValidWithTooSmallBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(2);
        Record record = new Record(buffer);
        assertFalse(record.isValid());
        try {
            record.ensureValid();
            fail("InvalidRecordException should have been thrown");
        } catch (InvalidRecordException e) { }
    }

    @Test
    public void testIsValidWithChecksumMismatch() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        // set checksum
        buffer.putInt(2);
        Record record = new Record(buffer);
        assertFalse(record.isValid());
        try {
            record.ensureValid();
            fail("InvalidRecordException should have been thrown");
        } catch (InvalidRecordException e) { }
    }

    @Test
    public void testIsValidWithFourBytesBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        Record record = new Record(buffer);
        // it is a bit weird that we return `true` in this case, we could extend the definition of `isValid` to
        // something like the following to detect a clearly corrupt record:
        // return size() >= recordSize(0, 0) && checksum() == computeChecksum();
        assertTrue(record.isValid());
        // no exception should be thrown
        record.ensureValid();
    }

}
