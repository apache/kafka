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

package org.apache.kafka.trogdor.workload;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;


public class PayloadTest {

    @Test
    public void testDefaultPayload() {
        ProducerPayload producerPayload = new ProducerPayload();

        // make sure that each time we produce a different value (except if compression rate is 0)
        byte[] prevValue = null;
        for (int i = 0; i < 1000; i++) {
            ProducerRecord<byte[], byte[]> record = producerPayload.nextRecord("test-topic");
            assertNull(record.key());
            assertEquals(ProducerPayload.DEFAULT_MESSAGE_SIZE, record.value().length);
            assertNotEquals("Iteration " + i, prevValue, record.value());
            prevValue = record.value().clone();
        }
    }

    @Test
    public void testNullKeyTypeValueSizeIsMessageSize() {
        final int size = 200;
        ProducerPayload producerPayload = new ProducerPayload(size);
        ProducerRecord<byte[], byte[]> record = producerPayload.nextRecord("test-topic");
        assertNull(record.key());
        assertEquals(size, record.value().length);
    }

    @Test
    public void testFixedSizeKeyContainingIntegerValue() {
        final int size = 200;
        ProducerPayload producerPayload = new ProducerPayload(size, PayloadKeyType.KEY_INTEGER);
        int keyVal = 2;
        while (keyVal < 5000000) {
            ProducerRecord<byte[], byte[]> record = producerPayload.nextRecord("test-topic", keyVal);
            assertEquals(4, record.key().length);
            assertEquals(size - 4, record.value().length);
            assertEquals(keyVal, ByteBuffer.wrap(record.key()).getInt());
            keyVal = keyVal * 64;
        }
    }

    @Test
    public void testTooSmallMessageSizeCreatesPayloadWithOneByteValues() {
        ProducerPayload producerPayload = new ProducerPayload(2, PayloadKeyType.KEY_INTEGER);
        ProducerRecord<byte[], byte[]> record = producerPayload.nextRecord("test-topic", 877);
        assertEquals(4, record.key().length);
        assertEquals(1, record.value().length);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNextRecordWithTopicOnlyFailsIfKeyTypeNotNull() {
        ProducerPayload producerPayload = new ProducerPayload(100, PayloadKeyType.KEY_INTEGER);
        producerPayload.nextRecord("test-topic");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNextRecordWithIntKeyFailsIfKeyTypeNull() {
        ProducerPayload producerPayload = new ProducerPayload(100);
        producerPayload.nextRecord("test-topic", 27);
    }
}
