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
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PayloadGeneratorTest {

    @Test
    public void testGeneratorStartsAtPositionZero() {
        PayloadGenerator payloadGenerator = new PayloadGenerator();
        assertEquals(0, payloadGenerator.position());
    }

    @Test
    public void testDefaultPayload() {
        final long numRecords = 262;
        PayloadGenerator payloadGenerator = new PayloadGenerator();

        // make sure that each time we produce a different value (except if compression rate is 0)
        byte[] prevValue = null;
        long expectedPosition = 0;
        for (int i = 0; i < numRecords; i++) {
            ProducerRecord<byte[], byte[]> record = payloadGenerator.nextRecord("test-topic");
            assertNull(record.key());
            assertEquals(PayloadGenerator.DEFAULT_MESSAGE_SIZE, record.value().length);
            assertEquals(++expectedPosition, payloadGenerator.position());
            assertFalse("Position " + payloadGenerator.position(),
                        Arrays.equals(prevValue, record.value()));
            prevValue = record.value().clone();
        }
    }

    @Test
    public void testNullKeyTypeValueSizeIsMessageSize() {
        final int size = 200;
        PayloadGenerator payloadGenerator = new PayloadGenerator(size);
        ProducerRecord<byte[], byte[]> record = payloadGenerator.nextRecord("test-topic");
        assertNull(record.key());
        assertEquals(size, record.value().length);
    }

    @Test
    public void testKeyContainsGeneratorPosition() {
        final long numRecords = 10;
        final int size = 200;
        PayloadGenerator generator = new PayloadGenerator(size, PayloadKeyType.KEY_MESSAGE_INDEX);
        for (int i = 0; i < numRecords; i++) {
            assertEquals(i, generator.position());
            ProducerRecord<byte[], byte[]> record = generator.nextRecord("test-topic");
            assertEquals(8, record.key().length);
            assertEquals(size - 8, record.value().length);
            assertEquals("i=" + i, i, ByteBuffer.wrap(record.key()).getLong());
        }
    }

    @Test
    public void testGeneratePayloadWithExplicitPosition() {
        final int size = 200;
        PayloadGenerator generator = new PayloadGenerator(size, PayloadKeyType.KEY_MESSAGE_INDEX);
        int position = 2;
        while (position < 5000000) {
            ProducerRecord<byte[], byte[]> record = generator.nextRecord("test-topic", position);
            assertEquals(8, record.key().length);
            assertEquals(size - 8, record.value().length);
            assertEquals(position, ByteBuffer.wrap(record.key()).getLong());
            position = position * 64;
        }
    }

    public void testSamePositionGeneratesSameKeyAndValue() {
        final int size = 100;
        PayloadGenerator generator = new PayloadGenerator(size, PayloadKeyType.KEY_MESSAGE_INDEX);
        ProducerRecord<byte[], byte[]> record1 = generator.nextRecord("test-topic");
        assertEquals(1, generator.position());
        ProducerRecord<byte[], byte[]> record2 = generator.nextRecord("test-topic");
        assertEquals(2, generator.position());
        ProducerRecord<byte[], byte[]> record3 = generator.nextRecord("test-topic", 0);
        // position should not change if we generated record with specific position
        assertEquals(2, generator.position());
        assertFalse("Values at different positions should not match.",
                    Arrays.equals(record1.value(), record2.value()));
        assertFalse("Values at different positions should not match.",
                    Arrays.equals(record3.value(), record2.value()));
        assertTrue("Values at the same position should match.",
                   Arrays.equals(record1.value(), record3.value()));
    }

    @Test
    public void testGeneratesDeterministicKeyValues() {
        final long numRecords = 194;
        final int size = 100;
        PayloadGenerator generator1 = new PayloadGenerator(size, PayloadKeyType.KEY_MESSAGE_INDEX);
        PayloadGenerator generator2 = new PayloadGenerator(size, PayloadKeyType.KEY_MESSAGE_INDEX);
        for (int i = 0; i < numRecords; ++i) {
            ProducerRecord<byte[], byte[]> record1 = generator1.nextRecord("test-topic");
            ProducerRecord<byte[], byte[]> record2 = generator2.nextRecord("test-topic");
            assertTrue(Arrays.equals(record1.value(), record2.value()));
            assertTrue(Arrays.equals(record1.key(), record2.key()));
        }
    }

    @Test
    public void testTooSmallMessageSizeCreatesPayloadWithOneByteValues() {
        PayloadGenerator payloadGenerator = new PayloadGenerator(2, PayloadKeyType.KEY_MESSAGE_INDEX);
        ProducerRecord<byte[], byte[]> record = payloadGenerator.nextRecord("test-topic", 877);
        assertEquals(8, record.key().length);
        assertEquals(1, record.value().length);
    }

}
