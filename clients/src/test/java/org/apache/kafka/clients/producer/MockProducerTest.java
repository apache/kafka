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
package org.apache.kafka.clients.producer;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;

public class MockProducerTest {

    private String topic = "topic";

    @Test
    public void testAutoCompleteMock() throws Exception {
        MockProducer producer = new MockProducer(true);
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(topic, "key".getBytes(), "value".getBytes());
        Future<RecordMetadata> metadata = producer.send(record);
        assertTrue("Send should be immediately complete", metadata.isDone());
        assertFalse("Send should be successful", isError(metadata));
        assertEquals("Offset should be 0", 0, metadata.get().offset());
        assertEquals(topic, metadata.get().topic());
        assertEquals("We should have the record in our history", asList(record), producer.history());
        producer.clear();
        assertEquals("Clear should erase our history", 0, producer.history().size());
    }

    @Test
    public void testManualCompletion() throws Exception {
        MockProducer producer = new MockProducer(false);
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<byte[], byte[]>("topic", "key1".getBytes(), "value1".getBytes());
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<byte[], byte[]>("topic", "key2".getBytes(), "value2".getBytes());
        Future<RecordMetadata> md1 = producer.send(record1);
        assertFalse("Send shouldn't have completed", md1.isDone());
        Future<RecordMetadata> md2 = producer.send(record2);
        assertFalse("Send shouldn't have completed", md2.isDone());
        assertTrue("Complete the first request", producer.completeNext());
        assertFalse("Requst should be successful", isError(md1));
        assertFalse("Second request still incomplete", md2.isDone());
        IllegalArgumentException e = new IllegalArgumentException("blah");
        assertTrue("Complete the second request with an error", producer.errorNext(e));
        try {
            md2.get();
            fail("Expected error to be thrown");
        } catch (ExecutionException err) {
            assertEquals(e, err.getCause());
        }
        assertFalse("No more requests to complete", producer.completeNext());
    }

    private boolean isError(Future<?> future) {
        try {
            future.get();
            return false;
        } catch (Exception e) {
            return true;
        }
    }
}
