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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RecordSendTest {

    private final TopicPartition topicPartition = new TopicPartition("test", 0);
    private final long baseOffset = 45;
    private final long relOffset = 5;

    /**
     * Test that waiting on a request that never completes times out
     */
    @Test
    public void testTimeout() throws Exception {
        ProduceRequestResult request = new ProduceRequestResult(topicPartition);
        FutureRecordMetadata future = new FutureRecordMetadata(request, relOffset,
                RecordBatch.NO_TIMESTAMP, 0L, 0, 0, new MockTime());
        assertFalse("Request is not completed", future.isDone());
        try {
            future.get(5, TimeUnit.MILLISECONDS);
            fail("Should have thrown exception.");
        } catch (TimeoutException e) { /* this is good */
        }

        request.set(baseOffset, RecordBatch.NO_TIMESTAMP, null);
        request.done();
        assertTrue(future.isDone());
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /**
     * Test that a request will throw the right exception
     */
    @Test(expected = ExecutionException.class)
    public void testError() throws Exception {
        FutureRecordMetadata future = new FutureRecordMetadata(produceRequestResult(baseOffset, new CorruptRecordException()),
                relOffset, RecordBatch.NO_TIMESTAMP, 0L, 0, 0, new MockTime());
        future.get();
    }

    /**
     * Test that a request will return the right offset
     */
    @Test
    public void testBlocking() throws Exception {
        FutureRecordMetadata future = new FutureRecordMetadata(produceRequestResult(baseOffset, null),
                relOffset, RecordBatch.NO_TIMESTAMP, 0L, 0, 0, new MockTime());
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /* create a new completed request result */
    private ProduceRequestResult produceRequestResult(final long baseOffset, final RuntimeException error) {
        final ProduceRequestResult request = new ProduceRequestResult(topicPartition);
        request.set(baseOffset, RecordBatch.NO_TIMESTAMP, error);
        request.done();
        return request;
    }

}
