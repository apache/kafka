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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


import org.apache.kafka.clients.producer.internals.FutureRecordMetadata;
import org.apache.kafka.clients.producer.internals.ProduceRequestResult;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.record.Record;
import org.junit.Test;

public class RecordSendTest {

    private TopicPartition topicPartition = new TopicPartition("test", 0);
    private long baseOffset = 45;
    private long relOffset = 5;

    /**
     * Test that waiting on a request that never completes times out
     */
    @Test
    public void testTimeout() throws Exception {
        ProduceRequestResult request = new ProduceRequestResult();
        FutureRecordMetadata future = new FutureRecordMetadata(request, relOffset,
                                                               Record.NO_TIMESTAMP, 0, 0, 0);
        assertFalse("Request is not completed", future.isDone());
        try {
            future.get(5, TimeUnit.MILLISECONDS);
            fail("Should have thrown exception.");
        } catch (TimeoutException e) { /* this is good */
        }

        request.done(topicPartition, baseOffset, null);
        assertTrue(future.isDone());
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /**
     * Test that an asynchronous request will eventually throw the right exception
     */
    @Test(expected = ExecutionException.class)
    public void testError() throws Exception {
        FutureRecordMetadata future = new FutureRecordMetadata(asyncRequest(baseOffset, new CorruptRecordException(), 50L),
                                                               relOffset, Record.NO_TIMESTAMP, 0, 0, 0);
        future.get();
    }

    /**
     * Test that an asynchronous request will eventually return the right offset
     */
    @Test
    public void testBlocking() throws Exception {
        FutureRecordMetadata future = new FutureRecordMetadata(asyncRequest(baseOffset, null, 50L),
                                                               relOffset, Record.NO_TIMESTAMP, 0, 0, 0);
        assertEquals(baseOffset + relOffset, future.get().offset());
    }

    /* create a new request result that will be completed after the given timeout */
    public ProduceRequestResult asyncRequest(final long baseOffset, final RuntimeException error, final long timeout) {
        final ProduceRequestResult request = new ProduceRequestResult();
        Thread thread = new Thread() {
            public void run() {
                try {
                    sleep(timeout);
                    request.done(topicPartition, baseOffset, error);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        };
        thread.start();
        return request;
    }

}
