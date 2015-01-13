/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.producer.internals.Metadata;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class SenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final int REQUEST_TIMEOUT_MS = 10000;

    private TopicPartition tp = new TopicPartition("test", 0);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private Cluster cluster = TestUtils.singletonCluster("test", 1);
    private Metrics metrics = new Metrics(time);
    Map<String, String> metricTags = new LinkedHashMap<String, String>();
    private RecordAccumulator accumulator = new RecordAccumulator(batchSize, 1024 * 1024, 0L, 0L, false, metrics, time, metricTags);
    private Sender sender = new Sender(client,
                                       metadata,
                                       this.accumulator,
                                       MAX_REQUEST_SIZE,
                                       ACKS_ALL,
                                       MAX_RETRIES,
                                       REQUEST_TIMEOUT_MS,
                                       metrics,
                                       time,
                                       "clientId");

    @Before
    public void setup() {
        metadata.update(cluster, time.milliseconds());
    }

    @Test
    public void testSimple() throws Exception {
        int offset = 0;
        Future<RecordMetadata> future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), CompressionType.NONE, null).future;
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        assertEquals("We should have a single produce request in flight.", 1, client.inFlightRequestCount());
        client.respond(produceResponse(tp.topic(), tp.partition(), offset, Errors.NONE.code()));
        sender.run(time.milliseconds());
        assertEquals("All requests completed.", offset, client.inFlightRequestCount());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());
        assertEquals(offset, future.get().offset());
    }

    @Test
    public void testRetries() throws Exception {
        // create a sender with retries = 1
        int maxRetries = 1;
        Sender sender = new Sender(client,
                                   metadata,
                                   this.accumulator,
                                   MAX_REQUEST_SIZE,
                                   ACKS_ALL,
                                   maxRetries,
                                   REQUEST_TIMEOUT_MS,
                                   new Metrics(),
                                   time,
                                   "clientId");
        // do a successful retry
        Future<RecordMetadata> future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), CompressionType.NONE, null).future;
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        assertEquals(1, client.inFlightRequestCount());
        client.disconnect(client.requests().peek().request().destination());
        assertEquals(0, client.inFlightRequestCount());
        sender.run(time.milliseconds()); // receive error
        sender.run(time.milliseconds()); // reconnect
        sender.run(time.milliseconds()); // resend
        assertEquals(1, client.inFlightRequestCount());
        int offset = 0;
        client.respond(produceResponse(tp.topic(), tp.partition(), offset, Errors.NONE.code()));
        sender.run(time.milliseconds());
        assertTrue("Request should have retried and completed", future.isDone());
        assertEquals(offset, future.get().offset());

        // do an unsuccessful retry
        future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), CompressionType.NONE, null).future;
        sender.run(time.milliseconds()); // send produce request
        for (int i = 0; i < maxRetries + 1; i++) {
            client.disconnect(client.requests().peek().request().destination());
            sender.run(time.milliseconds()); // receive error
            sender.run(time.milliseconds()); // reconnect
            sender.run(time.milliseconds()); // resend
        }
        sender.run(time.milliseconds());
        completedWithError(future, Errors.NETWORK_EXCEPTION);
    }

    private void completedWithError(Future<RecordMetadata> future, Errors error) throws Exception {
        assertTrue("Request should be completed", future.isDone());
        try {
            future.get();
            fail("Should have thrown an exception.");
        } catch (ExecutionException e) {
            assertEquals(error.exception().getClass(), e.getCause().getClass());
        }
    }

    private Struct produceResponse(String topic, int part, long offset, int error) {
        Struct struct = new Struct(ProtoUtils.currentResponseSchema(ApiKeys.PRODUCE.id));
        Struct response = struct.instance("responses");
        response.set("topic", topic);
        Struct partResp = response.instance("partition_responses");
        partResp.set("partition", part);
        partResp.set("error_code", (short) error);
        partResp.set("base_offset", offset);
        response.set("partition_responses", new Object[] { partResp });
        struct.set("responses", new Object[] { response });
        return struct;
    }

}
