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
package org.apache.kafka.clients.producer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SenderTest {

    private static final int MAX_REQUEST_SIZE = 1024 * 1024;
    private static final short ACKS_ALL = -1;
    private static final int MAX_RETRIES = 0;
    private static final String CLIENT_ID = "clientId";
    private static final String METRIC_GROUP = "producer-metrics";
    private static final double EPS = 0.0001;
    private static final int MAX_BLOCK_TIMEOUT = 1000;
    private static final int REQUEST_TIMEOUT = 1000;

    private TopicPartition tp = new TopicPartition("test", 0);
    private MockTime time = new MockTime();
    private MockClient client = new MockClient(time);
    private int batchSize = 16 * 1024;
    private Metadata metadata = new Metadata(0, Long.MAX_VALUE);
    private Cluster cluster = TestUtils.singletonCluster("test", 1);
    private Metrics metrics = null;
    private RecordAccumulator accumulator = null;
    private Sender sender = null;

    @Before
    public void setup() {
        Map<String, String> metricTags = new LinkedHashMap<String, String>();
        metricTags.put("client-id", CLIENT_ID);
        MetricConfig metricConfig = new MetricConfig().tags(metricTags);
        metrics = new Metrics(metricConfig, time);
        accumulator = new RecordAccumulator(batchSize, 1024 * 1024, CompressionType.NONE, 0L, 0L, false, metrics, time);
        sender = new Sender(client,
                            metadata,
                            this.accumulator,
                            MAX_REQUEST_SIZE,
                            ACKS_ALL,
                            MAX_RETRIES,
                            metrics,
                            time,
                            CLIENT_ID,
                            REQUEST_TIMEOUT);

        metadata.update(cluster, time.milliseconds());
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    @Test
    public void testSimple() throws Exception {
        long offset = 0;
        Future<RecordMetadata> future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
        sender.run(time.milliseconds()); // connect
        sender.run(time.milliseconds()); // send produce request
        assertEquals("We should have a single produce request in flight.", 1, client.inFlightRequestCount());
        client.respond(produceResponse(tp, offset, Errors.NONE.code(), 0));
        sender.run(time.milliseconds());
        assertEquals("All requests completed.", offset, (long) client.inFlightRequestCount());
        sender.run(time.milliseconds());
        assertTrue("Request should be completed", future.isDone());
        assertEquals(offset, future.get().offset());
    }

    /*
     * Send multiple requests. Verify that the client side quota metrics have the right values
     */
    @Test
    public void testQuotaMetrics() throws Exception {
        final long offset = 0;
        for (int i = 1; i <= 3; i++) {
            Future<RecordMetadata> future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // send produce request
            client.respond(produceResponse(tp, offset, Errors.NONE.code(), 100 * i));
            sender.run(time.milliseconds());
        }
        Map<MetricName, KafkaMetric> allMetrics = metrics.metrics();
        KafkaMetric avgMetric = allMetrics.get(metrics.metricName("produce-throttle-time-avg", METRIC_GROUP, ""));
        KafkaMetric maxMetric = allMetrics.get(metrics.metricName("produce-throttle-time-max", METRIC_GROUP, ""));
        assertEquals(200, avgMetric.value(), EPS);
        assertEquals(300, maxMetric.value(), EPS);
    }

    @Test
    public void testRetries() throws Exception {
        // create a sender with retries = 1
        int maxRetries = 1;
        Metrics m = new Metrics();
        try {
            Sender sender = new Sender(client,
                                       metadata,
                                       this.accumulator,
                                       MAX_REQUEST_SIZE,
                                       ACKS_ALL,
                                       maxRetries,
                                       m,
                                       time,
                                       "clientId",
                                       REQUEST_TIMEOUT);
            // do a successful retry
            Future<RecordMetadata> future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // connect
            sender.run(time.milliseconds()); // send produce request
            String id = client.requests().peek().request().destination();
            Node node = new Node(Integer.valueOf(id), "localhost", 0);
            assertEquals(1, client.inFlightRequestCount());
            assertTrue("Client ready status should be true", client.isReady(node, 0L));
            client.disconnect(id);
            assertEquals(0, client.inFlightRequestCount());
            assertFalse("Client ready status should be false", client.isReady(node, 0L));
            sender.run(time.milliseconds()); // receive error
            sender.run(time.milliseconds()); // reconnect
            sender.run(time.milliseconds()); // resend
            assertEquals(1, client.inFlightRequestCount());
            long offset = 0;
            client.respond(produceResponse(tp, offset, Errors.NONE.code(), 0));
            sender.run(time.milliseconds());
            assertTrue("Request should have retried and completed", future.isDone());
            assertEquals(offset, future.get().offset());

            // do an unsuccessful retry
            future = accumulator.append(tp, "key".getBytes(), "value".getBytes(), null, MAX_BLOCK_TIMEOUT).future;
            sender.run(time.milliseconds()); // send produce request
            for (int i = 0; i < maxRetries + 1; i++) {
                client.disconnect(client.requests().peek().request().destination());
                sender.run(time.milliseconds()); // receive error
                sender.run(time.milliseconds()); // reconnect
                sender.run(time.milliseconds()); // resend
            }
            sender.run(time.milliseconds());
            completedWithError(future, Errors.NETWORK_EXCEPTION);
        } finally {
            m.close();
        }
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

    private Struct produceResponse(TopicPartition tp, long offset, int error, int throttleTimeMs) {
        ProduceResponse.PartitionResponse resp = new ProduceResponse.PartitionResponse((short) error, offset);
        Map<TopicPartition, ProduceResponse.PartitionResponse> partResp = Collections.singletonMap(tp, resp);
        ProduceResponse response = new ProduceResponse(partResp, throttleTimeMs);
        return response.toStruct();
    }

}
