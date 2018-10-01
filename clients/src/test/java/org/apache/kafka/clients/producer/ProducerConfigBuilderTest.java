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

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.metrics.FakeMetricsReporter;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class ProducerConfigBuilderTest {

    @Test
    public void builderAllConfigurations() {
        List<String> bootStrapServers = Collections.singletonList("localhost:9999");
        List<Class<?>> interceptorClasses = Collections.singletonList(ProducerInterceptor.class);
        List<Class<?>> metricsReporterClasses =Collections.singletonList(FakeMetricsReporter.class);
        Map<String, Object> properties = ProducerConfig.builder()
                .bootstrapServers(bootStrapServers)
                .acks("all")
                .batchSize(3)
                .bufferMemory(387623832L)
                .clientId("clientId")
                .compressionType("compressionType")
                .connectionsMaxIdle(3000L)
                .deliveryTimeout(4321)
                .enableIdempotence(true)
                .interceptorClasses(interceptorClasses)
                .keySerializerClass(StringSerializer.class)
                .linger(1234)
                .maxBlock(5000L)
                .maxInFlightRequestsPerConnection(100)
                .maxRequestSize(356880)
                .metadataMaxAge(9876L)
                .metricsNumSamples(9)
                .metricsRecordingLevel("DEBUG")
                .metricsReporterClasses(metricsReporterClasses)
                .partitionerClass(DefaultPartitioner.class)
                .metricsSampleWindow(2468L)
                .property("key", "value")
                .receiveBuffer(7)
                .reconnectBackoff(1000L)
                .reconnectBackoffMax(3000L)
                .requestTimeout(6000)
                .retries(3)
                .retryBackoff(1500L)
                .sendBuffer(8)
                .transactionalId("t-id")
                .transactionalTimeout(9000L)
                .valueSerializerClass(StringSerializer.class)
                .build();

        assertEquals(properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), bootStrapServers);
        assertEquals(properties.get(ProducerConfig.ACKS_CONFIG), "all");
        assertEquals(properties.get(ProducerConfig.BATCH_SIZE_CONFIG), 3);
        assertEquals(properties.get(ProducerConfig.BUFFER_MEMORY_CONFIG), 387623832L);
        assertEquals(properties.get(ProducerConfig.CLIENT_ID_CONFIG), "clientId");
        assertEquals(properties.get(ProducerConfig.COMPRESSION_TYPE_CONFIG), "compressionType");
        assertEquals(properties.get(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), 3000L);
        assertEquals(properties.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), 4321);
        assertEquals(properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);
        assertEquals(properties.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG), Collections.singletonList(ProducerInterceptor.class.getName()));
        assertEquals(properties.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), StringSerializer.class.getName());
        assertEquals(properties.get(ProducerConfig.LINGER_MS_CONFIG), 1234);
        assertEquals(properties.get(ProducerConfig.MAX_BLOCK_MS_CONFIG), 5000L);
        assertEquals(properties.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION), 100);
        assertEquals(properties.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), 356880);
        assertEquals(properties.get(ProducerConfig.METADATA_MAX_AGE_CONFIG), 9876L);
        assertEquals(properties.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG), 9);
        assertEquals(properties.get(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG), "DEBUG");
        assertEquals(properties.get(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG),  Collections.singletonList(FakeMetricsReporter.class.getName()));
        assertEquals(properties.get(ProducerConfig.PARTITIONER_CLASS_CONFIG), DefaultPartitioner.class.getName());
        assertEquals(properties.get(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), 2468L);
        assertEquals(properties.get("key"), "value");
        assertEquals(properties.get(ProducerConfig.RECEIVE_BUFFER_CONFIG), 7);
        assertEquals(properties.get(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG), 1000L);
        assertEquals(properties.get(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG), 3000L);
        assertEquals(properties.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), 6000);
        assertEquals(properties.get(ProducerConfig.RETRIES_CONFIG), 3);
        assertEquals(properties.get(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), 1500L);
        assertEquals(properties.get(ProducerConfig.SEND_BUFFER_CONFIG), 8);
        assertEquals(properties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG), "t-id");
        assertEquals(properties.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 9000L);
        assertEquals(properties.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), StringSerializer.class.getName());
    }

}
