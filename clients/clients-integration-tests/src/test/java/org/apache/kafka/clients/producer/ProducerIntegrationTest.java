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

import kafka.utils.TestUtils;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.common.compress.NoCompression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


public class ProducerIntegrationTest {

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testInFlightBatchShouldNotBeCorrupted(ClusterInstance cluster) throws InterruptedException,
        ExecutionException {
        String topic = "test-topic";
        cluster.createTopic("test-topic", 1, (short) 1);
        try (var producer = expireProducer(cluster)) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes())).get();
        }
        try (var consumer = cluster.consumer()) {
            consumer.subscribe(List.of(topic));
            TestUtils.waitUntilTrue(() -> consumer.poll(Duration.ofSeconds(1)).count() == 1, () -> "failed to poll data", 5000, 100L);
        }
    }


    @SuppressWarnings({"unchecked", "this-escape"})
    private Producer<byte[], byte[]> expireProducer(ClusterInstance cluster) {
        Map<String, Object> config = Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 2000,
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1500
        );
        return new EvilKafkaProducerBuilder().build(config);
    }

    static class EvilKafkaProducerBuilder {

        Serializer<byte[]> serializer = new ByteArraySerializer();
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = new LogContext("[expire Producer test ]");
        Metrics metrics = new Metrics(Time.SYSTEM);

        String clientId;
        String transactionalId;
        ProducerConfig config;
        ProducerMetadata metadata;
        RecordAccumulator accumulator;
        Partitioner partitioner;
        Sender sender;
        ProducerInterceptors<String, String> interceptors;

        @SuppressWarnings({"unchecked", "this-escape"})
        Producer<byte[], byte[]> build(Map<String, Object> configs) {
            this.config = new ProducerConfig(ProducerConfig.appendSerializerToConfig(configs, null, null));
            transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            return new KafkaProducer<>(
                config,
                logContext,
                metrics,
                serializer,
                serializer,
                buildMetadata(),
                buildAccumulator(),
                null,
                buildSender(),
                buildInterceptors(),
                buildPartition(),
                Time.SYSTEM,
                ioThread(),
                Optional.empty()
            );
        }


        private ProducerInterceptors buildInterceptors() {
            this.interceptors = new ProducerInterceptors<>(List.of(), metrics);
            return this.interceptors;
        }

        private Partitioner buildPartition() {
            this.partitioner = config.getConfiguredInstance(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                Partitioner.class,
                Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            return this.partitioner;
        }

        private Sender buildSender() {
            int maxInflightRequests = config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
            int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
            Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);
            KafkaClient client = ClientUtils.createNetworkClient(config,
                this.metrics,
                "producer",
                logContext,
                apiVersions,
                Time.SYSTEM,
                maxInflightRequests,
                metadata,
                throttleTimeSensor,
                null);

            short acks = Short.parseShort(config.getString(ProducerConfig.ACKS_CONFIG));
            this.sender = new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                config.getInt(ProducerConfig.RETRIES_CONFIG),
                metricsRegistry.senderMetrics,
                Time.SYSTEM,
                requestTimeoutMs,
                config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                null) {
                @Override
                protected long sendProducerData(long now) {
                    long result = super.sendProducerData(now);
                    try {
                        //  Ensure the batch expires.
                        Thread.sleep(500);
                        return result;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
            return this.sender;
        }

        private RecordAccumulator buildAccumulator() {
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
            int batchSize = Math.max(1, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG));
            Plugin<Partitioner> partitionerPlugin = Plugin.wrapInstance(
                config.getConfiguredInstance(
                    ProducerConfig.PARTITIONER_CLASS_CONFIG,
                    Partitioner.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId)),
                metrics,
                ProducerConfig.PARTITIONER_CLASS_CONFIG);
            boolean enableAdaptivePartitioning = partitionerPlugin.get() == null &&
                config.getBoolean(ProducerConfig.PARTITIONER_ADPATIVE_PARTITIONING_ENABLE_CONFIG);
            this.accumulator = new RecordAccumulator(logContext,
                batchSize,
                NoCompression.NONE,
                (int) Math.min(config.getLong(ProducerConfig.LINGER_MS_CONFIG), Integer.MAX_VALUE),
                retryBackoffMs,
                retryBackoffMaxMs,
                config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG),
                new RecordAccumulator.PartitionerConfig(
                    enableAdaptivePartitioning,
                    config.getLong(ProducerConfig.PARTITIONER_AVAILABILITY_TIMEOUT_MS_CONFIG)
                ),
                metrics,
                "producer-metrics",
                Time.SYSTEM,
                null,
                new EvilBufferPool(config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG), batchSize, metrics,
                    Time.SYSTEM, "producer-metrics"));
            return accumulator;
        }

        private ProducerMetadata buildMetadata() {
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            long retryBackoffMaxMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            ClusterResourceListeners clusterResourceListeners = ClientUtils.configureClusterResourceListeners(
                List.of(),
                reporters,
                List.of(
                    Plugin.wrapInstance(serializer, metrics, ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).get(),
                    Plugin.wrapInstance(serializer, metrics, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).get()));
            this.metadata = new ProducerMetadata(retryBackoffMs,
                retryBackoffMaxMs,
                config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                config.getLong(ProducerConfig.METADATA_MAX_IDLE_CONFIG),
                logContext,
                clusterResourceListeners,
                Time.SYSTEM);
            metadata.bootstrap(ClientUtils.parseAndValidateAddresses(config));
            return metadata;
        }

        private Sender.SenderThread ioThread() {
            Sender.SenderThread ioThread = new Sender.SenderThread("test_io_thread", sender, true);
            ioThread.start();
            return ioThread;
        }
    }

    static class EvilBufferPool extends BufferPool {

        public EvilBufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
            super(memory, poolableSize, metrics, time, metricGrpName);
        }

        /**
         * Override deallocate to intentionally corrupt the ByteBuffer being returned to the pool.
         * This is used to simulate a scenario where an in-flight buffer is mistakenly reused
         * and its contents are unexpectedly modified, helping expose buffer reuse bugs.
         */
        @Override
        public void deallocate(ByteBuffer buffer, int size) {
            //  Ensure atomicity using reentrant behavior
            lock.lock();
            try {
                Arrays.fill(buffer.array(), (byte) 0);
                super.deallocate(buffer, size);
            } finally {
                lock.unlock();
            }
        }
    }
}
