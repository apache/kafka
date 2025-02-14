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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.JsonUtil;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.WorkerUtils;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.apache.kafka.trogdor.task.WorkerStatusTracker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class SustainedConnectionWorker implements TaskWorker {
    private static final Logger log = LoggerFactory.getLogger(SustainedConnectionWorker.class);

    // This is the metadata for the test itself.
    private final String id;
    private final SustainedConnectionSpec spec;

    // These variables are used to maintain the connections.
    private static final int BACKOFF_PERIOD_MS = 10;
    private ExecutorService workerExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private KafkaFutureImpl<String> doneFuture;
    private ArrayList<SustainedConnection> connections;

    // These variables are used when tracking the reported status of the worker.
    private static final int REPORT_INTERVAL_MS = 5000;
    private WorkerStatusTracker status;
    private AtomicLong totalProducerConnections;
    private AtomicLong totalProducerFailedConnections;
    private AtomicLong totalConsumerConnections;
    private AtomicLong totalConsumerFailedConnections;
    private AtomicLong totalMetadataConnections;
    private AtomicLong totalMetadataFailedConnections;
    private AtomicLong totalAbortedThreads;
    private Future<?> statusUpdaterFuture;
    private ScheduledExecutorService statusUpdaterExecutor;

    public SustainedConnectionWorker(String id, SustainedConnectionSpec spec) {
        this.id = id;
        this.spec = spec;
    }

    @Override
    public void start(Platform platform, WorkerStatusTracker status,
                      KafkaFutureImpl<String> doneFuture) throws Exception {
        if (!running.compareAndSet(false, true)) {
            throw new IllegalStateException("SustainedConnectionWorker is already running.");
        }
        log.info("{}: Activating SustainedConnectionWorker with {}", this.id, this.spec);
        this.doneFuture = doneFuture;
        this.status = status;
        this.connections = new ArrayList<>();

        // Initialize all status reporting metrics to 0.
        this.totalProducerConnections = new AtomicLong(0);
        this.totalProducerFailedConnections = new AtomicLong(0);
        this.totalConsumerConnections = new AtomicLong(0);
        this.totalConsumerFailedConnections = new AtomicLong(0);
        this.totalMetadataConnections = new AtomicLong(0);
        this.totalMetadataFailedConnections = new AtomicLong(0);
        this.totalAbortedThreads = new AtomicLong(0);

        // Create the worker classes and add them to the list of items to act on.
        for (int i = 0; i < this.spec.producerConnectionCount(); i++) {
            this.connections.add(new ProducerSustainedConnection());
        }
        for (int i = 0; i < this.spec.consumerConnectionCount(); i++) {
            this.connections.add(new ConsumerSustainedConnection());
        }
        for (int i = 0; i < this.spec.metadataConnectionCount(); i++) {
            this.connections.add(new MetadataSustainedConnection());
        }

        // Create the status reporter thread and schedule it.
        this.statusUpdaterExecutor = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("StatusUpdaterWorkerThread%d", false));
        this.statusUpdaterFuture = this.statusUpdaterExecutor.scheduleAtFixedRate(
            new StatusUpdater(), 0, REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);

        // Create the maintainer pool, add all the maintainer threads, then start it.
        this.workerExecutor = Executors.newFixedThreadPool(spec.numThreads(),
            ThreadUtils.createThreadFactory("SustainedConnectionWorkerThread%d", false));
        for (int i = 0; i < this.spec.numThreads(); i++) {
            this.workerExecutor.submit(new MaintainLoop());
        }
    }

    private interface SustainedConnection extends AutoCloseable {
        boolean needsRefresh(long milliseconds);
        void refresh();
        void claim();
    }

    private abstract static class ClaimableConnection implements SustainedConnection {

        protected long nextUpdate = 0;
        protected boolean inUse = false;
        protected long refreshRate;

        @Override
        public boolean needsRefresh(long milliseconds) {
            return !this.inUse && (milliseconds > this.nextUpdate);
        }

        @Override
        public void claim() {
            this.inUse = true;
        }

        @Override
        public void close() {
            this.closeQuietly();
        }

        protected void completeRefresh() {
            this.nextUpdate = Time.SYSTEM.milliseconds() + this.refreshRate;
            this.inUse = false;
        }

        protected abstract void closeQuietly();

    }

    private class MetadataSustainedConnection extends ClaimableConnection {

        private Admin client;
        private final Properties props;

        MetadataSustainedConnection() {

            // These variables are used to maintain the connection itself.
            this.client = null;
            this.refreshRate = SustainedConnectionWorker.this.spec.refreshRateMs();

            // This variable is used to maintain the connection properties.
            this.props = new Properties();
            this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SustainedConnectionWorker.this.spec.bootstrapServers());
            WorkerUtils.addConfigsToProperties(
                    this.props, SustainedConnectionWorker.this.spec.commonClientConf(), SustainedConnectionWorker.this.spec.commonClientConf());
        }

        @Override
        public void refresh() {
            try {
                if (this.client == null) {
                    // Housekeeping to track the number of opened connections.
                    SustainedConnectionWorker.this.totalMetadataConnections.incrementAndGet();

                    // Create the admin client connection.
                    this.client = Admin.create(this.props);
                }

                // Fetch some metadata to keep the connection alive.
                this.client.describeCluster().nodes().get();

            } catch (Throwable e) {
                // Set the admin client to be recreated on the next cycle.
                this.closeQuietly();

                // Housekeeping to track the number of opened connections and failed connection attempts.
                SustainedConnectionWorker.this.totalMetadataConnections.decrementAndGet();
                SustainedConnectionWorker.this.totalMetadataFailedConnections.incrementAndGet();
                SustainedConnectionWorker.log.error("Error while refreshing sustained AdminClient connection", e);
            }

            // Schedule this again and set to not in use.
            this.completeRefresh();
        }

        @Override
        protected void closeQuietly() {
            Utils.closeQuietly(this.client, "AdminClient");
            this.client = null;
        }
    }

    private class ProducerSustainedConnection extends ClaimableConnection {

        private KafkaProducer<byte[], byte[]> producer;
        private List<TopicPartition> partitions;
        private Iterator<TopicPartition> partitionsIterator;
        private final String topicName;
        private final PayloadIterator keys;
        private final PayloadIterator values;
        private final Properties props;

        ProducerSustainedConnection() {

            // These variables are used to maintain the connection itself.
            this.producer = null;
            this.partitions = null;
            this.topicName = SustainedConnectionWorker.this.spec.topicName();
            this.partitionsIterator = null;
            this.keys = new PayloadIterator(SustainedConnectionWorker.this.spec.keyGenerator());
            this.values = new PayloadIterator(SustainedConnectionWorker.this.spec.valueGenerator());
            this.refreshRate = SustainedConnectionWorker.this.spec.refreshRateMs();

            // This variable is used to maintain the connection properties.
            this.props = new Properties();
            this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SustainedConnectionWorker.this.spec.bootstrapServers());
            WorkerUtils.addConfigsToProperties(
                    this.props, SustainedConnectionWorker.this.spec.commonClientConf(), SustainedConnectionWorker.this.spec.producerConf());
        }

        @Override
        public void refresh() {
            try {
                if (this.producer == null) {
                    // Housekeeping to track the number of opened connections.
                    SustainedConnectionWorker.this.totalProducerConnections.incrementAndGet();

                    // Create the producer, fetch the specified topic's partitions and randomize them.
                    this.producer = new KafkaProducer<>(this.props, new ByteArraySerializer(), new ByteArraySerializer());
                    this.partitions = this.producer.partitionsFor(this.topicName).stream()
                             .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                             .toList();
                    Collections.shuffle(this.partitions);
                }

                // Create a new iterator over the partitions if the current one doesn't exist or is exhausted.
                if (this.partitionsIterator == null || !this.partitionsIterator.hasNext()) {
                    this.partitionsIterator = this.partitions.iterator();
                }

                // Produce a single record and send it synchronously.
                TopicPartition partition = this.partitionsIterator.next();
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        partition.topic(), partition.partition(), keys.next(), values.next());
                producer.send(record).get();

            } catch (Throwable e) {
                // Set the producer to be recreated on the next cycle.
                this.closeQuietly();

                // Housekeeping to track the number of opened connections and failed connection attempts.
                SustainedConnectionWorker.this.totalProducerConnections.decrementAndGet();
                SustainedConnectionWorker.this.totalProducerFailedConnections.incrementAndGet();
                SustainedConnectionWorker.log.error("Error while refreshing sustained KafkaProducer connection", e);

            }

            // Schedule this again and set to not in use.
            this.completeRefresh();
        }

        @Override
        protected void closeQuietly() {
            Utils.closeQuietly(this.producer, "KafkaProducer");
            this.producer = null;
            this.partitions = null;
            this.partitionsIterator = null;
        }
    }

    private class ConsumerSustainedConnection extends ClaimableConnection {

        private KafkaConsumer<byte[], byte[]> consumer;
        private TopicPartition activePartition;
        private final String topicName;
        private final Random rand;
        private final Properties props;

        ConsumerSustainedConnection() {
            // These variables are used to maintain the connection itself.
            this.topicName = SustainedConnectionWorker.this.spec.topicName();
            this.consumer = null;
            this.activePartition = null;
            this.rand = new Random();
            this.refreshRate = SustainedConnectionWorker.this.spec.refreshRateMs();

            // This variable is used to maintain the connection properties.
            this.props = new Properties();
            this.props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SustainedConnectionWorker.this.spec.bootstrapServers());
            this.props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            this.props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            this.props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024);
            WorkerUtils.addConfigsToProperties(
                    this.props, SustainedConnectionWorker.this.spec.commonClientConf(), SustainedConnectionWorker.this.spec.consumerConf());
        }

        @Override
        public void refresh() {
            try {

                if (this.consumer == null) {

                    // Housekeeping to track the number of opened connections.
                    SustainedConnectionWorker.this.totalConsumerConnections.incrementAndGet();

                    // Create the consumer and fetch the partitions for the specified topic.
                    this.consumer = new KafkaConsumer<>(this.props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
                    List<TopicPartition> partitions = this.consumer.partitionsFor(this.topicName).stream()
                            .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                            .toList();

                    // Select a random partition and assign it.
                    this.activePartition = partitions.get(this.rand.nextInt(partitions.size()));
                    this.consumer.assign(Collections.singletonList(this.activePartition));
                }

                // The behavior when passing in an empty list is to seek to the end of all subscribed partitions.
                this.consumer.seekToEnd(Collections.emptyList());

                // Poll to keep the connection alive, ignoring any records returned.
                this.consumer.poll(Duration.ofMillis(50));

            } catch (Throwable e) {

                // Set the consumer to be recreated on the next cycle.
                this.closeQuietly();

                // Housekeeping to track the number of opened connections and failed connection attempts.
                SustainedConnectionWorker.this.totalConsumerConnections.decrementAndGet();
                SustainedConnectionWorker.this.totalConsumerFailedConnections.incrementAndGet();
                SustainedConnectionWorker.log.error("Error while refreshing sustained KafkaConsumer connection", e);

            }

            // Schedule this again and set to not in use.
            this.completeRefresh();
        }

        @Override
        protected void closeQuietly() {
            Utils.closeQuietly(this.consumer, "KafkaConsumer");
            this.consumer = null;
            this.activePartition = null;
        }
    }

    public class MaintainLoop implements Runnable {
        @Override
        public void run() {
            try {
                while (!doneFuture.isDone()) {
                    Optional<SustainedConnection> currentConnection = SustainedConnectionWorker.this.findConnectionToMaintain();
                    if (currentConnection.isPresent()) {
                        currentConnection.get().refresh();
                    } else {
                        Time.SYSTEM.sleep(SustainedConnectionWorker.BACKOFF_PERIOD_MS);
                    }
                }
            } catch (Exception e) {
                SustainedConnectionWorker.this.totalAbortedThreads.incrementAndGet();
                SustainedConnectionWorker.log.error("Aborted thread while maintaining sustained connections", e);
            }
        }
    }

    private synchronized Optional<SustainedConnection> findConnectionToMaintain() {
        final long milliseconds = Time.SYSTEM.milliseconds();
        for (SustainedConnection connection : this.connections) {
            if (connection.needsRefresh(milliseconds)) {
                connection.claim();
                return Optional.of(connection);
            }
        }
        return Optional.empty();
    }

    private class StatusUpdater implements Runnable {
        @Override
        public void run() {
            try {
                JsonNode node = JsonUtil.JSON_SERDE.valueToTree(
                    new StatusData(
                            SustainedConnectionWorker.this.totalProducerConnections.get(),
                            SustainedConnectionWorker.this.totalProducerFailedConnections.get(),
                            SustainedConnectionWorker.this.totalConsumerConnections.get(),
                            SustainedConnectionWorker.this.totalConsumerFailedConnections.get(),
                            SustainedConnectionWorker.this.totalMetadataConnections.get(),
                            SustainedConnectionWorker.this.totalMetadataFailedConnections.get(),
                            SustainedConnectionWorker.this.totalAbortedThreads.get(),
                            Time.SYSTEM.milliseconds()));
                status.update(node);
            } catch (Exception e) {
                SustainedConnectionWorker.log.error("Aborted test while running StatusUpdater", e);
                WorkerUtils.abort(log, "StatusUpdater", e, doneFuture);
            }
        }
    }

    public static class StatusData {
        private final long totalProducerConnections;
        private final long totalProducerFailedConnections;
        private final long totalConsumerConnections;
        private final long totalConsumerFailedConnections;
        private final long totalMetadataConnections;
        private final long totalMetadataFailedConnections;
        private final long totalAbortedThreads;
        private final long updatedMs;

        @JsonCreator
        StatusData(@JsonProperty("totalProducerConnections") long totalProducerConnections,
                   @JsonProperty("totalProducerFailedConnections") long totalProducerFailedConnections,
                   @JsonProperty("totalConsumerConnections") long totalConsumerConnections,
                   @JsonProperty("totalConsumerFailedConnections") long totalConsumerFailedConnections,
                   @JsonProperty("totalMetadataConnections") long totalMetadataConnections,
                   @JsonProperty("totalMetadataFailedConnections") long totalMetadataFailedConnections,
                   @JsonProperty("totalAbortedThreads") long totalAbortedThreads,
                   @JsonProperty("updatedMs") long updatedMs) {
            this.totalProducerConnections = totalProducerConnections;
            this.totalProducerFailedConnections = totalProducerFailedConnections;
            this.totalConsumerConnections = totalConsumerConnections;
            this.totalConsumerFailedConnections = totalConsumerFailedConnections;
            this.totalMetadataConnections = totalMetadataConnections;
            this.totalMetadataFailedConnections = totalMetadataFailedConnections;
            this.totalAbortedThreads = totalAbortedThreads;
            this.updatedMs = updatedMs;
        }

        @JsonProperty
        public long totalProducerConnections() {
            return totalProducerConnections;
        }

        @JsonProperty
        public long totalProducerFailedConnections() {
            return totalProducerFailedConnections;
        }

        @JsonProperty
        public long totalConsumerConnections() {
            return totalConsumerConnections;
        }

        @JsonProperty
        public long totalConsumerFailedConnections() {
            return totalConsumerFailedConnections;
        }

        @JsonProperty
        public long totalMetadataConnections() {
            return totalMetadataConnections;
        }

        @JsonProperty
        public long totalMetadataFailedConnections() {
            return totalMetadataFailedConnections;
        }

        @JsonProperty
        public long totalAbortedThreads() {
            return totalAbortedThreads;
        }

        @JsonProperty
        public long updatedMs() {
            return updatedMs;
        }
    }

    @Override
    public void stop(Platform platform) throws Exception {
        if (!running.compareAndSet(true, false)) {
            throw new IllegalStateException("SustainedConnectionWorker is not running.");
        }
        log.info("{}: Deactivating SustainedConnectionWorker.", this.id);

        // Shut down the periodic status updater and perform a final update on the
        // statistics.  We want to do this first, before deactivating any threads.
        // Otherwise, if some threads take a while to terminate, this could lead
        // to a misleading rate getting reported.
        this.statusUpdaterFuture.cancel(false);
        this.statusUpdaterExecutor.shutdown();
        this.statusUpdaterExecutor.awaitTermination(1, TimeUnit.HOURS);
        this.statusUpdaterExecutor = null;
        new StatusUpdater().run();

        doneFuture.complete("");
        for (SustainedConnection connection : this.connections) {
            connection.close();
        }
        workerExecutor.shutdownNow();
        workerExecutor.awaitTermination(1, TimeUnit.HOURS);
        this.workerExecutor = null;
        this.status = null;
        this.connections = null;
    }
}
