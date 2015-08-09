/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.clients.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.processor.PTopology;
import org.apache.kafka.clients.processor.ProcessorConfig;
import org.apache.kafka.clients.processor.ProcessorProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class KStreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(KStreamThread.class);

    private final Class<? extends PTopology> topologyClass;
    private final ArrayList<StreamGroup> streamGroups = new ArrayList<>();
    private final Map<Integer, ProcessorContextImpl> kstreamContexts = new HashMap<>();
    private final IngestorImpl ingestor;
    private final RecordCollectorImpl collector;
    private final ProcessorProperties properties;
    private final ProcessorConfig config;
    private final Metrics metrics;
    private final KafkaStreamingMetrics streamingMetrics;
    private final Time time;
    private volatile boolean running;
    private long lastCommit;
    private long nextStateCleaning;
    private long recordsProcessed;

    protected final ConsumerRebalanceCallback rebalanceCallback = new ConsumerRebalanceCallback() {
        @Override
        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            ingestor.init();
            addPartitions(assignment);
        }

        @Override
        public void onPartitionsRevoked(Consumer<?, ?> consumer, Collection<TopicPartition> assignment) {
            commitAll(time.milliseconds());
            removePartitions();
            ingestor.clear();
        }
    };

    @SuppressWarnings("unchecked")
    public KStreamThread(Class<? extends PTopology> topologyClass, ProcessorProperties properties) throws Exception {
        super();

        if (properties.timestampExtractor() == null)
            throw new NullPointerException("timestamp extractor is missing");

        this.metrics = new Metrics();
        this.config = new ProcessorConfig(properties.config());
        this.topologyClass = topologyClass;

        this.properties = properties;
        this.streamingMetrics = new KafkaStreamingMetrics();

        // build the topology without initialization to get the topics for consumer
        PTopology topology = topologyClass.getConstructor().newInstance();
        topology.build();

        // create the producer and consumer clients
        Producer<byte[], byte[]> producer = new KafkaProducer<>(properties.config(), new ByteArraySerializer(), new ByteArraySerializer());
        this.collector = new RecordCollectorImpl(producer, (Serializer<Object>) properties.keySerializer(), (Serializer<Object>) properties.valueSerializer());

        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties.config(), rebalanceCallback, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        this.ingestor = new IngestorImpl(consumer, topology.topics());

        this.running = true;
        this.lastCommit = 0;
        this.nextStateCleaning = Long.MAX_VALUE;
        this.recordsProcessed = 0;
        this.time = new SystemTime();
    }

    /**
     * Execute the stream processors
     */
    @Override
    public synchronized void run() {
        log.info("Starting a kstream thread");
        try {
            ingestor.open();
            runLoop();
        } catch (RuntimeException e) {
            log.error("Uncaught error during processing: ", e);
            throw e;
        } finally {
            shutdown();
        }
    }

    private void shutdown() {
        log.info("Shutting down a kstream thread");
        commitAll(time.milliseconds());

        collector.close();
        ingestor.close();
        removePartitions();
        log.info("kstream thread shutdown complete");
    }

    /**
     * Shutdown this streaming thread.
     */
    public synchronized void close() {
        running = false;
    }

    private void runLoop() {
        try {
            boolean readyForNextExecution = false;

            while (stillRunning()) {
                ingestor.poll(readyForNextExecution ? 0 : this.config.pollTimeMs);

                for (StreamGroup group : this.streamGroups) {
                    readyForNextExecution = group.process();
                }

                maybeCommit();
                maybeCleanState();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean stillRunning() {
        if (!running) {
            log.debug("Shutting down at user request.");
            return false;
        }
        if (config.totalRecordsToProcess >= 0 && recordsProcessed >= config.totalRecordsToProcess) {
            log.debug("Shutting down as we've reached the user-configured limit of {} records to process.", config.totalRecordsToProcess);
            return false;
        }
        return true;
    }

    private void maybeCommit() {
        long now = time.milliseconds();
        if (config.commitTimeMs >= 0 && lastCommit + config.commitTimeMs < now) {
            log.trace("Committing processor instances because the commit interval has elapsed.");
            commitAll(now);
        }
    }

    private void commitAll(long now) {
        Map<TopicPartition, Long> commit = new HashMap<>();
        for (ProcessorContextImpl context : kstreamContexts.values()) {
            context.flush();
            commit.putAll(context.consumedOffsets());
        }

        // check if commit is really needed, i.e. if all the offsets are already committed
        if (ingestor.commitNeeded(commit)) {
            // TODO: for exactly-once we need to make sure the flush and commit
            // are executed atomically whenever it is triggered by user
            collector.flush();
            ingestor.commit(commit); // TODO: can this be async?
            streamingMetrics.commitTime.record(now - lastCommit);
        }
    }

    /* delete any state dirs that aren't for active contexts */
    private void maybeCleanState() {
        long now = time.milliseconds();
        if (now > nextStateCleaning) {
            File[] stateDirs = config.stateDir.listFiles();
            if (stateDirs != null) {
                for (File dir : stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());
                        if (!kstreamContexts.keySet().contains(id)) {
                            log.info("Deleting obsolete state directory {} after {} delay ms.", dir.getAbsolutePath(), config.stateCleanupDelay);
                            Utils.rm(dir);
                        }
                    } catch (NumberFormatException e) {
                        log.warn("Deleting unknown directory in state directory {}.", dir.getAbsolutePath());
                        Utils.rm(dir);
                    }
                }
            }
            nextStateCleaning = Long.MAX_VALUE;
        }
    }

    private void addPartitions(Collection<TopicPartition> assignment) {
        HashSet<TopicPartition> partitions = new HashSet<>(assignment);

        for (TopicPartition partition : partitions) {
            final Integer id = partition.partition(); // TODO: switch this to the group id
            ProcessorContextImpl context = kstreamContexts.get(id);
            if (context == null) {
                try {
                    // build the topology and initialize with the context
                    PTopology topology = this.topologyClass.getConstructor().newInstance();
                    context = new ProcessorContextImpl(id, ingestor, topology, collector, properties, config, metrics);
                    topology.build();
                    topology.init(context);
                    context.initialized();
                    kstreamContexts.put(id, context);
                } catch (Exception e) {
                    throw new KafkaException(e);
                }

                streamGroups.add(context.streamGroup);
            }

            context.addPartition(partition);
        }

        nextStateCleaning = time.milliseconds() + config.stateCleanupDelay;
    }

    private void removePartitions() {
        for (ProcessorContextImpl context : kstreamContexts.values()) {
            log.info("Removing task context {}", context.id());
            try {
                context.close();
            } catch (Exception e) {
                throw new KafkaException(e);
            }
            streamingMetrics.processorDestruction.record();
        }
        streamGroups.clear();
    }

    private class KafkaStreamingMetrics {
        final Sensor commitTime;
        final Sensor processTime;
        final Sensor windowTime;
        final Sensor processorCreation;
        final Sensor processorDestruction;

        public KafkaStreamingMetrics() {
            String group = "kafka-streaming";

            this.commitTime = metrics.sensor("commit-time");
            this.commitTime.add(new MetricName(group, "commit-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "commits-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "commits-per-second"), new Rate(new Count()));

            this.processTime = metrics.sensor("process-time");
            this.commitTime.add(new MetricName(group, "process-time-avg-ms"), new Avg());
            this.commitTime.add(new MetricName(group, "process-time-max-ms"), new Max());
            this.commitTime.add(new MetricName(group, "process-calls-per-second"), new Rate(new Count()));

            this.windowTime = metrics.sensor("window-time");
            this.windowTime.add(new MetricName(group, "window-time-avg-ms"), new Avg());
            this.windowTime.add(new MetricName(group, "window-time-max-ms"), new Max());
            this.windowTime.add(new MetricName(group, "window-calls-per-second"), new Rate(new Count()));

            this.processorCreation = metrics.sensor("processor-creation");
            this.processorCreation.add(new MetricName(group, "processor-creation"), new Rate(new Count()));

            this.processorDestruction = metrics.sensor("processor-destruction");
            this.processorDestruction.add(new MetricName(group, "processor-destruction"), new Rate(new Count()));

        }

    }

}
