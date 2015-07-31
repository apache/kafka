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

package io.confluent.streaming.internal;

<<<<<<< HEAD
import io.confluent.streaming.KStreamTopology;
=======
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamJob;
>>>>>>> new api model
import io.confluent.streaming.StreamingConfig;
import io.confluent.streaming.util.ParallelExecutor;
import io.confluent.streaming.util.Util;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KStreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(KStreamThread.class);

    private final KStreamTopology topology;
    private final ArrayList<StreamGroup> streamGroups = new ArrayList<>();
    private final ParallelExecutor parallelExecutor;
    private final Map<Integer, KStreamContextImpl> kstreamContexts = new HashMap<>();
    private final IngestorImpl ingestor;
    private final RecordCollectorImpl collector;
    private final StreamingConfig streamingConfig;
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
    public KStreamThread(KStreamTopology topology, Set<String> topics, StreamingConfig streamingConfig, Metrics metrics) {
        super();
        this.config = new ProcessorConfig(streamingConfig.config());
        this.topology = topology;
        this.streamingConfig = streamingConfig;
        this.metrics = metrics;
        this.streamingMetrics = new KafkaStreamingMetrics();

        Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(streamingConfig.config(), rebalanceCallback, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        this.ingestor = new IngestorImpl(consumer, topics);

        Producer<byte[], byte[]> producer = new KafkaProducer<>(streamingConfig.config(), new ByteArraySerializer(), new ByteArraySerializer());
        this.collector = new RecordCollectorImpl(producer, (Serializer<Object>)streamingConfig.keySerializer(), (Serializer<Object>)streamingConfig.valueSerializer());

        this.running = true;
        this.lastCommit = 0;
        this.nextStateCleaning = Long.MAX_VALUE;
        this.recordsProcessed = 0;
        this.time = new SystemTime();

        // TODO: Fix this after the threading model is decided (also fix KafkaStreaming)
        this.parallelExecutor = new ParallelExecutor(this.config.numStreamThreads);
    }

    /**
     * Execute the stream processors
     */
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
        parallelExecutor.shutdown();
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

                readyForNextExecution = parallelExecutor.execute(streamGroups);

                maybeCommit();
                maybeCleanState();
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    private boolean stillRunning() {
        if(!running) {
            log.debug("Shutting down at user request.");
            return false;
        }
        if(config.totalRecordsToProcess >= 0 && recordsProcessed >= config.totalRecordsToProcess) {
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
        for (KStreamContextImpl context : kstreamContexts.values()) {
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
        if(now > nextStateCleaning) {
            File[] stateDirs = config.stateDir.listFiles();
            if(stateDirs != null) {
                for(File dir: stateDirs) {
                    try {
                        Integer id = Integer.parseInt(dir.getName());
                        if(!kstreamContexts.keySet().contains(id)) {
                            log.info("Deleting obsolete state directory {} after {} delay ms.", dir.getAbsolutePath(), config.stateCleanupDelay);
                            Util.rm(dir);
                        }
                    } catch(NumberFormatException e) {
                        log.warn("Deleting unknown directory in state directory {}.", dir.getAbsolutePath());
                        Util.rm(dir);
                    }
                }
            }
            nextStateCleaning = Long.MAX_VALUE;
        }
    }

    private void addPartitions(Collection<TopicPartition> assignment) {
        HashSet<TopicPartition> partitions = new HashSet<>(assignment);

<<<<<<< HEAD
<<<<<<< HEAD
        ingestor.init();
=======
        Consumer<byte[], byte[]> restoreConsumer =
          new KafkaConsumer<>(streamingConfig.config(), null, new ByteArrayDeserializer(), new ByteArrayDeserializer());
>>>>>>> close stream groups in context

        for (TopicPartition partition : partitions) {
            final Integer id = partition.partition(); // TODO: switch this to the group id
            KStreamContextImpl context = kstreamContexts.get(id);
            if (context == null) {
                try {
                    context = new KStreamContextImpl(id, ingestor, collector, streamingConfig, config, metrics);
                    context.init(topology.sourceStreams());

=======
        for (TopicPartition partition : partitions) {
            final Integer id = partition.partition(); // TODO: switch this to the group id
            KStreamContextImpl context = kstreamContexts.get(id);
            if (context == null) {
                try {
                    KStreamInitializerImpl initializer = new KStreamInitializerImpl(
                      streamingConfig.keySerializer(),
                      streamingConfig.valueSerializer(),
                      streamingConfig.keyDeserializer(),
                      streamingConfig.valueDeserializer()
                    );
                    KStreamJob job = (KStreamJob) Utils.newInstance(jobClass);

                    job.init(initializer);

                    context = new KStreamContextImpl(id, ingestor, collector, streamingConfig, config, metrics);
                    context.init(initializer.sourceStreams());

>>>>>>> new api model
                    kstreamContexts.put(id, context);
                }
                catch (Exception e) {
                    throw new KafkaException(e);
                }

                streamGroups.add(context.streamGroup);
            }
        }

        nextStateCleaning = time.milliseconds() + config.stateCleanupDelay;
    }

    private void removePartitions() {
        for (KStreamContextImpl context : kstreamContexts.values()) {
            log.info("Removing task context {}", context.id());
            try {
                context.close();
            }
            catch (Exception e) {
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
