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

package io.confluent.streaming;

import io.confluent.streaming.internal.KStreamThread;
import io.confluent.streaming.internal.ProcessorConfig;
<<<<<<< HEAD
import io.confluent.streaming.internal.IngestorImpl;
<<<<<<< HEAD
import io.confluent.streaming.internal.StreamSynchronizer;
<<<<<<< HEAD
=======
import io.confluent.streaming.internal.StreamGroup;
>>>>>>> remove SyncGroup from user facing APIs
import io.confluent.streaming.util.ParallelExecutor;
=======
>>>>>>> removed some generics
import io.confluent.streaming.util.Util;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
=======
>>>>>>> added KStreamThread
import org.apache.kafka.common.metrics.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Kafka Streaming allows for performing continuous computation on input coming from one or more input topics and
 * sends output to zero or more output topics.
 * <p>
 * This processing is done by implementing the {@link KStreamJob} interface to specify the transformation. The
 * {@link KafkaStreaming} instance will be responsible for the lifecycle of these processors. It will instantiate and
 * start one or more of these processors to process the Kafka partitions assigned to this particular instance.
 * <p>
 * This streaming instance will co-ordinate with any other instances (whether in this same process, on other processes
 * on this machine, or on remote machines). These processes will divide up the work so that all partitions are being
 * consumed. If instances are added or die, the corresponding {@link KStreamJob} instances will be shutdown or
 * started in the appropriate processes to balance processing load.
 * <p>
 * Internally the {@link KafkaStreaming} instance contains a normal {@link org.apache.kafka.clients.producer.KafkaProducer KafkaProducer}
 * and {@link org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer} instance that is used for reading input and writing output.
 * <p>
 * A simple example might look like this:
 * <pre>
 *    Properties props = new Properties();
 *    props.put("bootstrap.servers", "localhost:4242");
 *    StreamingConfig config = new StreamingConfig(props);
 *    config.processor(ExampleStreamProcessor.class);
 *    config.serialization(new StringSerializer(), new StringDeserializer());
 *    KafkaStreaming container = new KafkaStreaming(MyKStreamJob.class, config);
 *    container.run();
 * </pre>
 *
 */
public class KafkaStreaming implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaStreaming.class);

<<<<<<< HEAD
<<<<<<< HEAD
    private final Class<? extends KStreamJob> jobClass;
    private final Set<String> topics;
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    private final Map<Integer, Collection<SyncGroup>> syncGroups = new HashMap<>();
    private final ArrayList<StreamSynchronizer<?, ?>> streamSynchronizers = new ArrayList<>();
=======
    private final Map<Integer, Collection<StreamGroup>> streamSynchronizersForPartition = new HashMap<>();
=======
>>>>>>> remove unused member variable
    private final ArrayList<StreamGroup> streamGroups = new ArrayList<>();
>>>>>>> remove SyncGroup from user facing APIs
    private final ParallelExecutor parallelExecutor;
=======
    private final Map<Integer, Collection<StreamSynchronizer>> streamSynchronizersForPartition = new HashMap<>();
>>>>>>> removed some generics
    private final Map<Integer, KStreamContextImpl> kstreamContexts = new HashMap<>();
    protected final Producer<byte[], byte[]> producer;
    protected final Consumer<byte[], byte[]> consumer;
    private final IngestorImpl ingestor;
    private final StreamingConfig streamingConfig;
=======
>>>>>>> added KStreamThread
=======
    //
    // Container State Transition
    //
    //           run()            startShutdown()            shutdown()
    // CREATED --------> RUNNING ----------------> STOPPING -----------> STOPPED
    //    |                                            ^
    //    |           startShutdown()                  |
    //    +--------------------------------------------+
    //
    private final int CREATED = 0;
    private final int RUNNING = 1;
    private final int STOPPING = 2;
    private final int STOPPED = 3;
    private int state = CREATED;

>>>>>>> removed Coordinator
    private final ProcessorConfig config;
    private final Object lock = new Object();
    private final KStreamThread[] threads;
    private final Set<String> topics;


<<<<<<< HEAD
    @SuppressWarnings("unchecked")
    protected KafkaStreaming(Class<? extends KStreamJob> jobClass,
                             StreamingConfig config,
                             Producer<byte[], byte[]> producer,
                             Consumer<byte[], byte[]> consumer) {
        this.jobClass = jobClass;
        this.producer = producer == null? new KafkaProducer<>(config.config(), new ByteArraySerializer(), new ByteArraySerializer()): producer;
        this.consumer = consumer == null? new KafkaConsumer<>(config.config(), rebalanceCallback, new ByteArrayDeserializer(), new ByteArrayDeserializer()): consumer;
        this.streamingConfig = config;
        this.metrics = new Metrics();
        this.streamingMetrics = new KafkaStreamingMetrics();
        this.config = new ProcessorConfig(config.config());
<<<<<<< HEAD
<<<<<<< HEAD
        this.ingestor =
<<<<<<< HEAD
            new IngestorImpl<>(this.consumer,
                               (Deserializer<Object>) config.keyDeserializer(),
                               (Deserializer<Object>) config.valueDeserializer(),
                               this.config.pollTimeMs);
=======
            new IngestorImpl(this.consumer,
                             (Deserializer<Object>) config.keyDeserializer(),
                             (Deserializer<Object>) config.valueDeserializer(),
                             this.config.pollTimeMs);
>>>>>>> removed some generics
=======
        this.ingestor = new IngestorImpl(this.consumer, this.config.pollTimeMs);
>>>>>>> clean up ingestor and stream synchronizer
=======
        this.ingestor = new IngestorImpl(this.consumer);
>>>>>>> use poll(0) for non-blocking poll
        this.running = true;
        this.lastCommit = 0;
        this.nextStateCleaning = Long.MAX_VALUE;
        this.recordsProcessed = 0;
        this.time = new SystemTime();
        this.parallelExecutor = new ParallelExecutor(this.config.numStreamThreads);
=======
    public KafkaStreaming(Class<? extends KStreamJob> jobClass, StreamingConfig streamingConfig) {
>>>>>>> added KStreamThread

        this.config = new ProcessorConfig(streamingConfig.config());
        try {
            this.topics = new HashSet<>(Arrays.asList(this.config.topics.split(",")));
        }
        catch (Exception e) {
            throw new KStreamException("failed to get a topic list from the streaming config", e);
        }

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
        for (StreamSynchronizer<?, ?> streamSynchronizer : streamSynchronizers) {
=======
        for (StreamGroup streamGroup : streamGroups) {
>>>>>>> remove SyncGroup from user facing APIs
            try {
                streamGroup.close();
            }
            catch(Exception e) {
                log.error("Error while closing stream synchronizers: ", e);
=======
        for (Map.Entry<Integer, Collection<StreamSynchronizer>> entry : streamSynchronizersForPartition.entrySet()) {
            for (StreamSynchronizer streamSynchronizer : entry.getValue()) {
                try {
                    streamSynchronizer.close();
                }
                catch(Exception e) {
                    log.error("Error while closing stream synchronizers: ", e);
                }
>>>>>>> removed some generics
=======
        Coordinator coordinator = new Coordinator() {
            @Override
            public void commit() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void shutdown() {
                startShutdown();
>>>>>>> added KStreamThread
            }
        };

<<<<<<< HEAD
        producer.close();
        consumer.close();
        parallelExecutor.shutdown();
<<<<<<< HEAD
<<<<<<< HEAD
        syncGroups.clear();
        streamSynchronizers.clear();
=======
        streamSynchronizersForPartition.clear();
=======
>>>>>>> remove unused member variable
        streamGroups.clear();
>>>>>>> remove SyncGroup from user facing APIs
        shutdownComplete.countDown();
        log.info("Shut down complete");
=======
=======
>>>>>>> removed Coordinator
        Metrics metrics = new Metrics();

        // TODO: Fix this after the threading model is decided (also fix KStreamThread)
        this.threads = new KStreamThread[1];
<<<<<<< HEAD
        threads[0] = new KStreamThread(jobClass, topics, streamingConfig, coordinator, metrics);
>>>>>>> added KStreamThread
=======
        threads[0] = new KStreamThread(jobClass, topics, streamingConfig, metrics);
>>>>>>> removed Coordinator
    }

    /**
     * Execute the stream processors
     */
<<<<<<< HEAD
    public synchronized void close() {
        running = false;
        try {
            shutdownComplete.await();
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    private void runLoop() {
        try {
            boolean readyForNextExecution = false;

            while (stillRunning()) {
                ingestor.poll(readyForNextExecution ? 0 : this.config.pollTimeMs);

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
                parallelExecutor.execute(streamSynchronizers, status);
=======
                parallelExecutor.execute(streamSynchronizers);
>>>>>>> clean up ingestor and stream synchronizer
=======
                parallelExecutor.execute(streamGroups);
>>>>>>> remove SyncGroup from user facing APIs
=======
                readyForNextExecution = parallelExecutor.execute(streamGroups);
>>>>>>> use poll(0) for non-blocking poll

=======
                for (Map.Entry<Integer, Collection<StreamSynchronizer>> entry : streamSynchronizersForPartition.entrySet()) {
                    for (StreamSynchronizer streamSynchronizer : entry.getValue()) {
                        streamSynchronizer.process();
                        pollRequired = pollRequired || streamSynchronizer.requiresPoll();
                    }
                }
>>>>>>> removed some generics
                maybeCommit();
                maybeCleanState();
=======
    public void run() {
        synchronized (lock) {
            log.info("Starting container");
            if (state == CREATED) {
                if (!config.stateDir.exists() && !config.stateDir.mkdirs())
                    throw new IllegalArgumentException("Failed to create state directory: " + config.stateDir.getAbsolutePath());

                for (KStreamThread thread : threads) thread.start();
                log.info("Start-up complete");
            } else {
                throw new IllegalStateException("This container was already started");
>>>>>>> added KStreamThread
            }

<<<<<<< HEAD
<<<<<<< HEAD
    private void commitAll(long now) {
        Map<TopicPartition, Long> commit = new HashMap<>();
        for (KStreamContextImpl context : kstreamContexts.values()) {
            context.flush();
            // check co-ordinator
        }
<<<<<<< HEAD
<<<<<<< HEAD
        for (StreamSynchronizer<?, ?> streamSynchronizer : streamSynchronizers) {
=======
        for (StreamGroup streamGroup : streamGroups) {
>>>>>>> remove SyncGroup from user facing APIs
            try {
                commit.putAll(streamGroup.consumedOffsets());
            }
            catch(Exception e) {
                log.error("Error while closing processor: ", e);
=======
        for (Map.Entry<Integer, Collection<StreamSynchronizer>> entry : streamSynchronizersForPartition.entrySet()) {
            for (StreamSynchronizer streamSynchronizer : entry.getValue()) {
                try {
                    commit.putAll(streamSynchronizer.consumedOffsets());
                }
                catch(Exception e) {
                    log.error("Error while closing processor: ", e);
                }
>>>>>>> removed some generics
            }
        }

        // check if commit is really needed, i.e. if all the offsets are already committed
        boolean commitNeeded = false;
        for (TopicPartition tp : commit.keySet()) {
            if (consumer.committed(tp) != commit.get(tp)) {
                commitNeeded = true;
                break;
=======
            while (!stopping) {
=======
            state = RUNNING;
            while (state == RUNNING) {
>>>>>>> removed Coordinator
                try {
                    lock.wait();
                }
                catch (InterruptedException ex) {
                    Thread.interrupted();
                }
>>>>>>> added KStreamThread
            }

            if (state == STOPPING) {
                log.info("Shutting down the container");

                for (KStreamThread thread : threads)
                    thread.close();

                for (KStreamThread thread : threads) {
                    try {
                        thread.join();
                    } catch (InterruptedException ex) {
                        Thread.interrupted();
                    }
<<<<<<< HEAD
                };

                kstreamContext =
                  new KStreamContextImpl(id, job, topics, ingestor, producer, coordinator, streamingConfig, config, metrics);

                kstreamContexts.put(id, kstreamContext);

                try {
                    kstreamContext.init(restoreConsumer);
                }
                catch (Exception e) {
                    throw new KafkaException(e);
                }

<<<<<<< HEAD
<<<<<<< HEAD
                Collection<SyncGroup> syncGroups = kstreamContext.syncGroups();
                this.syncGroups.put(id, syncGroups);
                for (SyncGroup syncGroup : syncGroups) {
                    streamSynchronizers.add(syncGroup.streamSynchronizer);
=======
                Collection<StreamGroup> streamGroups = kstreamContext.streamSynchronizers();
                for (StreamGroup streamGroup : streamGroups) {
                    streamGroups.add(streamGroup);
>>>>>>> remove SyncGroup from user facing APIs
                }
=======
                streamSynchronizersForPartition.put(id, kstreamContext.streamSynchronizers());
>>>>>>> removed some generics
=======
                }
                state = STOPPED;
                lock.notifyAll();
                log.info("Shutdown complete");
>>>>>>> added KStreamThread
            }
        }
    }

    /**
     * Shutdown this streaming instance.
     */
    public void close() {
        synchronized (lock) {
            if (state == CREATED || state == RUNNING) {
                state = STOPPING;
                lock.notifyAll();
            }
            while (state == STOPPING) {
                try {
                    lock.wait();
                }
                catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
        }
    }

}
