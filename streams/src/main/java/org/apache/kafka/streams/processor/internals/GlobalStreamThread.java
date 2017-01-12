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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.state.internals.ThreadCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * This is the thread responsible for keeping all Global State Stores updated.
 * It delegates most of the responsibility to the internal class StateConsumer
 */
public class GlobalStreamThread extends Thread {

    private static final Logger log = LoggerFactory.getLogger(GlobalStreamThread.class);
    private final StreamsConfig config;
    private final Consumer<byte[], byte[]> consumer;
    private final StateDirectory stateDirectory;
    private final Time time;
    private final ThreadCache cache;
    private final StreamsMetrics streamsMetrics;
    private final ProcessorTopology topology;
    private volatile boolean running = false;
    private volatile StreamsException startupException;

    public GlobalStreamThread(final ProcessorTopology topology,
                              final StreamsConfig config,
                              final Consumer<byte[], byte[]> globalConsumer,
                              final StateDirectory stateDirectory,
                              final Metrics metrics,
                              final Time time,
                              final String clientId
    ) {
        super("GlobalStreamThread");
        this.topology = topology;
        this.config = config;
        this.consumer = globalConsumer;
        this.stateDirectory = stateDirectory;
        this.time = time;
        long cacheSizeBytes = Math.max(0, config.getLong(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG) /
                (config.getInt(StreamsConfig.NUM_STREAM_THREADS_CONFIG) + 1));
        final String threadClientId = clientId + "-" + getName();
        this.streamsMetrics = new StreamsMetricsImpl(metrics, threadClientId, Collections.singletonMap("client-id", threadClientId));
        this.cache = new ThreadCache(threadClientId, cacheSizeBytes, streamsMetrics);
    }

    static class StateConsumer {
        private final Consumer<byte[], byte[]> consumer;
        private final GlobalStateMaintainer stateMaintainer;
        private final Time time;
        private final long pollMs;
        private final long flushInterval;

        private long lastFlush;

        StateConsumer(final Consumer<byte[], byte[]> consumer,
                      final GlobalStateMaintainer stateMaintainer,
                      final Time time,
                      final long pollMs,
                      final long flushInterval) {
            this.consumer = consumer;
            this.stateMaintainer = stateMaintainer;
            this.time = time;
            this.pollMs = pollMs;
            this.flushInterval = flushInterval;
        }

        void initialize() {
            final Map<TopicPartition, Long> partitionOffsets = stateMaintainer.initialize();
            consumer.assign(partitionOffsets.keySet());
            for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
                consumer.seek(entry.getKey(), entry.getValue());
            }
            lastFlush = time.milliseconds();
        }

        void pollAndUpdate() {
            final ConsumerRecords<byte[], byte[]> received = consumer.poll(pollMs);
            for (ConsumerRecord<byte[], byte[]> record : received) {
                stateMaintainer.update(record);
            }
            final long now = time.milliseconds();
            if (flushInterval >= 0 && now >= lastFlush + flushInterval) {
                stateMaintainer.flushState();
                lastFlush = now;
            }
        }

        public void close() throws IOException {
            // just log an error if the consumer throws an exception during close
            // so we can always attempt to close the state stores.
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Failed to cleanly close GlobalStreamThread consumer", e);
            }

            stateMaintainer.close();

        }
    }

    @Override
    public void run() {
        final StateConsumer stateConsumer = initialize();
        if (stateConsumer == null) {
            return;
        }

        try {
            while (running) {
                stateConsumer.pollAndUpdate();
            }
            log.debug("Shutting down GlobalStreamThread at user request");
        } finally {
            try {
                stateConsumer.close();
            } catch (IOException e) {
                log.error("Failed to cleanly shutdown GlobalStreamThread", e);
            }
        }
    }

    private StateConsumer initialize() {
        try {
            final GlobalStateManager stateMgr = new GlobalStateManagerImpl(topology, consumer, stateDirectory);
            final StateConsumer stateConsumer
                    = new StateConsumer(consumer,
                                        new GlobalStateUpdateTask(topology,
                                                                  new GlobalProcessorContextImpl(
                                                                          config,
                                                                          stateMgr,
                                                                          streamsMetrics,
                                                                          cache),
                                                                  stateMgr),
                                        time,
                                        config.getLong(StreamsConfig.POLL_MS_CONFIG),
                                        config.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
            stateConsumer.initialize();
            running = true;
            return stateConsumer;
        } catch (StreamsException e) {
            startupException = e;
        } catch (Exception e) {
            startupException = new StreamsException("Exception caught during initialization of GlobalStreamThread", e);
        }
        return null;
    }

    @Override
    public synchronized void start() {
        super.start();
        while (!running) {
            Utils.sleep(1);
            if (startupException != null) {
                throw startupException;
            }
        }
    }


    public void close() {
        running = false;
    }

    public boolean stillRunning() {
        return running;
    }


}
