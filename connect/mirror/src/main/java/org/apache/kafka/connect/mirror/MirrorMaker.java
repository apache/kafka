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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.util.ConnectUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * A set of Connect Herders for replicating between multiple Kafka clusters.
 *
 * Each Herder represents a separate source->target replication flow.
 */
public class MirrorMaker {
    private static final Logger log = LoggerFactory.getLogger(MirrorMaker.class);

    private static final List<Class> CONNECTOR_CLASSES = Arrays.asList(
        MirrorSourceConnector.class,
        MirrorCheckpointConnector.class,
        MirrorHeartbeatConnector.class);
 
    private final Map<SourceAndTarget, Herder> herders = new HashMap<>();
    private CountDownLatch startLatch;
    private CountDownLatch stopLatch;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ShutdownHook shutdownHook;
    private final String advertisedBaseUrl;
    private final Time time;
    private final MirrorMakerConfig config;

    public MirrorMaker(Map<String, String> props, Time time) {
        log.debug("Kafka MirrorMaker instance created");
        this.time = time;
        this.advertisedBaseUrl = "TODO";
        this.config = new MirrorMakerConfig(props);
        enabledClusterPairs().forEach(x -> addHerder(x, config.workerConfig(x)));
        shutdownHook = new ShutdownHook();
    }

    public void start() {
        log.info("Kafka MirrorMaker starting with {} herders.", herders.size());
        if (startLatch != null) {
            throw new IllegalStateException("MirrorMaker instance already started");
        }
        startLatch = new CountDownLatch(herders.size());
        stopLatch = new CountDownLatch(herders.size());
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        for (Herder herder : herders.values()) {
            try {
                herder.start();
            } finally {
                startLatch.countDown();
            }
        }
        log.info("Starting connectors...");
        enabledClusterPairs().forEach(x -> startConnectors(x));
        log.info("Kafka MirrorMaker started");
    }

    public void stop() {
        boolean wasShuttingDown = shutdown.getAndSet(true);
        if (!wasShuttingDown) {
            log.info("Kafka MirrorMaker stopping");
            for (Herder herder : herders.values()) {
                try {
                    herder.stop();
                } finally {
                    stopLatch.countDown();
                }
            }
            log.info("Kafka MirrorMaker stopped");
        }
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for Kafka Connect to shutdown");
        }
    }

    public void pause(String source, String target) {
        SourceAndTarget sourceAndTarget = new SourceAndTarget(source, target);
        checkHerder(sourceAndTarget);
        Herder herder = herders.get(sourceAndTarget);
        CONNECTOR_CLASSES.forEach(x -> herder.pauseConnector(x.getSimpleName()));
    }

    public void resume(String source, String target) {
        SourceAndTarget sourceAndTarget = new SourceAndTarget(source, target);
        checkHerder(sourceAndTarget);
        Herder herder = herders.get(sourceAndTarget);
        CONNECTOR_CLASSES.forEach(x -> herder.resumeConnector(x.getSimpleName()));
    }

    private void startConnector(SourceAndTarget sourceAndTarget, Class connectorClass) {
        checkHerder(sourceAndTarget);
        Map<String, String> connectorProps = config.connectorBaseConfig(sourceAndTarget, connectorClass);
        herders.get(sourceAndTarget)
                .putConnectorConfig(connectorClass.getSimpleName(), connectorProps, true, (e, x) -> {
                    if (e instanceof NotLeaderException) {
                        // No way to determine if the connector is a leader or not beforehand.
                        log.info("Connector {} is a follower. Using existing configuration.", sourceAndTarget);
                    } else {
                        log.info("Connector {} configured.", sourceAndTarget, e);
                    }
                });
    }

    private void checkHerder(SourceAndTarget sourceAndTarget) {
        if (!herders.containsKey(sourceAndTarget)) {
            throw new IllegalArgumentException("No herder for " + sourceAndTarget.toString());
        }
    }

    private void startConnectors(SourceAndTarget sourceAndTarget) {
        CONNECTOR_CLASSES.forEach(x -> startConnector(sourceAndTarget, x));
    }

    private List<SourceAndTarget> enabledClusterPairs() {
        List<SourceAndTarget> pairs = new ArrayList<>();
        for (String source : config.clusters()) {
            for (String target : config.clusters()) {
                SourceAndTarget sourceAndTarget = new SourceAndTarget(source, target);
                if (!source.equals(target) && config.enabled(sourceAndTarget)) {
                    pairs.add(sourceAndTarget);
                }
            }
        }
        return pairs;
    }

    private void addHerder(SourceAndTarget sourceAndTarget, Map<String, String> workerProps) {
        log.info("creating herder for " + sourceAndTarget.toString());
        MirrorMetrics.metricsFor(sourceAndTarget); // for side effect
        String advertisedUrl = advertisedBaseUrl + "/" + sourceAndTarget.source();
        String workerId = sourceAndTarget.toString();
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        DistributedConfig config = new DistributedConfig(workerProps);
        String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);
        Worker worker = new Worker(workerId, time, plugins, config, offsetBackingStore);
        WorkerConfigTransformer configTransformer = worker.configTransformer();
        Converter internalValueConverter = worker.getInternalValueConverter();
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter);
        statusBackingStore.configure(config);
        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                config,
                configTransformer);
        Herder herder = new DistributedHerder(config, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl);
        herders.put(sourceAndTarget, herder);
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                startLatch.await();
                MirrorMaker.this.stop();
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for Kafka Connect startup to finish");
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            log.error("Usage: MirrorMaker mirror-maker.properties");
            Exit.exit(1);
        }

        try {
            log.info("Kafka MirrorMaker initializing ...");

            String mirrorMakerPropsFile = args[0];
            Map<String, String> mirrorMakerProps = !mirrorMakerPropsFile.isEmpty() ?
                    Utils.propsToStringMap(Utils.loadProps(mirrorMakerPropsFile)) : Collections.emptyMap();

            MirrorMaker mirrorMaker = new MirrorMaker(mirrorMakerProps, Time.SYSTEM);
            
            try {
                mirrorMaker.start();
            } catch (Exception e) {
                log.error("Failed to start MirrorMaker", e);
                mirrorMaker.stop();
                Exit.exit(3);
            }

                        // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
            mirrorMaker.awaitStop();

        } catch (Throwable t) {
            log.error("Stopping due to error", t);
            Exit.exit(2);
        }
    }

}
