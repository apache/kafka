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
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;

import org.apache.kafka.connect.util.SharedTopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.ArgumentParsers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;
import java.io.File;

/**
 *  Entry point for "MirrorMaker 2.0".
 *  <p>
 *  MirrorMaker runs a set of Connectors between multiple clusters, in order to replicate data, configuration,
 *  ACL rules, and consumer group state.
 *  </p>
 *  <p>
 *  Configuration is via a top-level "mm2.properties" file, which supports per-cluster and per-replication
 *  sub-configs. Each source->target replication must be explicitly enabled. For example:
 *  </p>
 *  <pre>
 *    clusters = primary, backup
 *    primary.bootstrap.servers = vip1:9092
 *    backup.bootstrap.servers = vip2:9092
 *    primary->backup.enabled = true
 *    backup->primary.enabled = true
 *  </pre>
 *  <p>
 *  Run as follows:
 *  </p>
 *  <pre>
 *    ./bin/connect-mirror-maker.sh mm2.properties
 *  </pre>
 *  <p>
 *  Additional information and example configurations are provided in ./connect/mirror/README.md
 *  </p>
 */
public class MirrorMaker {
    private static final Logger log = LoggerFactory.getLogger(MirrorMaker.class);

    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60L;
    private static final ConnectorClientConfigOverridePolicy CLIENT_CONFIG_OVERRIDE_POLICY =
            new AllConnectorClientConfigOverridePolicy();

    private static final List<Class<?>> CONNECTOR_CLASSES = Arrays.asList(
        MirrorSourceConnector.class,
        MirrorHeartbeatConnector.class,
        MirrorCheckpointConnector.class);
 
    private final Map<SourceAndTarget, Herder> herders = new HashMap<>();
    private CountDownLatch startLatch;
    private CountDownLatch stopLatch;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final ShutdownHook shutdownHook;
    private final String advertisedBaseUrl;
    private final Time time;
    private final MirrorMakerConfig config;
    private final Set<String> clusters;
    private final Set<SourceAndTarget> herderPairs;

    /**
     * @param config    MM2 configuration from mm2.properties file
     * @param clusters  target clusters for this node. These must match cluster
     *                  aliases as defined in the config. If null or empty list,
     *                  uses all clusters in the config.
     * @param time      time source
     */
    public MirrorMaker(MirrorMakerConfig config, List<String> clusters, Time time) {
        log.debug("Kafka MirrorMaker instance created");
        this.time = time;
        this.advertisedBaseUrl = "NOTUSED";
        this.config = config;
        if (clusters != null && !clusters.isEmpty()) {
            this.clusters = new HashSet<>(clusters);
        } else {
            // default to all clusters
            this.clusters = config.clusters();
        }
        log.info("Targeting clusters {}", this.clusters);
        this.herderPairs = config.clusterPairs().stream()
            .filter(x -> this.clusters.contains(x.target()))
            .collect(Collectors.toSet());
        if (herderPairs.isEmpty()) {
            throw new IllegalArgumentException("No source->target replication flows.");
        }
        this.herderPairs.forEach(this::addHerder);
        shutdownHook = new ShutdownHook();
    }

    /**
     * @param config    MM2 configuration from mm2.properties file
     * @param clusters  target clusters for this node. These must match cluster
     *                  aliases as defined in the config. If null or empty list,
     *                  uses all clusters in the config.
     * @param time      time source
     */
    public MirrorMaker(Map<String, String> config, List<String> clusters, Time time) {
        this(new MirrorMakerConfig(config), clusters, time);
    }

    public MirrorMaker(Map<String, String> props, List<String> clusters) {
        this(props, clusters, Time.SYSTEM);
    }

    public MirrorMaker(Map<String, String> props) {
        this(props, null);
    }


    public void start() {
        log.info("Kafka MirrorMaker starting with {} herders.", herders.size());
        if (startLatch != null) {
            throw new IllegalStateException("MirrorMaker instance already started");
        }
        startLatch = new CountDownLatch(herders.size());
        stopLatch = new CountDownLatch(herders.size());
        Exit.addShutdownHook("mirror-maker-shutdown-hook", shutdownHook);
        for (Herder herder : herders.values()) {
            try {
                herder.start();
            } finally {
                startLatch.countDown();
            }
        }
        log.info("Configuring connectors...");
        herderPairs.forEach(this::configureConnectors);
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
            log.info("Kafka MirrorMaker stopped.");
        }
    }

    public void awaitStop() {
        try {
            stopLatch.await();
        } catch (InterruptedException e) {
            log.error("Interrupted waiting for MirrorMaker to shutdown");
        }
    }

    private void configureConnector(SourceAndTarget sourceAndTarget, Class<?> connectorClass) {
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

    private void configureConnectors(SourceAndTarget sourceAndTarget) {
        CONNECTOR_CLASSES.forEach(x -> configureConnector(sourceAndTarget, x));
    }

    private void addHerder(SourceAndTarget sourceAndTarget) {
        log.info("creating herder for " + sourceAndTarget.toString());
        Map<String, String> workerProps = config.workerConfig(sourceAndTarget);
        String advertisedUrl = advertisedBaseUrl + "/" + sourceAndTarget.source();
        String workerId = sourceAndTarget.toString();
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        DistributedConfig distributedConfig = new DistributedConfig(workerProps);
        String kafkaClusterId = distributedConfig.kafkaClusterId();
        String clientIdBase = ConnectUtils.clientIdBase(distributedConfig);
        // Create the admin client to be shared by all backing stores for this herder
        Map<String, Object> adminProps = new HashMap<>(distributedConfig.originals());
        ConnectUtils.addMetricsContextProperties(adminProps, distributedConfig, kafkaClusterId);
        SharedTopicAdmin sharedAdmin = new SharedTopicAdmin(adminProps);
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(sharedAdmin, () -> clientIdBase);
        offsetBackingStore.configure(distributedConfig);
        Worker worker = new Worker(workerId, time, plugins, distributedConfig, offsetBackingStore, CLIENT_CONFIG_OVERRIDE_POLICY);
        WorkerConfigTransformer configTransformer = worker.configTransformer();
        Converter internalValueConverter = worker.getInternalValueConverter();
        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(time, internalValueConverter, sharedAdmin, clientIdBase);
        statusBackingStore.configure(distributedConfig);
        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                distributedConfig,
                configTransformer,
                sharedAdmin,
                clientIdBase);
        // Pass the shared admin to the distributed herder as an additional AutoCloseable object that should be closed when the
        // herder is stopped. MirrorMaker has multiple herders, and having the herder own the close responsibility is much easier than
        // tracking the various shared admin objects in this class.
        // Do not provide a restClient to the DistributedHerder to indicate that request forwarding is disabled
        Herder herder = new DistributedHerder(distributedConfig, time, worker,
                kafkaClusterId, statusBackingStore, configBackingStore,
                advertisedUrl, null, CLIENT_CONFIG_OVERRIDE_POLICY, sharedAdmin);
        herders.put(sourceAndTarget, herder);
    }

    private class ShutdownHook extends Thread {
        @Override
        public void run() {
            try {
                if (!startLatch.await(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    log.error("Timed out in shutdown hook waiting for MirrorMaker startup to finish. Unable to shutdown cleanly.");
                }
            } catch (InterruptedException e) {
                log.error("Interrupted in shutdown hook while waiting for MirrorMaker startup to finish. Unable to shutdown cleanly.");
            } finally {
                MirrorMaker.this.stop();
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParser parser = ArgumentParsers.newArgumentParser("connect-mirror-maker");
        parser.description("MirrorMaker 2.0 driver");
        parser.addArgument("config").type(Arguments.fileType().verifyCanRead())
            .metavar("mm2.properties").required(true)
            .help("MM2 configuration file.");
        parser.addArgument("--clusters").nargs("+").metavar("CLUSTER").required(false)
            .help("Target cluster to use for this node.");
        Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(-1);
            return;
        }
        File configFile = ns.get("config");
        List<String> clusters = ns.getList("clusters");
        try {
            log.info("Kafka MirrorMaker initializing ...");

            Properties props = Utils.loadProps(configFile.getPath());
            Map<String, String> config = Utils.propsToStringMap(props);
            MirrorMaker mirrorMaker = new MirrorMaker(config, clusters, Time.SYSTEM);
            
            try {
                mirrorMaker.start();
            } catch (Exception e) {
                log.error("Failed to start MirrorMaker", e);
                mirrorMaker.stop();
                Exit.exit(3);
            }

            mirrorMaker.awaitStop();

        } catch (Throwable t) {
            log.error("Stopping due to error", t);
            Exit.exit(2);
        }
    }

}
