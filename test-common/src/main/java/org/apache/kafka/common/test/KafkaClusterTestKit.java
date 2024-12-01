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

package org.apache.kafka.common.test;

import kafka.raft.KafkaRaftManager;
import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.FaultHandlerFactory;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRaftServer;
import kafka.server.SharedServer;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.network.SocketServerConfigs;
import org.apache.kafka.raft.DynamicVoters;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.storage.internals.log.CleanerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.server.config.ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.LOG_DIRS_CONFIG;


public class KafkaClusterTestKit implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(KafkaClusterTestKit.class);

    static class SimpleFaultHandlerFactory implements FaultHandlerFactory {
        private final MockFaultHandler fatalFaultHandler = new MockFaultHandler("fatalFaultHandler");
        private final MockFaultHandler nonFatalFaultHandler = new MockFaultHandler("nonFatalFaultHandler");

        MockFaultHandler fatalFaultHandler() {
            return fatalFaultHandler;
        }

        MockFaultHandler nonFatalFaultHandler() {
            return nonFatalFaultHandler;
        }

        @Override
        public FaultHandler build(String name, boolean fatal, Runnable action) {
            if (fatal) {
                return fatalFaultHandler;
            } else {
                return nonFatalFaultHandler;
            }
        }
    }

    public static class Builder {
        private final TestKitNodes nodes;
        private final Map<String, Object> configProps = new HashMap<>();
        private final SimpleFaultHandlerFactory faultHandlerFactory = new SimpleFaultHandlerFactory();
        private final PreboundSocketFactoryManager socketFactoryManager = new PreboundSocketFactoryManager();
        private final String brokerListenerName;
        private final String controllerListenerName;
        private final String brokerSecurityProtocol;
        private final String controllerSecurityProtocol;

        public Builder(TestKitNodes nodes) {
            this.nodes = nodes;
            this.brokerListenerName = nodes.brokerListenerName().value();
            this.controllerListenerName = nodes.controllerListenerName().value();
            this.brokerSecurityProtocol = nodes.brokerListenerProtocol().name;
            this.controllerSecurityProtocol = nodes.controllerListenerProtocol().name;
        }

        public Builder setConfigProp(String key, Object value) {
            this.configProps.put(key, value);
            return this;
        }

        private KafkaConfig createNodeConfig(TestKitNode node) throws IOException {
            TestKitNode brokerNode = nodes.brokerNodes().get(node.id());
            TestKitNode controllerNode = nodes.controllerNodes().get(node.id());

            Map<String, Object> props = new HashMap<>(configProps);
            props.put(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG,
                    Long.toString(TimeUnit.MINUTES.toMillis(10)));
            props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, roles(node.id()));
            props.put(KRaftConfigs.NODE_ID_CONFIG,
                    Integer.toString(node.id()));
            // In combined mode, always prefer the metadata log directory of the controller node.
            if (controllerNode != null) {
                props.put(KRaftConfigs.METADATA_LOG_DIR_CONFIG,
                        controllerNode.metadataDirectory());
            } else {
                props.put(KRaftConfigs.METADATA_LOG_DIR_CONFIG,
                        node.metadataDirectory());
            }
            if (brokerNode != null) {
                // Set the log.dirs according to the broker node setting (if there is a broker node)
                props.put(LOG_DIRS_CONFIG,
                        String.join(",", brokerNode.logDataDirectories()));
            } else {
                // Set log.dirs equal to the metadata directory if there is just a controller.
                props.put(LOG_DIRS_CONFIG,
                    controllerNode.metadataDirectory());
            }

            // We allow configuring the listeners and related properties via Builder::setConfigProp,
            // and they shouldn't be overridden here
            props.putIfAbsent(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, String.format("%s:%s,%s:%s",
                    brokerListenerName, brokerSecurityProtocol, controllerListenerName, controllerSecurityProtocol));
            props.putIfAbsent(SocketServerConfigs.LISTENERS_CONFIG, listeners(node.id()));
            props.putIfAbsent(INTER_BROKER_LISTENER_NAME_CONFIG, brokerListenerName);
            props.putIfAbsent(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, controllerListenerName);

            StringBuilder quorumVoterStringBuilder = new StringBuilder();
            String prefix = "";
            for (int nodeId : nodes.controllerNodes().keySet()) {
                quorumVoterStringBuilder.append(prefix).
                    append(nodeId).
                    append("@").
                    append("localhost").
                    append(":").
                    append(socketFactoryManager.getOrCreatePortForListener(nodeId, controllerListenerName));
                prefix = ",";
            }
            props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, quorumVoterStringBuilder.toString());

            // reduce log cleaner offset map memory usage
            props.putIfAbsent(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152");

            // Add associated broker node property overrides
            if (brokerNode != null) {
                props.putAll(brokerNode.propertyOverrides());
            }
            // Add associated controller node property overrides
            if (controllerNode != null) {
                props.putAll(controllerNode.propertyOverrides());
            }
            props.putIfAbsent(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true");
            props.putIfAbsent(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
            return new KafkaConfig(props, false);
        }

        public KafkaClusterTestKit build() throws Exception {
            Map<Integer, ControllerServer> controllers = new HashMap<>();
            Map<Integer, BrokerServer> brokers = new HashMap<>();
            Map<Integer, SharedServer> jointServers = new HashMap<>();
            File baseDirectory = null;

            try {
                baseDirectory = new File(nodes.baseDirectory());
                for (TestKitNode node : nodes.controllerNodes().values()) {
                    socketFactoryManager.getOrCreatePortForListener(node.id(), controllerListenerName);
                }
                for (TestKitNode node : nodes.controllerNodes().values()) {
                    setupNodeDirectories(baseDirectory, node.metadataDirectory(), Collections.emptyList());
                    KafkaConfig config = createNodeConfig(node);
                    SharedServer sharedServer = new SharedServer(
                        config,
                        node.initialMetaPropertiesEnsemble(),
                        Time.SYSTEM,
                        new Metrics(),
                        CompletableFuture.completedFuture(QuorumConfig.parseVoterConnections(config.quorumConfig().voters())),
                        Collections.emptyList(),
                        faultHandlerFactory,
                        socketFactoryManager.getOrCreateSocketFactory(node.id())
                    );
                    ControllerServer controller = null;
                    try {
                        controller = new ControllerServer(
                                sharedServer,
                                KafkaRaftServer.configSchema(),
                                nodes.bootstrapMetadata());
                    } catch (Throwable e) {
                        log.error("Error creating controller {}", node.id(), e);
                        Utils.swallow(log, Level.WARN, "sharedServer.stopForController error", sharedServer::stopForController);
                        throw e;
                    }
                    controllers.put(node.id(), controller);
                    jointServers.put(node.id(), sharedServer);
                }
                for (TestKitNode node : nodes.brokerNodes().values()) {
                    SharedServer sharedServer = jointServers.get(node.id());
                    if (sharedServer == null) {
                        KafkaConfig config = createNodeConfig(node);
                        sharedServer = new SharedServer(
                            config,
                            node.initialMetaPropertiesEnsemble(),
                            Time.SYSTEM,
                            new Metrics(),
                            CompletableFuture.completedFuture(QuorumConfig.parseVoterConnections(config.quorumConfig().voters())),
                            Collections.emptyList(),
                            faultHandlerFactory,
                            socketFactoryManager.getOrCreateSocketFactory(node.id())
                        );
                        jointServers.put(node.id(), sharedServer);
                    }
                    BrokerServer broker = null;
                    try {
                        broker = new BrokerServer(sharedServer);
                    } catch (Throwable e) {
                        log.error("Error creating broker {}", node.id(), e);
                        Utils.swallow(log, Level.WARN, "sharedServer.stopForBroker error", sharedServer::stopForBroker);
                        throw e;
                    }
                    brokers.put(node.id(), broker);
                }
            } catch (Exception e) {
                for (BrokerServer brokerServer : brokers.values()) {
                    brokerServer.shutdown();
                }
                for (ControllerServer controller : controllers.values()) {
                    controller.shutdown();
                }
                if (baseDirectory != null) {
                    Utils.delete(baseDirectory);
                }
                socketFactoryManager.close();
                throw e;
            }
            return new KafkaClusterTestKit(
                    nodes,
                    controllers,
                    brokers,
                    baseDirectory,
                    faultHandlerFactory,
                    socketFactoryManager);
        }

        private String listeners(int node) {
            if (nodes.isCombined(node)) {
                return String.format("%s://localhost:0,%s://localhost:0", brokerListenerName, controllerListenerName);
            }
            if (nodes.controllerNodes().containsKey(node)) {
                return String.format("%s://localhost:0", controllerListenerName);
            }
            return String.format("%s://localhost:0", brokerListenerName);
        }

        private String roles(int node) {
            if (nodes.isCombined(node)) {
                return "broker,controller";
            }
            if (nodes.controllerNodes().containsKey(node)) {
                return "controller";
            }
            return "broker";
        }

        private static void setupNodeDirectories(File baseDirectory,
                                                 String metadataDirectory,
                                                 Collection<String> logDataDirectories) throws Exception {
            Files.createDirectories(new File(baseDirectory, "local").toPath());
            Files.createDirectories(Paths.get(metadataDirectory));
            for (String logDataDirectory : logDataDirectories) {
                Files.createDirectories(Paths.get(logDataDirectory));
            }
        }
    }

    private static final String KAFKA_CLUSTER_THREAD_PREFIX = "kafka-cluster-test-kit-";
    private final ExecutorService executorService;
    private final KafkaClusterThreadFactory threadFactory = new KafkaClusterThreadFactory(KAFKA_CLUSTER_THREAD_PREFIX);
    private final TestKitNodes nodes;
    private final Map<Integer, ControllerServer> controllers;
    private final Map<Integer, BrokerServer> brokers;
    private final File baseDirectory;
    private final SimpleFaultHandlerFactory faultHandlerFactory;
    private final PreboundSocketFactoryManager socketFactoryManager;
    private final String controllerListenerName;

    private KafkaClusterTestKit(
        TestKitNodes nodes,
        Map<Integer, ControllerServer> controllers,
        Map<Integer, BrokerServer> brokers,
        File baseDirectory,
        SimpleFaultHandlerFactory faultHandlerFactory,
        PreboundSocketFactoryManager socketFactoryManager
    ) {
        /*
          Number of threads = Total number of brokers + Total number of controllers + Total number of Raft Managers
                            = Total number of brokers + Total number of controllers * 2
                              (Raft Manager per broker/controller)
        */
        int numOfExecutorThreads = (nodes.brokerNodes().size() + nodes.controllerNodes().size()) * 2;
        this.executorService = Executors.newFixedThreadPool(numOfExecutorThreads, threadFactory);
        this.nodes = nodes;
        this.controllers = controllers;
        this.brokers = brokers;
        this.baseDirectory = baseDirectory;
        this.faultHandlerFactory = faultHandlerFactory;
        this.socketFactoryManager = socketFactoryManager;
        this.controllerListenerName = nodes.controllerListenerName().value();
    }

    public void format() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (ControllerServer controller : controllers.values()) {
                futures.add(executorService.submit(() -> {
                    formatNode(controller.sharedServer().metaPropsEnsemble(), true);
                }));
            }
            for (Entry<Integer, BrokerServer> entry : brokers.entrySet()) {
                BrokerServer broker = entry.getValue();
                futures.add(executorService.submit(() -> {
                    formatNode(broker.sharedServer().metaPropsEnsemble(),
                        !nodes.isCombined(nodes().brokerNodes().get(entry.getKey()).id()));
                }));
            }
            for (Future<?> future: futures) {
                future.get();
            }
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        }
    }

    private void formatNode(
        MetaPropertiesEnsemble ensemble,
        boolean writeMetadataDirectory
    ) {
        try {
            Formatter formatter = new Formatter();
            formatter.setNodeId(ensemble.nodeId().getAsInt());
            formatter.setClusterId(ensemble.clusterId().get());
            if (writeMetadataDirectory) {
                formatter.setDirectories(ensemble.logDirProps().keySet());
            } else {
                formatter.setDirectories(ensemble.logDirProps().keySet().stream().
                    filter(d -> !ensemble.metadataLogDir().get().equals(d)).
                    collect(Collectors.toSet()));
            }
            if (formatter.directories().isEmpty()) {
                return;
            }
            formatter.setReleaseVersion(nodes.bootstrapMetadata().metadataVersion());
            formatter.setFeatureLevel(KRaftVersion.FEATURE_NAME,
                nodes.bootstrapMetadata().featureLevel(KRaftVersion.FEATURE_NAME));
            formatter.setUnstableFeatureVersionsEnabled(true);
            formatter.setIgnoreFormatted(false);
            formatter.setControllerListenerName(controllerListenerName);
            if (writeMetadataDirectory) {
                formatter.setMetadataLogDirectory(ensemble.metadataLogDir().get());
            } else {
                formatter.setMetadataLogDirectory(Optional.empty());
            }
            if (nodes.bootstrapMetadata().featureLevel(KRaftVersion.FEATURE_NAME) > 0) {
                StringBuilder dynamicVotersBuilder = new StringBuilder();
                String prefix = "";
                for (TestKitNode controllerNode : nodes.controllerNodes().values()) {
                    int port = socketFactoryManager.
                        getOrCreatePortForListener(controllerNode.id(), controllerListenerName);
                    dynamicVotersBuilder.append(prefix);
                    prefix = ",";
                    dynamicVotersBuilder.append(String.format("%d@localhost:%d:%s",
                        controllerNode.id(), port, controllerNode.metadataDirectoryId()));
                }
                formatter.setInitialControllers(DynamicVoters.parse(dynamicVotersBuilder.toString()));
            }
            formatter.run();
        } catch (Exception e) {
            throw new RuntimeException("Failed to format node " + ensemble.nodeId(), e);
        }
    }

    public void startup() throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            // Note the startup order here is chosen to be consistent with
            // `KafkaRaftServer`. See comments in that class for an explanation.
            for (ControllerServer controller : controllers.values()) {
                futures.add(executorService.submit(controller::startup));
            }
            for (BrokerServer broker : brokers.values()) {
                futures.add(executorService.submit(broker::startup));
            }
            for (Future<?> future: futures) {
                future.get();
            }
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        }
    }

    /**
     * Wait for a controller to mark all the brokers as ready (registered and unfenced).
     * And also wait for the metadata cache up-to-date in each broker server.
     */
    public void waitForReadyBrokers() throws ExecutionException, InterruptedException {
        // We can choose any controller, not just the active controller.
        // If we choose a standby controller, we will wait slightly longer.
        ControllerServer controllerServer = controllers.values().iterator().next();
        Controller controller = controllerServer.controller();
        controller.waitForReadyBrokers(brokers.size()).get();

        // make sure metadata cache in each broker server is up-to-date
        TestUtils.waitForCondition(() ->
                brokers().values().stream().allMatch(brokerServer -> brokerServer.metadataCache().getAliveBrokers().size() == brokers.size()),
            "Failed to wait for publisher to publish the metadata update to each broker.");
    }

    public class ClientPropertiesBuilder {
        private Properties properties;
        private boolean usingBootstrapControllers = false;

        public ClientPropertiesBuilder() {
            this.properties = new Properties();
        }

        public ClientPropertiesBuilder(Properties properties) {
            this.properties = properties;
        }

        public ClientPropertiesBuilder setUsingBootstrapControllers(boolean usingBootstrapControllers) {
            this.usingBootstrapControllers = usingBootstrapControllers;
            return this;
        }

        public Properties build() {
            if (usingBootstrapControllers) {
                properties.setProperty(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, bootstrapControllers());
                properties.remove(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
            } else {
                properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
                properties.remove(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG);
            }
            return properties;
        }
    }

    public ClientPropertiesBuilder newClientPropertiesBuilder(Properties properties) {
        return new ClientPropertiesBuilder(properties);
    }

    public ClientPropertiesBuilder newClientPropertiesBuilder() {
        return new ClientPropertiesBuilder();
    }

    public Properties clientProperties() {
        return new ClientPropertiesBuilder().build();
    }

    public String bootstrapServers() {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (Entry<Integer, BrokerServer> entry : brokers.entrySet()) {
            int brokerId = entry.getKey();
            BrokerServer broker = entry.getValue();
            ListenerName listenerName = nodes.brokerListenerName();
            int port = broker.boundPort(listenerName);
            if (port <= 0) {
                throw new RuntimeException("Broker " + brokerId + " does not yet " +
                    "have a bound port for " + listenerName + ".  Did you start " +
                    "the cluster yet?");
            }
            bld.append(prefix).append("localhost:").append(port);
            prefix = ",";
        }
        return bld.toString();
    }

    public String bootstrapControllers() {
        StringBuilder bld = new StringBuilder();
        String prefix = "";
        for (Entry<Integer, ControllerServer> entry : controllers.entrySet()) {
            int id = entry.getKey();
            ControllerServer controller = entry.getValue();
            ListenerName listenerName = nodes.controllerListenerName();
            int port = controller.socketServer().boundPort(listenerName);
            if (port <= 0) {
                throw new RuntimeException("Controller " + id + " does not yet " +
                        "have a bound port for " + listenerName + ".  Did you start " +
                        "the cluster yet?");
            }
            bld.append(prefix).append("localhost:").append(port);
            prefix = ",";
        }
        return bld.toString();
    }

    public Map<Integer, ControllerServer> controllers() {
        return controllers;
    }

    public Controller waitForActiveController() throws InterruptedException {
        AtomicReference<Controller> active = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(() -> {
            for (ControllerServer controllerServer : controllers.values()) {
                if (controllerServer.controller().isActive()) {
                    active.set(controllerServer.controller());
                }
            }
            if (active.get() == null)
                throw new RuntimeException("Controller not active");
        });
        return active.get();
    }

    public Map<Integer, BrokerServer> brokers() {
        return brokers;
    }

    public Map<Integer, KafkaRaftManager<ApiMessageAndVersion>> raftManagers() {
        Map<Integer, KafkaRaftManager<ApiMessageAndVersion>> results = new HashMap<>();
        for (BrokerServer brokerServer : brokers().values()) {
            results.put(brokerServer.config().brokerId(), brokerServer.sharedServer().raftManager());
        }
        for (ControllerServer controllerServer : controllers().values()) {
            if (!results.containsKey(controllerServer.config().nodeId())) {
                results.put(controllerServer.config().nodeId(), controllerServer.sharedServer().raftManager());
            }
        }
        return results;
    }

    public TestKitNodes nodes() {
        return nodes;
    }

    public MockFaultHandler fatalFaultHandler() {
        return faultHandlerFactory.fatalFaultHandler();
    }

    public MockFaultHandler nonFatalFaultHandler() {
        return faultHandlerFactory.nonFatalFaultHandler();
    }

    @Override
    public void close() throws Exception {
        List<Entry<String, Future<?>>> futureEntries = new ArrayList<>();
        try {
            // Note the shutdown order here is chosen to be consistent with
            // `KafkaRaftServer`. See comments in that class for an explanation.
            for (Entry<Integer, BrokerServer> entry : brokers.entrySet()) {
                int brokerId = entry.getKey();
                BrokerServer broker = entry.getValue();
                futureEntries.add(new SimpleImmutableEntry<>("broker" + brokerId,
                    executorService.submit((Runnable) broker::shutdown)));
            }
            waitForAllFutures(futureEntries);
            futureEntries.clear();
            for (Entry<Integer, ControllerServer> entry : controllers.entrySet()) {
                int controllerId = entry.getKey();
                ControllerServer controller = entry.getValue();
                futureEntries.add(new SimpleImmutableEntry<>("controller" + controllerId,
                    executorService.submit(controller::shutdown)));
            }
            waitForAllFutures(futureEntries);
            futureEntries.clear();
            Utils.delete(baseDirectory);
        } catch (Exception e) {
            for (Entry<String, Future<?>> entry : futureEntries) {
                entry.getValue().cancel(true);
            }
            throw e;
        } finally {
            ThreadUtils.shutdownExecutorServiceQuietly(executorService, 5, TimeUnit.MINUTES);
            socketFactoryManager.close();
        }
        waitForAllThreads();
        faultHandlerFactory.fatalFaultHandler().maybeRethrowFirstException();
        faultHandlerFactory.nonFatalFaultHandler().maybeRethrowFirstException();
    }

    private void waitForAllFutures(List<Entry<String, Future<?>>> futureEntries)
            throws Exception {
        for (Entry<String, Future<?>> entry : futureEntries) {
            log.debug("waiting for {} to shut down.", entry.getKey());
            entry.getValue().get();
            log.debug("{} successfully shut down.", entry.getKey());
        }
    }

    private void waitForAllThreads() throws InterruptedException {
        TestUtils.waitForCondition(() -> Thread.getAllStackTraces().keySet()
                    .stream().noneMatch(t -> threadFactory.getThreadIds().contains(t.getId())),
                "Failed to wait for all threads to shut down.");
    }
}
