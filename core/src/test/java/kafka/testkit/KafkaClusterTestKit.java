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

package kafka.testkit;

import kafka.server.BrokerServer;
import kafka.server.ControllerServer;
import kafka.server.FaultHandlerFactory;
import kafka.server.JointServer;
import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.KafkaRaftServer;
import kafka.server.MetaProperties;
import kafka.tools.StorageTool;
import kafka.utils.Logging;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
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
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;


@SuppressWarnings("deprecation") // Needed for Scala 2.12 compatibility
public class KafkaClusterTestKit implements AutoCloseable {
    private final static Logger log = LoggerFactory.getLogger(KafkaClusterTestKit.class);

    /**
     * This class manages a future which is completed with the proper value for
     * controller.quorum.voters once the randomly assigned ports for all the controllers are
     * known.
     */
    private static class ControllerQuorumVotersFutureManager implements AutoCloseable {
        private final int expectedControllers;
        private final CompletableFuture<Map<Integer, RaftConfig.AddressSpec>> future = new CompletableFuture<>();
        private final Map<Integer, Integer> controllerPorts = new TreeMap<>();

        ControllerQuorumVotersFutureManager(int expectedControllers) {
            this.expectedControllers = expectedControllers;
        }

        synchronized void registerPort(int nodeId, int port) {
            controllerPorts.put(nodeId, port);
            if (controllerPorts.size() >= expectedControllers) {
                future.complete(controllerPorts.entrySet().stream().
                    collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new RaftConfig.InetAddressSpec(new InetSocketAddress("localhost", entry.getValue()))
                    )));
            }
        }

        void fail(Throwable e) {
            future.completeExceptionally(e);
        }

        @Override
        public void close() {
            future.cancel(true);
        }
    }

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
        private TestKitNodes nodes;
        private Map<String, String> configProps = new HashMap<>();
        private SimpleFaultHandlerFactory faultHandlerFactory = new SimpleFaultHandlerFactory();

        public Builder(TestKitNodes nodes) {
            this.nodes = nodes;
        }

        public Builder setConfigProp(String key, String value) {
            this.configProps.put(key, value);
            return this;
        }

        public KafkaClusterTestKit build() throws Exception {
            Map<Integer, ControllerServer> controllers = new HashMap<>();
            Map<Integer, BrokerServer> brokers = new HashMap<>();
            Map<Integer, JointServer> jointServers = new HashMap<>();
            String uninitializedQuorumVotersString = nodes.controllerNodes().keySet().stream().
                map(controllerNode -> String.format("%d@0.0.0.0:0", controllerNode)).
                collect(Collectors.joining(","));
            /*
              Number of threads = Total number of brokers + Total number of controllers + Total number of Raft Managers
                                = Total number of brokers + Total number of controllers * 2
                                  (Raft Manager per broker/controller)
             */
            int numOfExecutorThreads = (nodes.brokerNodes().size() + nodes.controllerNodes().size()) * 2;
            ExecutorService executorService = null;
            ControllerQuorumVotersFutureManager connectFutureManager =
                new ControllerQuorumVotersFutureManager(nodes.controllerNodes().size());
            File baseDirectory = null;

            try {
                baseDirectory = TestUtils.tempDirectory();
                nodes = nodes.copyWithAbsolutePaths(baseDirectory.getAbsolutePath());
                executorService = Executors.newFixedThreadPool(numOfExecutorThreads,
                    ThreadUtils.createThreadFactory("KafkaClusterTestKit%d", false));
                for (ControllerNode node : nodes.controllerNodes().values()) {
                    Map<String, String> props = new HashMap<>(configProps);
                    props.put(KafkaConfig$.MODULE$.ProcessRolesProp(), roles(node.id()));
                    props.put(KafkaConfig$.MODULE$.NodeIdProp(),
                        Integer.toString(node.id()));
                    props.put(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                        node.metadataDirectory());
                    props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
                        "EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
                    props.put(KafkaConfig$.MODULE$.ListenersProp(), listeners(node.id()));
                    props.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(),
                        nodes.interBrokerListenerName().value());
                    props.put(KafkaConfig$.MODULE$.ControllerListenerNamesProp(),
                        "CONTROLLER");
                    // Note: we can't accurately set controller.quorum.voters yet, since we don't
                    // yet know what ports each controller will pick.  Set it to a dummy string \
                    // for now as a placeholder.
                    props.put(RaftConfig.QUORUM_VOTERS_CONFIG, uninitializedQuorumVotersString);

                    // reduce log cleaner offset map memory usage
                    props.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), "2097152");

                    setupNodeDirectories(baseDirectory, node.metadataDirectory(), Collections.emptyList());
                    KafkaConfig config = new KafkaConfig(props, false, Option.empty());

                    MetaProperties metaProperties = MetaProperties.apply(nodes.clusterId().toString(), node.id());
                    BootstrapMetadata bootstrapMetadata = BootstrapMetadata.
                        fromVersion(nodes.bootstrapMetadataVersion(), "testkit");
                    String threadNamePrefix = (nodes.brokerNodes().containsKey(node.id())) ?
                        String.format("colocated%d", node.id()) :
                            String.format("controller%d", node.id());
                    JointServer jointServer = new JointServer(config,
                            metaProperties,
                            Time.SYSTEM,
                            new Metrics(),
                            Option.apply(threadNamePrefix),
                            connectFutureManager.future,
                            faultHandlerFactory);
                    ControllerServer controller = null;
                    try {
                        controller = new ControllerServer(
                                jointServer,
                                KafkaRaftServer.configSchema(),
                                bootstrapMetadata);
                    } catch (Throwable e) {
                        log.error("Error creating controller {}", node.id(), e);
                        if (controller != null) controller.shutdown();
                        throw e;
                    }
                    controllers.put(node.id(), controller);
                    controller.socketServerFirstBoundPortFuture().whenComplete((port, e) -> {
                        if (e != null) {
                            connectFutureManager.fail(e);
                        } else {
                            connectFutureManager.registerPort(node.id(), port);
                        }
                    });
                    jointServers.put(node.id(), jointServer);
                }
                for (BrokerNode node : nodes.brokerNodes().values()) {
                    Map<String, String> props = new HashMap<>(configProps);
                    props.put(KafkaConfig$.MODULE$.ProcessRolesProp(), roles(node.id()));
                    props.put(KafkaConfig$.MODULE$.BrokerIdProp(),
                        Integer.toString(node.id()));
                    if (nodes.controllerNodes().containsKey(node.id())) {
                        // Combined mode: use the metadata log dir of the associated controller node.
                        props.put(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                                nodes.controllerNodes().get(node.id()).metadataDirectory());
                    } else {
                        // Separate mode: use the broker node's log dir.
                        props.put(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                                node.metadataDirectory());
                    }
                    props.put(KafkaConfig$.MODULE$.LogDirsProp(),
                        String.join(",", node.logDataDirectories()));
                    props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
                        "EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT");
                    props.put(KafkaConfig$.MODULE$.ListenersProp(), listeners(node.id()));
                    props.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(),
                        nodes.interBrokerListenerName().value());
                    props.put(KafkaConfig$.MODULE$.ControllerListenerNamesProp(),
                        "CONTROLLER");

                    setupNodeDirectories(baseDirectory, node.metadataDirectory(),
                        node.logDataDirectories());

                    // Just like above, we set a placeholder voter list here until we
                    // find out what ports the controllers picked.
                    props.put(RaftConfig.QUORUM_VOTERS_CONFIG, uninitializedQuorumVotersString);
                    props.putAll(node.propertyOverrides());
                    KafkaConfig config = new KafkaConfig(props, false, Option.empty());

                    JointServer jointServer = jointServers.get(node.id());
                    if (jointServer == null) {
                        MetaProperties metaProperties = MetaProperties.apply(nodes.clusterId().toString(), node.id());
                        jointServer = new JointServer(config,
                                metaProperties,
                                Time.SYSTEM,
                                new Metrics(),
                                Option.apply(String.format("broker%d_", node.id())),
                                connectFutureManager.future,
                                faultHandlerFactory);
                        jointServers.put(node.id(), jointServer);
                    }
                    BrokerServer broker = null;
                    try {
                        broker = new BrokerServer(
                                jointServer,
                                JavaConverters.asScalaBuffer(Collections.<String>emptyList()).toSeq());
                    } catch (Throwable e) {
                        log.error("Error creating broker {}", node.id(), e);
                        if (broker != null) broker.shutdown();
                        throw e;
                    }
                    brokers.put(node.id(), broker);
                }
            } catch (Exception e) {
                if (executorService != null) {
                    executorService.shutdownNow();
                    executorService.awaitTermination(5, TimeUnit.MINUTES);
                }
                for (BrokerServer brokerServer : brokers.values()) {
                    brokerServer.shutdown();
                }
                for (ControllerServer controller : controllers.values()) {
                    controller.shutdown();
                }
                connectFutureManager.close();
                if (baseDirectory != null) {
                    Utils.delete(baseDirectory);
                }
                throw e;
            }
            return new KafkaClusterTestKit(executorService,
                nodes,
                controllers,
                brokers,
                connectFutureManager,
                baseDirectory,
                faultHandlerFactory);
        }

        private String listeners(int node) {
            if (nodes.isCoResidentNode(node)) {
                return "EXTERNAL://localhost:0,CONTROLLER://localhost:0";
            }
            if (nodes.controllerNodes().containsKey(node)) {
                return "CONTROLLER://localhost:0";
            }
            return "EXTERNAL://localhost:0";
        }

        private String roles(int node) {
            if (nodes.isCoResidentNode(node)) {
                return "broker,controller";
            }
            if (nodes.controllerNodes().containsKey(node)) {
                return "controller";
            }
            return "broker";
        }

        static private void setupNodeDirectories(File baseDirectory,
                                                 String metadataDirectory,
                                                 Collection<String> logDataDirectories) throws Exception {
            Files.createDirectories(new File(baseDirectory, "local").toPath());
            Files.createDirectories(Paths.get(metadataDirectory));
            for (String logDataDirectory : logDataDirectories) {
                Files.createDirectories(Paths.get(logDataDirectory));
            }
        }
    }

    private final ExecutorService executorService;
    private final TestKitNodes nodes;
    private final Map<Integer, ControllerServer> controllers;
    private final Map<Integer, BrokerServer> brokers;
    private final ControllerQuorumVotersFutureManager controllerQuorumVotersFutureManager;
    private final File baseDirectory;
    private final SimpleFaultHandlerFactory faultHandlerFactory;

    private KafkaClusterTestKit(
        ExecutorService executorService,
        TestKitNodes nodes,
        Map<Integer, ControllerServer> controllers,
        Map<Integer, BrokerServer> brokers,
        ControllerQuorumVotersFutureManager controllerQuorumVotersFutureManager,
        File baseDirectory,
        SimpleFaultHandlerFactory faultHandlerFactory
    ) {
        this.executorService = executorService;
        this.nodes = nodes;
        this.controllers = controllers;
        this.brokers = brokers;
        this.controllerQuorumVotersFutureManager = controllerQuorumVotersFutureManager;
        this.baseDirectory = baseDirectory;
        this.faultHandlerFactory = faultHandlerFactory;
    }

    public void format() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Entry<Integer, ControllerServer> entry : controllers.entrySet()) {
                int nodeId = entry.getKey();
                ControllerServer controller = entry.getValue();
                formatNodeAndLog(nodes.controllerProperties(nodeId), controller.config().metadataLogDir(),
                    controller, futures::add);
            }
            for (Entry<Integer, BrokerServer> entry : brokers.entrySet()) {
                if (!controllers.containsKey(entry.getKey())) {
                    int nodeId = entry.getKey();
                    BrokerServer broker = entry.getValue();
                    formatNodeAndLog(nodes.brokerProperties(nodeId), broker.config().metadataLogDir(),
                            broker, futures::add);
                }
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

    private void formatNodeAndLog(MetaProperties properties, String metadataLogDir, Logging loggingMixin,
                                  Consumer<Future<?>> futureConsumer) {
        futureConsumer.accept(executorService.submit(() -> {
            try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                try (PrintStream out = new PrintStream(stream)) {
                    StorageTool.formatCommand(out,
                            JavaConverters.asScalaBuffer(Collections.singletonList(metadataLogDir)).toSeq(),
                            properties,
                            MetadataVersion.MINIMUM_BOOTSTRAP_VERSION,
                            false);
                } finally {
                    for (String line : stream.toString().split(String.format("%n"))) {
                        loggingMixin.info(() -> line);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
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

    public Properties controllerClientProperties() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        if (!controllers.isEmpty()) {
            Collection<Node> controllerNodes = RaftConfig.voterConnectionsToNodes(
                controllerQuorumVotersFutureManager.future.get());

            StringBuilder bld = new StringBuilder();
            String prefix = "";
            for (Node node : controllerNodes) {
                bld.append(prefix).append(node.id()).append('@');
                bld.append(node.host()).append(":").append(node.port());
                prefix = ",";
            }
            properties.setProperty(RaftConfig.QUORUM_VOTERS_CONFIG, bld.toString());
            properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                controllerNodes.stream().map(n -> n.host() + ":" + n.port()).
                    collect(Collectors.joining(",")));
        }
        return properties;
    }

    public Properties clientProperties() {
        return clientProperties(new Properties());
    }

    public Properties clientProperties(Properties configOverrides) {
        if (!brokers.isEmpty()) {
            StringBuilder bld = new StringBuilder();
            String prefix = "";
            for (Entry<Integer, BrokerServer> entry : brokers.entrySet()) {
                int brokerId = entry.getKey();
                BrokerServer broker = entry.getValue();
                ListenerName listenerName = nodes.externalListenerName();
                int port = broker.boundPort(listenerName);
                if (port <= 0) {
                    throw new RuntimeException("Broker " + brokerId + " does not yet " +
                        "have a bound port for " + listenerName + ".  Did you start " +
                        "the cluster yet?");
                }
                bld.append(prefix).append("localhost:").append(port);
                prefix = ",";
            }
            configOverrides.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bld.toString());
        }
        return configOverrides;
    }

    public Map<Integer, ControllerServer> controllers() {
        return controllers;
    }

    public Map<Integer, BrokerServer> brokers() {
        return brokers;
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
            controllerQuorumVotersFutureManager.close();

            // Note the shutdown order here is chosen to be consistent with
            // `KafkaRaftServer`. See comments in that class for an explanation.

            for (Entry<Integer, BrokerServer> entry : brokers.entrySet()) {
                int brokerId = entry.getKey();
                BrokerServer broker = entry.getValue();
                futureEntries.add(new SimpleImmutableEntry<>("broker" + brokerId,
                    executorService.submit(broker::shutdown)));
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
            executorService.shutdownNow();
            executorService.awaitTermination(5, TimeUnit.MINUTES);
        }
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
}
