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

import kafka.server.KafkaConfig;
import kafka.server.KafkaConfig$;
import kafka.server.Kip500Controller;
import kafka.server.MetaProperties;
import kafka.tools.StorageTool;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metalog.LocalLogManager;
import org.apache.kafka.test.TestUtils;
import scala.collection.JavaConverters;
import scala.compat.java8.OptionConverters;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@SuppressWarnings("deprecation") // Needed for Scala 2.12 compatibility
public class KafkaClusterTestKit implements AutoCloseable {
    /**
     * This class manages a future which is completed with the proper value for
     * controller.connect once the randomly assigned ports for all the controllers are
     * known.
     */
    private static class ControllerConnectFutureManager implements AutoCloseable {
        private final int expectedControllers;
        private final CompletableFuture<String> future = new CompletableFuture<>();
        private final Map<Integer, Integer> controllerPorts = new TreeMap<>();

        ControllerConnectFutureManager(int expectedControllers) {
            this.expectedControllers = expectedControllers;
        }

        synchronized void registerPort(int nodeId, int port) {
            controllerPorts.put(nodeId, port);
            if (controllerPorts.size() >= expectedControllers) {
                future.complete(controllerPorts.entrySet().stream().
                    map(e -> String.format("%d@localhost:%d", e.getKey(), e.getValue())).
                    collect(Collectors.joining(",")));
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

    public static class Builder {
        private final TestKitNodes nodes;
        private Map<String, String> configProps = new HashMap<>();

        public Builder(TestKitNodes nodes) {
            this.nodes = nodes;
        }

        public Builder setConfigProp(String key, String value) {
            this.configProps.put(key, value);
            return this;
        }

        public KafkaClusterTestKit build() throws Exception {
            Map<Integer, Kip500Controller> controllers = new HashMap<>();
            ExecutorService executorService = null;
            ControllerConnectFutureManager connectFutureManager =
                new ControllerConnectFutureManager(nodes.controllerNodes().size());
            File baseDirectory = null;
            try {
                baseDirectory = TestUtils.tempDirectory();
                executorService = Executors.newFixedThreadPool(4,
                    ThreadUtils.createThreadFactory("KafkaClusterTestKit%d", false));
                Time time = Time.SYSTEM;
                for (ControllerNode node : nodes.controllerNodes().values()) {
                    Map<String, String> props = new HashMap<>(configProps);
                    props.put(KafkaConfig$.MODULE$.ProcessRolesProp(), "controller");
                    props.put(KafkaConfig$.MODULE$.ControllerIdProp(),
                        Integer.toString(node.id()));
                    props.put(KafkaConfig$.MODULE$.MetadataLogDirProp(),
                        Paths.get(baseDirectory.getAbsolutePath(),
                            node.logDirectories().get(0)).toAbsolutePath().toString());
                    props.put(KafkaConfig$.MODULE$.ListenerSecurityProtocolMapProp(),
                        "CONTROLLER:PLAINTEXT");
                    props.put(KafkaConfig$.MODULE$.ListenersProp(),
                        "CONTROLLER://localhost:0");
                    props.put(KafkaConfig$.MODULE$.ControllerListenerNamesProp(),
                        "CONTROLLER");
                    // Note: we can't accurately set controller.connect yet, since we don't
                    // yet know what ports each controller will pick.  Set it to an
                    // empty string for now as a placeholder.
                    props.put(KafkaConfig$.MODULE$.ControllerConnectProp(), "");
                    KafkaConfig config = new KafkaConfig(props, false,
                        OptionConverters.toScala(Optional.empty()));

                    String threadNamePrefix = String.format("controller%d_", node.id());
                    LogContext logContext = new LogContext("[Controller id=" + node.id() + "] ");

                    LocalLogManager metaLogManager = new LocalLogManager(
                        logContext,
                        node.id(),
                        config.metadataLogDir(),
                        threadNamePrefix
                    );
                    MetaProperties properties = MetaProperties.apply(
                        nodes.clusterId(),
                        OptionConverters.toScala(Optional.empty()),
                        OptionConverters.toScala(Optional.of(node.id()))
                    );

                    Kip500Controller controller = new Kip500Controller(
                        properties,
                        config,
                        metaLogManager,
                        OptionConverters.toScala(Optional.empty()),
                        time,
                        new Metrics(),
                        OptionConverters.toScala(Optional.of(threadNamePrefix)),
                        connectFutureManager.future
                    );
                    controllers.put(node.id(), controller);
                    controller.socketServerFirstBoundPortFuture().whenComplete((port, e) -> {
                        if (e != null) {
                            connectFutureManager.fail(e);
                        } else {
                            connectFutureManager.registerPort(node.id(), port);
                        }
                    });
                }
            } catch (Exception e) {
                if (executorService != null) {
                    executorService.shutdownNow();
                    executorService.awaitTermination(1, TimeUnit.DAYS);
                }
                for (Kip500Controller controller : controllers.values()) {
                    controller.shutdown();
                }
                connectFutureManager.close();
                if (baseDirectory != null) {
                    Utils.delete(baseDirectory);
                }
                throw e;
            }
            return new KafkaClusterTestKit(executorService, nodes, controllers,
                connectFutureManager, baseDirectory);
        }
    }

    private final ExecutorService executorService;
    private final TestKitNodes nodes;
    private final Map<Integer, Kip500Controller> controllers;
    private final ControllerConnectFutureManager controllerConnectFutureManager;
    private final File baseDirectory;

    private KafkaClusterTestKit(ExecutorService executorService,
                                TestKitNodes nodes,
                                Map<Integer, Kip500Controller> controllers,
                                ControllerConnectFutureManager controllerConnectFutureManager,
                                File baseDirectory) {
        this.executorService = executorService;
        this.nodes = nodes;
        this.controllers = controllers;
        this.controllerConnectFutureManager = controllerConnectFutureManager;
        this.baseDirectory = baseDirectory;
    }

    public void format() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Map.Entry<Integer, Kip500Controller> entry : controllers.entrySet()) {
                int nodeId = entry.getKey();
                Kip500Controller controller = entry.getValue();

                MetaProperties properties = MetaProperties.apply(
                    nodes.clusterId(),
                    OptionConverters.toScala(Optional.empty()),
                    OptionConverters.toScala(Optional.of(nodeId))
                );

                futures.add(executorService.submit(() -> {
                    try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
                        try (PrintStream out = new PrintStream(stream)) {
                            StorageTool.formatCommand(out,
                                JavaConverters.asScalaBuffer(Collections.singletonList(
                                    controller.config().metadataLogDir())).toSeq(),
                                properties,
                                false);
                        } finally {
                            for (String line : stream.toString().split(String.format("%n"))) {
                                controller.info(() -> line);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
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

    public void startup() throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (Kip500Controller controller : controllers.values()) {
                futures.add(executorService.submit(() -> {
                    controller.startup();
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

    public Properties clientProperties() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        if (!controllers.isEmpty()) {
            Collection<Node> controllerNodes = JavaConverters.asJavaCollection(
                KafkaConfig$.MODULE$.controllerConnectStringsToNodes(
                    controllerConnectFutureManager.future.get()));
            StringBuilder bld = new StringBuilder();
            String prefix = "";
            for (Node node : controllerNodes) {
                bld.append(prefix).append(node.id()).append('@');
                bld.append(node.host()).append(":").append(node.port());
                prefix = ",";
            }
            properties.setProperty(KafkaConfig$.MODULE$.ControllerConnectProp(), bld.toString());
            properties.setProperty("bootstrap.servers",
                controllerNodes.stream().map(n -> n.host() + ":" + n.port()).
                    collect(Collectors.joining(",")));
        }
        return properties;
    }

    @Override
    public void close() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        try {
            controllerConnectFutureManager.close();
            for (Kip500Controller controller : controllers.values()) {
                futures.add(executorService.submit(() -> {
                    controller.shutdown();
                }));
            }
            for (Future<?> future: futures) {
                future.get();
            }
            Utils.delete(baseDirectory);
        } catch (Exception e) {
            for (Future<?> future: futures) {
                future.cancel(true);
            }
            throw e;
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(1, TimeUnit.DAYS);
        }
    }
}
