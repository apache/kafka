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
package org.apache.kafka.tools;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.EndpointType;
import org.apache.kafka.clients.admin.internals.AdminBootstrapAddresses;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

import static org.apache.kafka.clients.admin.KafkaAdminClient.parseDescribeClusterResponse;

public class BrokerApiVersionsCommand {
    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    public static void execute(String... args) throws IOException, InterruptedException {
        BrokerVersionCommandOptions opts = new BrokerVersionCommandOptions(args);
        try (AdminClient adminClient = createAdminClient(opts)) {
            adminClient.awaitBrokers();
            Map<Node, KafkaFuture<NodeApiVersions>> brokerMap = adminClient.listAllBrokerVersionInfo();
            brokerMap.forEach((broker, future) -> {
                try {
                    NodeApiVersions apiVersions = future.get();
                    System.out.print(broker + " -> " + apiVersions.toString(true) + "\n");
                } catch (Exception e) {
                    System.out.print(broker + " -> ERROR: " + e.getMessage() + "\n");
                }
            });
        }
    }

    private static AdminClient createAdminClient(BrokerVersionCommandOptions opts) throws IOException {
        Properties props = opts.options.has(opts.commandConfigOpt) ?
                Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) :
                new Properties();
        boolean usingBootstrapController = opts.options.has(opts.bootstrapControllerOpt);
        if (usingBootstrapController) {
            props.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, opts.options.valueOf(opts.bootstrapControllerOpt));
        } else {
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        }

        return AdminClient.create(props, usingBootstrapController);
    }

    private static class BrokerVersionCommandOptions extends CommandDefaultOptions {
        private static final String BOOTSTRAP_SERVER_DOC = "The server to connect to.";
        private static final String BOOTSTRAP_CONTROLLER_DOC = "The controller to connect to.";
        private static final String COMMAND_CONFIG_DOC = "A property file containing configs to be passed to Admin Client.";

        final OptionSpec<String> commandConfigOpt;
        final OptionSpec<String> bootstrapServerOpt;
        final OptionSpec<String> bootstrapControllerOpt;

        BrokerVersionCommandOptions(String[] args) {
            super(args);
            commandConfigOpt = parser.accepts("command-config", COMMAND_CONFIG_DOC)
                    .withRequiredArg()
                    .describedAs("command config property file")
                    .ofType(String.class);
            bootstrapServerOpt = parser.accepts("bootstrap-server", BOOTSTRAP_SERVER_DOC)
                    .withRequiredArg()
                    .describedAs("server(s) to use for bootstrapping")
                    .ofType(String.class);
            bootstrapControllerOpt = parser.accepts("bootstrap-controller", BOOTSTRAP_CONTROLLER_DOC)
                    .withRequiredArg()
                    .describedAs("controller(s) to use for bootstrapping")
                    .ofType(String.class);
            options = parser.parse(args);
            checkArgs();
        }

        private void checkArgs() {
            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to retrieve broker version information.");
            Optional<String> bootstrapServer = Optional.ofNullable(options.valueOf(bootstrapServerOpt));
            Optional<String> bootstrapController = Optional.ofNullable(options.valueOf(bootstrapControllerOpt));
            CommandLineUtils.initializeBootstrapProperties(new Properties(), bootstrapServer, bootstrapController);
        }
    }

    protected static class AdminClient implements AutoCloseable {
        private static final Logger LOGGER = LoggerFactory.getLogger(AdminClient.class);
        private static final int DEFAULT_CONNECTION_MAX_IDLE_MS = 9 * 60 * 1000;
        private static final int DEFAULT_REQUEST_TIMEOUT_MS = 5000;
        private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 100;
        private static final int DEFAULT_RECONNECT_BACKOFF_MS = 50;
        private static final int DEFAULT_RECONNECT_BACKOFF_MAX = 50;
        private static final int DEFAULT_SEND_BUFFER_BYTES = 128 * 1024;
        private static final int DEFAULT_RECEIVE_BUFFER_BYTES = 32 * 1024;
        private static final int DEFAULT_RETRY_BACKOFF_MS = 100;

        private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
        private static final ConfigDef ADMIN_CONFIG_DEF = new ConfigDef()
                .define(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, ConfigDef.Type.LIST, List.of(), ConfigDef.ValidList.anyNonDuplicateValues(true, false), ConfigDef.Importance.HIGH, AdminClientConfig.BOOTSTRAP_CONTROLLERS_DOC)
                .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, List.of(), ConfigDef.ValidList.anyNonDuplicateValues(true, false), ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, ConfigDef.Type.STRING, ClientDnsLookup.USE_ALL_DNS_IPS.toString(), ConfigDef.ValidString.in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(), ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()), ConfigDef.Importance.MEDIUM, CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, ConfigDef.Type.STRING, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(SecurityProtocol.class)), ConfigDef.Importance.MEDIUM, CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, DEFAULT_REQUEST_TIMEOUT_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
                .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, ConfigDef.Type.LONG, CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, ConfigDef.Type.LONG, CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, DEFAULT_RETRY_BACKOFF_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();

        private final Time time;
        private final NetworkClient client;
        private final List<Node> bootstrapBrokers;
        private final boolean usingBootstrapController;
        private final AdminMetadataUpder adminMetadataUpder;
        // store the sync response temperature
        private ClientResponse clientResponse;

        static AdminClient create(Properties props, boolean usingBootstrapController) {
            return create(new AbstractConfig(ADMIN_CONFIG_DEF, props, false), usingBootstrapController);
        }

        static AdminClient create(AbstractConfig config, boolean usingBootstrapController) {
            String clientId = "admin-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
            LogContext logContext = new LogContext("[LegacyAdminClient clientId=" + clientId + "] ");
            Time time = Time.SYSTEM;
            Metrics metrics = new Metrics(time);

            Cluster cluster = Cluster.bootstrap(AdminBootstrapAddresses.fromConfig(config).addresses());

            Selector selector = new Selector(
                    DEFAULT_CONNECTION_MAX_IDLE_MS,
                    metrics,
                    time,
                    "admin",
                    ClientUtils.createChannelBuilder(config, time, logContext),
                    logContext);
            AdminMetadataUpder adminMetadataUpder = new AdminMetadataUpder(cluster);
            NetworkClient networkClient = new NetworkClient(
                    selector,
                    adminMetadataUpder,
                    clientId,
                    DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
                    DEFAULT_RECONNECT_BACKOFF_MS,
                    DEFAULT_RECONNECT_BACKOFF_MAX,
                    DEFAULT_SEND_BUFFER_BYTES,
                    DEFAULT_RECEIVE_BUFFER_BYTES,
                    config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG),
                    config.getLong(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG),
                    time,
                    true,
                    new ApiVersions(),
                    logContext,
                    MetadataRecoveryStrategy.NONE);

            return new AdminClient(time, networkClient, cluster.nodes(), usingBootstrapController, adminMetadataUpder);
        }

        AdminClient(Time time, NetworkClient client, List<Node> bootstrapBrokers, boolean usingBootstrapController, AdminMetadataUpder adminMetadataUpder) {
            this.time = time;
            this.client = client;
            this.bootstrapBrokers = bootstrapBrokers;
            this.usingBootstrapController = usingBootstrapController;
            this.adminMetadataUpder = adminMetadataUpder;
        }

        private AbstractResponse sendSync(Node target, AbstractRequest.Builder<?> request, boolean waitConnection) {
            long now = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(target.idString(), request, now, true, DEFAULT_REQUEST_TIMEOUT_MS,
                    response -> this.clientResponse = response
            );

            if (waitConnection) {
                awaitConnect(target, now);
            }

            client.send(clientRequest, now);

            while (clientResponse == null) {
                client.poll(DEFAULT_REQUEST_TIMEOUT_MS, now);
            }

            AbstractResponse abstractResponse = clientResponse.responseBody();
            this.clientResponse = null;

            return abstractResponse;
        }

        private void awaitConnectAllBroker(List<Node> nodes, long now) {
            for (Node node : nodes) {
                awaitConnect(node, now);
            }
        }

        private void awaitConnect(Node node, long now) {
            while (!client.ready(node, now)) {
                client.poll(100, now);
            }
        }

        private AbstractResponse sendAnyNode(AbstractRequest.Builder<?> request) {
            for (Node broker : bootstrapBrokers) {
                try {
                    return sendSync(broker, request, true);
                } catch (AuthenticationException e) {
                    throw e;
                } catch (Exception e) {
                    LOGGER.debug("Request {} failed against node {}", request.apiKey(), broker, e);
                }
            }
            throw new RuntimeException("Request " + request.apiKey() + " failed on brokers " + bootstrapBrokers);
        }

        protected KafkaFuture<NodeApiVersions> getNodeApiVersions(Node node) {
            final KafkaFutureImpl<NodeApiVersions> future = new KafkaFutureImpl<>();
            try {
                ApiVersionsResponse response = (ApiVersionsResponse) sendSync(node, new ApiVersionsRequest.Builder(), false);
                Errors error = Errors.forCode(response.data().errorCode());
                if (error.exception() != null) {
                    future.completeExceptionally(error.exception());
                } else {
                    future.complete(new NodeApiVersions(response.data().apiKeys(), response.data().supportedFeatures()));
                }
            } catch (Exception e) {
                future.completeExceptionally(e);
            }

            return future;
        }

        public void awaitBrokers() throws InterruptedException {
            List<Node> nodes;
            do {
                nodes = findAllBrokers();
                if (nodes.isEmpty()) {
                    TimeUnit.MILLISECONDS.sleep(50);
                }
            } while (nodes.isEmpty());
        }

        private List<Node> findAllBrokers() {
            List<Node> nodes;
            if (usingBootstrapController) {
                DescribeClusterResponse response = (DescribeClusterResponse) sendAnyNode(new DescribeClusterRequest.Builder(
                        new DescribeClusterRequestData()
                        .setIncludeClusterAuthorizedOperations(false)
                        .setEndpointType(EndpointType.CONTROLLER.id())));
                Cluster cluster = parseDescribeClusterResponse(response.data());
                adminMetadataUpder.updateCluster(cluster);
                nodes = cluster.nodes();
            } else {
                MetadataResponse response = (MetadataResponse) sendAnyNode(MetadataRequest.Builder.allTopics());
                if (!response.errors().isEmpty()) {
                    LOGGER.debug("Metadata request contained errors: {}", response.errors());
                }
                nodes = response.buildCluster().nodes();
            }

            awaitConnectAllBroker(nodes, time.milliseconds());
            return nodes;
        }

        public Map<Node, KafkaFuture<NodeApiVersions>> listAllBrokerVersionInfo() {
            return findAllBrokers().stream()
                    .collect(Collectors.toMap(
                            broker -> broker,
                            this::getNodeApiVersions
                    ));
        }

        @Override
        public void close() {
            client.close();
        }
    }

    private static class AdminMetadataUpder implements MetadataUpdater {
        private Cluster cluster;


        public AdminMetadataUpder(Cluster cluster) {
            this.cluster = cluster;
        }

        public void updateCluster(Cluster cluster) {
            this.cluster = cluster;
        }

        @Override
        public List<Node> fetchNodes() {
            return cluster.nodes();
        }

        @Override
        public boolean isUpdateDue(long now) {
            return false;
        }

        @Override
        public long maybeUpdate(long now) {
            return Long.MAX_VALUE;
        }

        @Override
        public void handleServerDisconnect(long now, String nodeId, Optional<AuthenticationException> maybeFatalException) {
            if (maybeFatalException.isPresent()) {
                throw maybeFatalException.get();
            }

        }

        @Override
        public void handleFailedRequest(long now, Optional<KafkaException> maybeFatalException) {
            // Not used in this context
        }

        @Override
        public void handleSuccessfulResponse(RequestHeader requestHeader, long now, MetadataResponse metadataResponse) {
            // Not used in this context
        }


        @Override
        public void close() { }
    }
}