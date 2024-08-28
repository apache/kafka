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
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.consumer.internals.ConsumerNetworkClient;
import org.apache.kafka.clients.consumer.internals.RequestFuture;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
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
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

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
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        return AdminClient.create(props);
    }

    private static class BrokerVersionCommandOptions extends CommandDefaultOptions {
        private static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server to connect to.";
        private static final String COMMAND_CONFIG_DOC = "A property file containing configs to be passed to Admin Client.";

        final OptionSpec<String> commandConfigOpt;
        final OptionSpec<String> bootstrapServerOpt;

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
            options = parser.parse(args);
            checkArgs();
        }

        private void checkArgs() {
            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to retrieve broker version information.");
            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
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
                .define(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, CommonClientConfigs.BOOTSTRAP_SERVERS_DOC)
                .define(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG, ConfigDef.Type.STRING, ClientDnsLookup.USE_ALL_DNS_IPS.toString(), ConfigDef.ValidString.in(ClientDnsLookup.USE_ALL_DNS_IPS.toString(), ClientDnsLookup.RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY.toString()), ConfigDef.Importance.MEDIUM, CommonClientConfigs.CLIENT_DNS_LOOKUP_DOC)
                .define(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, ConfigDef.Type.STRING, CommonClientConfigs.DEFAULT_SECURITY_PROTOCOL, ConfigDef.CaseInsensitiveValidString.in(Utils.enumOptions(SecurityProtocol.class)), ConfigDef.Importance.MEDIUM, CommonClientConfigs.SECURITY_PROTOCOL_DOC)
                .define(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, ConfigDef.Type.INT, DEFAULT_REQUEST_TIMEOUT_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.REQUEST_TIMEOUT_MS_DOC)
                .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, ConfigDef.Type.LONG, CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_DOC)
                .define(CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, ConfigDef.Type.LONG, CommonClientConfigs.DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_DOC)
                .define(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG, ConfigDef.Type.LONG, DEFAULT_RETRY_BACKOFF_MS, ConfigDef.Importance.MEDIUM, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
                .withClientSslSupport()
                .withClientSaslSupport();

        private final Time time;
        private final ConsumerNetworkClient client;
        private final List<Node> bootstrapBrokers;

        static AdminClient create(Properties props) {
            return create(new AbstractConfig(ADMIN_CONFIG_DEF, props, false));
        }

        static AdminClient create(AbstractConfig config) {
            String clientId = "admin-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
            LogContext logContext = new LogContext("[LegacyAdminClient clientId=" + clientId + "] ");
            Time time = Time.SYSTEM;
            Metrics metrics = new Metrics(time);
            Metadata metadata = new Metadata(
                    CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MS,
                    CommonClientConfigs.DEFAULT_RETRY_BACKOFF_MAX_MS,
                    60 * 60 * 1000L, logContext,
                    new ClusterResourceListeners());
            metadata.bootstrap(ClientUtils.parseAndValidateAddresses(
                    config.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(CommonClientConfigs.CLIENT_DNS_LOOKUP_CONFIG)));
            Selector selector = new Selector(
                    DEFAULT_CONNECTION_MAX_IDLE_MS,
                    metrics,
                    time,
                    "admin",
                    ClientUtils.createChannelBuilder(config, time, logContext),
                    logContext);
            NetworkClient networkClient = new NetworkClient(
                    selector,
                    metadata,
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
            ConsumerNetworkClient highLevelClient = new ConsumerNetworkClient(
                    logContext,
                    networkClient,
                    metadata,
                    time,
                    config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG),
                    config.getInt(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG),
                    Integer.MAX_VALUE);
            return new AdminClient(time, highLevelClient, metadata.fetch().nodes());
        }

        AdminClient(Time time, ConsumerNetworkClient client, List<Node> bootstrapBrokers) {
            this.time = time;
            this.client = client;
            this.bootstrapBrokers = bootstrapBrokers;
        }

        private AbstractResponse send(Node target, AbstractRequest.Builder<?> request) {
            RequestFuture<ClientResponse> future = client.send(target, request);
            while (!future.isDone()) {
                client.poll(time.timer(DEFAULT_REQUEST_TIMEOUT_MS));
            }
            if (future.succeeded()) {
                return future.value().responseBody();
            } else {
                throw future.exception();
            }
        }

        private AbstractResponse sendAnyNode(AbstractRequest.Builder<?> request) {
            for (Node broker : bootstrapBrokers) {
                try {
                    return send(broker, request);
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
                ApiVersionsResponse response = (ApiVersionsResponse) send(node, new ApiVersionsRequest.Builder());
                Errors error = Errors.forCode(response.data().errorCode());
                if (error.exception() != null) {
                    future.completeExceptionally(error.exception());
                } else {
                    future.complete(new NodeApiVersions(response.data().apiKeys(), response.data().supportedFeatures(), response.data().zkMigrationReady()));
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
            MetadataResponse response = (MetadataResponse) sendAnyNode(MetadataRequest.Builder.allTopics());
            if (!response.errors().isEmpty()) {
                LOGGER.debug("Metadata request contained errors: {}", response.errors());
            }
            return response.buildCluster().nodes();
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
            try {
                client.close();
            } catch (IOException e) {
                LOGGER.error("Exception closing nioSelector:", e);
            }
        }
    }
}