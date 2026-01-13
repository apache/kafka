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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.internals.InternalDescribeFeaturesResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;

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
        Properties props = opts.options.has(opts.commandConfigOpt) ?
                Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) :
                new Properties();
        boolean usingBootstrapController = opts.options.has(opts.bootstrapControllerOpt);
        if (usingBootstrapController) {
            props.put(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, opts.options.valueOf(opts.bootstrapControllerOpt));
        } else {
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        }

        try (AdminClient adminClient = AdminClient.create(props)) {
            Collection<Node> nodes = adminClient.describeCluster().nodes().get();
            Map<Node, InternalDescribeFeaturesResult> nodeApiVersions = new TreeMap<>(Comparator.comparingInt(Node::id));
            nodes.forEach(node -> {
                    InternalDescribeFeaturesResult result = (InternalDescribeFeaturesResult) adminClient.describeFeatures(
                        new DescribeFeaturesOptions().nodeId(node.id()));
                    nodeApiVersions.put(node, result);
                }
            );

            nodeApiVersions.forEach((broker, future) -> {
                try {
                    NodeApiVersions apiVersions = future.nodeApiVersions().get();
                    System.out.print(broker + " -> " + apiVersions.toString(true) + "\n");
                } catch (Exception e) {
                    System.out.print(broker + " -> ERROR: " + e.getMessage() + "\n");
                }
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
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
}