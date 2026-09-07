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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ControllerIdNotRegisteredException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;

import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

public class ClusterTool {

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println(e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("kafka-cluster")
                .defaultHelp(true)
                .description("The Kafka cluster tool.");
        Subparsers subparsers = parser.addSubparsers().dest("command");

        Subparser clusterIdParser = subparsers.addParser("cluster-id")
                .help("Get information about the ID of a cluster.");
        Subparser unregisterParser = subparsers.addParser("unregister")
                .help("Unregister a broker.");
        Subparser listEndpoints = subparsers.addParser("list-endpoints")
                .help("List endpoints");
        // Aiven fork addition (KAFKA-20295). Deliberately not "unregister-controller": on this
        // branch the controller's registration is not removed, only excluded from feature /
        // metadata.version upgrade decisions (see docs/operations/kraft.md). It uses the same
        // wire format as apache/kafka#22191's (KIP-1312) unregister-controller RPC, but this
        // branch only writes the decommission marker.
        Subparser decommissionControllerParser = subparsers.addParser("decommission-controller")
                .help("Decommission a stopped controller: retire it from feature / metadata.version " +
                        "upgrade decisions. This branch retains the registration, so it remains " +
                        "listed by list-endpoints / DescribeCluster and MetadataShell; that is by " +
                        "design (see docs/operations/kraft.md). The 4.4+ forward-port removes a " +
                        "marked registration when this command is re-run after metadata.version " +
                        "is finalized to 4.4-IV2.");
        for (Subparser subpparser : List.of(clusterIdParser, unregisterParser, listEndpoints,
                decommissionControllerParser)) {
            MutuallyExclusiveGroup connectionOptions = subpparser.addMutuallyExclusiveGroup().required(true);
            connectionOptions.addArgument("--bootstrap-server", "-b")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");
            connectionOptions.addArgument("--bootstrap-controller", "-C")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the KRaft controllers.");
            subpparser.addArgument("--config")
                    .action(store())
                    .help("(DEPRECATED) A property file containing configurations for the Admin client. " +
                            "This option will be removed in a future version. Use --command-config instead.");
            subpparser.addArgument("--command-config", "-c")
                    .action(store())
                    .help("Config properties file for the Admin client.");
        }
        unregisterParser.addArgument("--id", "-i")
                .type(Integer.class)
                .action(store())
                .required(true)
                .help("The ID of the broker to unregister.");
        listEndpoints.addArgument("--include-fenced-brokers")
                .action(storeTrue())
                .help("Whether to include fenced brokers when listing broker endpoints");
        decommissionControllerParser.addArgument("--id", "-i")
                .type(Integer.class)
                .action(store())
                .required(true)
                .help("The ID of the stopped controller to decommission. It must not be the " +
                        "active controller and must not be present in this cluster's " +
                        "controller.quorum.voters configuration.");

        Namespace namespace = parser.parseArgsOrFail(args);
        String command = namespace.getString("command");
        String configFile = namespace.getString("config");
        String commandConfigFile = namespace.getString("command_config");
        if (configFile != null && commandConfigFile != null) {
            throw new ArgumentParserException("--config and --command-config cannot be specified together.", parser);
        }
        if (configFile != null) {
            System.out.println("Option --config has been deprecated and will be removed in a future version. Use --command-config instead.");
            commandConfigFile = configFile;
        }
        Properties properties = (commandConfigFile != null) ? Utils.loadProps(commandConfigFile) : new Properties();

        CommandLineUtils.initializeBootstrapProperties(properties,
                Optional.ofNullable(namespace.getString("bootstrap_server")),
                Optional.ofNullable(namespace.getString("bootstrap_controller")));

        switch (command) {
            case "cluster-id": {
                try (Admin adminClient = Admin.create(properties)) {
                    clusterIdCommand(System.out, adminClient);
                }
                break;
            }
            case "unregister": {
                try (Admin adminClient = Admin.create(properties)) {
                    unregisterCommand(System.out, adminClient, namespace.getInt("id"));
                }
                break;
            }
            case "list-endpoints": {
                try (Admin adminClient = Admin.create(properties)) {
                    boolean includeFencedBrokers = Optional.of(namespace.getBoolean("include_fenced_brokers")).orElse(false);
                    boolean listControllerEndpoints = namespace.getString("bootstrap_controller") != null;
                    if (includeFencedBrokers && listControllerEndpoints) {
                        throw new IllegalArgumentException("The option --include-fenced-brokers is only supported with --bootstrap-server option");
                    }
                    listEndpoints(System.out, adminClient, listControllerEndpoints, includeFencedBrokers);
                }
                break;
            }
            case "decommission-controller": {
                try (Admin adminClient = Admin.create(properties)) {
                    decommissionControllerCommand(System.out, adminClient, namespace.getInt("id"));
                }
                break;
            }
            default:
                throw new RuntimeException("Unknown command " + command);
        }
    }

    static void clusterIdCommand(PrintStream stream, Admin adminClient) throws Exception {
        String clusterId = adminClient.describeCluster().clusterId().get();
        if (clusterId != null) {
            stream.println("Cluster ID: " + clusterId);
        } else {
            stream.println("No cluster ID found. The Kafka version is probably too old.");
        }
    }

    static void unregisterCommand(PrintStream stream, Admin adminClient, int id) throws Exception {
        try {
            adminClient.unregisterBroker(id).all().get();
            stream.println("Broker " + id + " is no longer registered.");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnsupportedVersionException) {
                stream.println("The target cluster does not support the broker unregistration API.");
            } else {
                throw ee;
            }
        }
    }

    // Aiven fork addition (KAFKA-20295). decommissionController is deliberately not on the public
    // Admin interface, so it is reached via a cast to KafkaAdminClient. That means a non-
    // KafkaAdminClient Admin (e.g. MockAdminClient in unit tests) cannot reach this command; fail
    // clearly with a TerseException instead of letting a raw ClassCastException escape.
    static void decommissionControllerCommand(PrintStream stream, Admin adminClient, int controllerId) throws Exception {
        if (!(adminClient instanceof KafkaAdminClient)) {
            throw new TerseException("The decommission-controller command requires a real " +
                    "KafkaAdminClient (got " + adminClient.getClass().getName() + " instead); " +
                    "decommissionController is not part of the public Admin interface.");
        }
        KafkaAdminClient kafkaAdminClient = (KafkaAdminClient) adminClient;
        try {
            kafkaAdminClient.decommissionController(controllerId).all().get();
            stream.println("Controller " + controllerId + " has been decommissioned: it no longer " +
                    "participates in feature and metadata.version upgrade decisions. Its " +
                    "registration is unchanged, so it remains listed by list-endpoints, " +
                    "DescribeCluster and MetadataShell; that is expected, not a failure (see " +
                    "docs/operations/kraft.md). The 4.4+ forward-port removes the retained " +
                    "registration when this command is re-run after metadata.version is " +
                    "finalized to 4.4-IV2.");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnsupportedVersionException
                    || cause instanceof ControllerIdNotRegisteredException
                    || cause instanceof InvalidRequestException) {
                throw new TerseException(cause.getMessage());
            } else {
                throw ee;
            }
        }
    }

    static void listEndpoints(PrintStream stream, Admin adminClient, boolean listControllerEndpoints, boolean includeFencedBrokers) throws Exception {
        try {
            DescribeClusterOptions option = new DescribeClusterOptions().includeFencedBrokers(includeFencedBrokers);
            Collection<Node> nodes = adminClient.describeCluster(option).nodes().get();

            String maxHostLength = String.valueOf(nodes.stream().map(node -> node.host().length()).max(Integer::compareTo).orElse(100));
            String maxRackLength = String.valueOf(nodes.stream().filter(Node::hasRack).map(node -> node.rack().length()).max(Integer::compareTo).orElse(10));

            if (listControllerEndpoints) {
                String format = "%-10s %-" + maxHostLength + "s %-10s %-" + maxRackLength + "s %-15s%n";
                stream.printf(format, "ID", "HOST", "PORT", "RACK", "ENDPOINT_TYPE");
                nodes.forEach(node -> stream.printf(format,
                        node.idString(),
                        node.host(),
                        node.port(),
                        node.rack(),
                        "controller"
                ));
            } else {
                String format = "%-10s %-" + maxHostLength + "s %-10s %-" + maxRackLength + "s %-10s %-15s%n";
                stream.printf(format, "ID", "HOST", "PORT", "RACK", "STATE", "ENDPOINT_TYPE");
                nodes.forEach(node -> stream.printf(format,
                        node.idString(),
                        node.host(),
                        node.port(),
                        node.rack(),
                        node.isFenced() ? "fenced" : "unfenced",
                        "broker"
                ));
            }
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            if (cause instanceof UnsupportedVersionException) {
                stream.println(ee.getCause().getMessage());
            } else {
                throw ee;
            }
        }
    }
}
