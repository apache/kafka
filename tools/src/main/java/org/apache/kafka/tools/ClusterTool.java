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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import net.sourceforge.argparse4j.inf.Subparsers;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static net.sourceforge.argparse4j.impl.Arguments.store;

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
        for (Subparser subpparser : Arrays.asList(clusterIdParser, unregisterParser)) {
            MutuallyExclusiveGroup connectionOptions = subpparser.addMutuallyExclusiveGroup().required(true);
            connectionOptions.addArgument("--bootstrap-server", "-b")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the Kafka cluster.");
            connectionOptions.addArgument("--bootstrap-controller", "-C")
                    .action(store())
                    .help("A list of host/port pairs to use for establishing the connection to the KRaft controllers.");
            subpparser.addArgument("--config", "-c")
                    .action(store())
                    .help("A property file containing configurations for the Admin client.");
        }
        unregisterParser.addArgument("--id", "-i")
                .type(Integer.class)
                .action(store())
                .required(true)
                .help("The ID of the broker to unregister.");

        Namespace namespace = parser.parseArgsOrFail(args);
        String command = namespace.getString("command");
        String configPath = namespace.getString("config");
        Properties properties = (configPath == null) ? new Properties() : Utils.loadProps(configPath);

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
}
