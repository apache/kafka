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

package org.apache.kafka.soak.tool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.soak.action.ActionRegistry;
import org.apache.kafka.soak.action.ActionScheduler;
import org.apache.kafka.soak.cloud.Ec2Cloud;
import org.apache.kafka.soak.cluster.SoakCluster;
import org.apache.kafka.soak.cluster.SoakClusterSpec;
import org.apache.kafka.soak.common.SoakLog;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * The soak command.
 */
public final class SoakTool {
    public static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.enable(SerializationFeature.INDENT_OUTPUT);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private static final String SOAK_CLUSTER_INPUT_PATH = "SOAK_CLUSTER_INPUT_PATH";
    private static final String SOAK_CLUSTER_OUTPUT_PATH = "SOAK_CLUSTER_OUTPUT_PATH";
    private static final String SOAK_AWS_SECURITY_GROUP = "SOAK_AWS_SECURITY_GROUP";
    private static final String SOAK_AWS_SECURITY_KEYPAIR = "SOAK_AWS_SECURITY_KEYPAIR";
    private static final String SOAK_TIMEOUT_SECONDS = "SOAK_TIMEOUT_SECONDS";
    private static final String SOAK_TARGETS = "SOAK_TARGETS";
    private static final String SOAK_KAFKA_PATH = "SOAK_KAFKA_PATH";
    private static final String SOAK_OUTPUT_DIRECTORY = "SOAK_OUTPUT_DIRECTORY";
    private static final String SOAK_OUTPUT_DIRECTORY_DEFAULT = "/tmp/soak";
    private static final String SOAK_VERBOSE = "SOAK_VERBOSE";
    private static final boolean SOAK_VERBOSE_DEFAULT = false;
    private static final String SOAK_TROGDOR_TASK_DELAY = "SOAK_TROGDOR_TASK_DELAY";
    private static final int SOAK_TROGDOR_TASK_DELAY_DEFAULT = 15000;

    private static final String SOAK_DESCRIPTION = String.format(
        "The Kafka soak cluster tool.%n" +
        "%n" +
        "Valid targets:%n" +
        "up:                Bring up all nodes.%n" +
        "  init:            Allocate nodes.%n" +
        "  setup:           Set up all nodes.%n" +
        "  start:           Start the system.%n" +
        "%n" +
        "status:            Get the system status.%n" +
        "  daemonStatus:    Get the status of system daemons.%n" +
        "  taskStatus:      Get the status of trogdor tasks.%n" +
        "%n" +
        "down:              Bring down all nodes.%n" +
        "  saveLogs:        Save the system logs.%n" +
        "  stop:            Stop the system.%n" +
        "  destroy:         Deallocate nodes.%n" +
        "%n" +
        "ssh [nodes] [cmd]: Ssh to the given node(s)%n" +
        "%n");

    private static String getEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            return val;
        }
        return defaultValue;
    }

    private static int getEnvInt(String name, Integer defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            try {
                return Integer.parseInt(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unable to parse value " + name +
                        " given for " + name, e);
            }
        }
        return defaultValue;
    }

    private static boolean getEnvBoolean(String name, boolean defaultValue) {
        String val = System.getenv(name);
        if (val != null) {
            try {
                return Boolean.parseBoolean(val);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Unable to parse value " + name +
                    " given for " + name, e);
            }
        }
        return defaultValue;
    }

    public static void main(String[] args) throws Throwable {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("soak-tool")
            .defaultHelp(true)
            .description(SOAK_DESCRIPTION);

        parser.addArgument("-c", "--cluster-input")
            .action(store())
            .type(String.class)
            .dest(SOAK_CLUSTER_INPUT_PATH)
            .metavar(SOAK_CLUSTER_INPUT_PATH)
            .setDefault(getEnv(SOAK_CLUSTER_INPUT_PATH, ""))
            .help("The path to the soak cluster file to read.");
        parser.addArgument("-r", "--cluster-output")
            .action(store())
            .type(String.class)
            .dest(SOAK_CLUSTER_OUTPUT_PATH)
            .metavar(SOAK_CLUSTER_OUTPUT_PATH)
            .setDefault(getEnv(SOAK_CLUSTER_OUTPUT_PATH, ""))
            .help("The path to use when writing a new soak cluster file.");
        parser.addArgument("--sg")
            .action(store())
            .type(String.class)
            .dest(SOAK_AWS_SECURITY_GROUP)
            .metavar(SOAK_AWS_SECURITY_GROUP)
            .setDefault(getEnv(SOAK_AWS_SECURITY_GROUP, ""))
            .help("The AWS security group name to use.");
        parser.addArgument("--keypair")
            .action(store())
            .type(String.class)
            .dest(SOAK_AWS_SECURITY_KEYPAIR)
            .metavar(SOAK_AWS_SECURITY_KEYPAIR)
            .setDefault(getEnv(SOAK_AWS_SECURITY_KEYPAIR, ""))
            .help("The AWS keypair name to use.");
        parser.addArgument("-t", "--timeout")
            .action(store())
            .type(Integer.class)
            .dest(SOAK_TIMEOUT_SECONDS)
            .metavar(SOAK_TIMEOUT_SECONDS)
            .setDefault(getEnvInt(SOAK_TIMEOUT_SECONDS, 360000))
            .help("The timeout in seconds.");
        parser.addArgument("--kafka-path")
            .action(store())
            .type(String.class)
            .dest(SOAK_KAFKA_PATH)
            .metavar(SOAK_KAFKA_PATH)
            .setDefault(getEnv(SOAK_KAFKA_PATH, ""))
            .help("The path to the Kafka directory.");
        parser.addArgument("-o", "--output-directory")
            .action(store())
            .type(String.class)
            .dest(SOAK_OUTPUT_DIRECTORY)
            .metavar(SOAK_OUTPUT_DIRECTORY)
            .setDefault(getEnv(SOAK_OUTPUT_DIRECTORY, SOAK_OUTPUT_DIRECTORY_DEFAULT))
            .help("The output path to store logs, cluster files, and other outputs in.");
        parser.addArgument("target")
            .nargs("*")
            .action(store())
            .required(false)
            .dest(SOAK_TARGETS)
            .metavar(SOAK_TARGETS)
            .help("The target action(s) to run.");
        parser.addArgument("--trogdor-task-delay")
            .action(store())
            .type(Integer.class)
            .dest(SOAK_TROGDOR_TASK_DELAY)
            .metavar(SOAK_TROGDOR_TASK_DELAY)
            .setDefault(getEnvInt(SOAK_TROGDOR_TASK_DELAY, SOAK_TROGDOR_TASK_DELAY_DEFAULT))
            .help("The number of milliseconds to delay before starting the trogdor tasks.");
        parser.addArgument("-v")
            .action(storeTrue())
            .type(Boolean.class)
            .required(false)
            .dest(SOAK_VERBOSE)
            .metavar(SOAK_VERBOSE)
            .setDefault(getEnvBoolean(SOAK_VERBOSE, SOAK_VERBOSE_DEFAULT))
            .help("Enable verbose logging.");

        try {
            Namespace res = parser.parseArgsOrFail(args);
            List<String> targets = res.<String>getList(SOAK_TARGETS);
            if (targets.isEmpty()) {
                parser.printHelp();
                System.exit(0);
            }
            String clusterOutputPath = res.getString(SOAK_CLUSTER_OUTPUT_PATH);
            if (clusterOutputPath.isEmpty()) {
                clusterOutputPath = Paths.get(res.getString(SOAK_OUTPUT_DIRECTORY),
                    "cluster.json").toAbsolutePath().toString();
            }
            if (res.getString(SOAK_CLUSTER_INPUT_PATH).isEmpty()) {
                throw new RuntimeException("Unable to You must specify an input cluster file using -c.");
            }
            SoakEnvironment env = new SoakEnvironment(
                    res.getString(SOAK_CLUSTER_INPUT_PATH),
                    clusterOutputPath,
                    res.getString(SOAK_AWS_SECURITY_GROUP),
                    res.getString(SOAK_AWS_SECURITY_KEYPAIR),
                    res.getInt(SOAK_TIMEOUT_SECONDS),
                    res.getString(SOAK_KAFKA_PATH),
                    res.getString(SOAK_OUTPUT_DIRECTORY),
                    res.getInt(SOAK_TROGDOR_TASK_DELAY));
            File clusterInputPath = new File(env.clusterInputPath());
            SoakClusterSpec clusterSpec =
                SoakTool.JSON_SERDE.readValue(clusterInputPath, SoakClusterSpec.class);
            Files.createDirectories(Paths.get(env.outputDirectory()));
            SoakLog clusterLog = SoakLog.fromStdout("cluster", res.getBoolean(SOAK_VERBOSE));

            SoakReturnCode exitCode;
            try (Ec2Cloud cloud = new Ec2Cloud(env.awsSecurityKeyPair(), env.awsSecurityGroup())) {
                try (SoakCluster cluster = new SoakCluster(env, cloud, clusterLog, clusterSpec)) {
                    Runtime.getRuntime().addShutdownHook(new Thread() {
                        @Override
                        public void run() {
                            cluster.shutdownManager().shutdown();
                        }
                    });
                    if (targets.contains(SoakSsh.COMMAND)) {
                        SoakSsh.run(cluster, targets);
                    } else {
                        try (ActionScheduler scheduler = cluster.createScheduler(targets,
                            ActionRegistry.INSTANCE.actions(cluster.nodes().keySet()))) {
                            scheduler.await(env.timeoutSecs(), TimeUnit.SECONDS);
                        }
                    }
                    cluster.shutdownManager().shutdown();
                    exitCode = cluster.shutdownManager().returnCode();
                }
            }
            System.exit(exitCode.code());
        } catch (Throwable exception) {
            System.out.printf("Exiting with exception: %s%n", Utils.fullStackTrace(exception));
            System.exit(1);
        }
    }
};
