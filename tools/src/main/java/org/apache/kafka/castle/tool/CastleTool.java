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

package org.apache.kafka.castle.tool;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.castle.common.JsonTransformer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.castle.action.ActionRegistry;
import org.apache.kafka.castle.action.ActionScheduler;
import org.apache.kafka.castle.cluster.CastleCluster;
import org.apache.kafka.castle.cluster.CastleClusterSpec;
import org.apache.kafka.castle.common.CastleLog;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * The castle command.
 */
public final class CastleTool {
    public static final ObjectMapper JSON_SERDE;

    static {
        JSON_SERDE = new ObjectMapper();
        JSON_SERDE.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JSON_SERDE.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
        JSON_SERDE.enable(SerializationFeature.INDENT_OUTPUT);
        JSON_SERDE.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    }

    private static final String CASTLE_CLUSTER_INPUT_PATH = "CASTLE_CLUSTER_INPUT_PATH";
    private static final String CASTLE_CLUSTER_OUTPUT_PATH = "CASTLE_CLUSTER_OUTPUT_PATH";
    private static final String CASTLE_TIMEOUT_SECONDS = "CASTLE_TIMEOUT_SECONDS";
    private static final String CASTLE_TARGETS = "CASTLE_TARGETS";
    private static final String CASTLE_KAFKA_PATH = "CASTLE_KAFKA_PATH";
    private static final String CASTLE_OUTPUT_DIRECTORY = "CASTLE_OUTPUT_DIRECTORY";
    private static final String CASTLE_OUTPUT_DIRECTORY_DEFAULT = "/tmp/castle";
    private static final String CASTLE_VERBOSE = "CASTLE_VERBOSE";
    private static final boolean CASTLE_VERBOSE_DEFAULT = false;
    private static final String CASTLE_PREFIX = "CASTLE_";

    private static final String CASTLE_DESCRIPTION = String.format(
        "The Kafka castle cluster tool.%n" +
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

    public static class CastleSubstituter implements JsonTransformer.Substituter {
        @Override
        public String substitute(String key) {
            if (!key.startsWith(CASTLE_PREFIX)) {
                return null;
            }
            String value = System.getenv(key);
            if (value == null) {
                throw new RuntimeException("You must set the environment variable " + key +
                    " to use this configuration file.");
            }
            return value;
        }
    }

    private static CastleClusterSpec readClusterSpec(File clusterInputPath) throws Throwable {
        JsonNode confNode = CastleTool.JSON_SERDE.readTree(clusterInputPath);
        JsonNode transofmredConfNode = JsonTransformer.transform(confNode, new CastleSubstituter());
        return CastleTool.JSON_SERDE.treeToValue(transofmredConfNode, CastleClusterSpec.class);
    }

    public static void main(String[] args) throws Throwable {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("castle-tool")
            .defaultHelp(true)
            .description(CASTLE_DESCRIPTION);

        parser.addArgument("-c", "--cluster-input")
            .action(store())
            .type(String.class)
            .dest(CASTLE_CLUSTER_INPUT_PATH)
            .metavar(CASTLE_CLUSTER_INPUT_PATH)
            .setDefault(getEnv(CASTLE_CLUSTER_INPUT_PATH, ""))
            .help("The path to the castle cluster file to read.");
        parser.addArgument("-r", "--cluster-output")
            .action(store())
            .type(String.class)
            .dest(CASTLE_CLUSTER_OUTPUT_PATH)
            .metavar(CASTLE_CLUSTER_OUTPUT_PATH)
            .setDefault(getEnv(CASTLE_CLUSTER_OUTPUT_PATH, ""))
            .help("The path to use when writing a new castle cluster file.");
        parser.addArgument("-t", "--timeout")
            .action(store())
            .type(Integer.class)
            .dest(CASTLE_TIMEOUT_SECONDS)
            .metavar(CASTLE_TIMEOUT_SECONDS)
            .setDefault(getEnvInt(CASTLE_TIMEOUT_SECONDS, 360000))
            .help("The timeout in seconds.");
        parser.addArgument("--kafka-path")
            .action(store())
            .type(String.class)
            .dest(CASTLE_KAFKA_PATH)
            .metavar(CASTLE_KAFKA_PATH)
            .setDefault(getEnv(CASTLE_KAFKA_PATH, ""))
            .help("The path to the Kafka directory.");
        parser.addArgument("-o", "--output-directory")
            .action(store())
            .type(String.class)
            .dest(CASTLE_OUTPUT_DIRECTORY)
            .metavar(CASTLE_OUTPUT_DIRECTORY)
            .setDefault(getEnv(CASTLE_OUTPUT_DIRECTORY, CASTLE_OUTPUT_DIRECTORY_DEFAULT))
            .help("The output path to store logs, cluster files, and other outputs in.");
        parser.addArgument("target")
            .nargs("*")
            .action(store())
            .required(false)
            .dest(CASTLE_TARGETS)
            .metavar(CASTLE_TARGETS)
            .help("The target action(s) to run.");
        parser.addArgument("-v")
            .action(storeTrue())
            .type(Boolean.class)
            .required(false)
            .dest(CASTLE_VERBOSE)
            .metavar(CASTLE_VERBOSE)
            .setDefault(getEnvBoolean(CASTLE_VERBOSE, CASTLE_VERBOSE_DEFAULT))
            .help("Enable verbose logging.");

        try {
            Namespace res = parser.parseArgsOrFail(args);
            List<String> targets = res.<String>getList(CASTLE_TARGETS);
            if (targets.isEmpty()) {
                parser.printHelp();
                System.exit(0);
            }
            String clusterOutputPath = res.getString(CASTLE_CLUSTER_OUTPUT_PATH);
            if (clusterOutputPath.isEmpty()) {
                clusterOutputPath = Paths.get(res.getString(CASTLE_OUTPUT_DIRECTORY),
                    "cluster.json").toAbsolutePath().toString();
            }
            if (res.getString(CASTLE_CLUSTER_INPUT_PATH).isEmpty()) {
                throw new RuntimeException("Unable to You must specify an input cluster file using -c.");
            }
            CastleEnvironment env = new CastleEnvironment(
                    res.getString(CASTLE_CLUSTER_INPUT_PATH),
                    clusterOutputPath,
                    res.getInt(CASTLE_TIMEOUT_SECONDS),
                    res.getString(CASTLE_KAFKA_PATH),
                    res.getString(CASTLE_OUTPUT_DIRECTORY));
            File clusterInputPath = new File(env.clusterInputPath());
            CastleClusterSpec clusterSpec = readClusterSpec(clusterInputPath);
            Files.createDirectories(Paths.get(env.outputDirectory()));
            CastleLog clusterLog = CastleLog.fromStdout("cluster", res.getBoolean(CASTLE_VERBOSE));

            CastleReturnCode exitCode;
            try (CastleCluster cluster = new CastleCluster(env, clusterLog, clusterSpec)) {
                Runtime.getRuntime().addShutdownHook(new Thread() {
                    @Override
                    public void run() {
                        cluster.shutdownManager().shutdown();
                    }
                });
                if (targets.contains(CastleSsh.COMMAND)) {
                    CastleSsh.run(cluster, targets);
                } else {
                    try (ActionScheduler scheduler = cluster.createScheduler(targets,
                        ActionRegistry.INSTANCE.actions(cluster.nodes().keySet()))) {
                        scheduler.await(env.timeoutSecs(), TimeUnit.SECONDS);
                    }
                }
                cluster.shutdownManager().shutdown();
                exitCode = cluster.shutdownManager().returnCode();
            }
            System.exit(exitCode.code());
        } catch (Throwable exception) {
            System.out.printf("Exiting with exception: %s%n", Utils.fullStackTrace(exception));
            System.exit(1);
        }
    }
};
