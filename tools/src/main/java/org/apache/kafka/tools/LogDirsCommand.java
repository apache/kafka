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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeLogDirsResult;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.ReplicaInfo;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

public class LogDirsCommand {

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

    private static void execute(String... args) throws Exception {
        LogDirsCommandOptions options = new LogDirsCommandOptions(args);
        try (Admin adminClient = createAdminClient(options)) {
            execute(options, adminClient);
        }
    }

    static void execute(LogDirsCommandOptions options, Admin adminClient) throws Exception {
        Set<String> topics = options.topics();
        Set<Integer> clusterBrokers = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
        Set<Integer> inputBrokers = options.brokers();
        Set<Integer> existingBrokers = inputBrokers.isEmpty() ? new HashSet<>(clusterBrokers) : new HashSet<>(inputBrokers);
        existingBrokers.retainAll(clusterBrokers);
        Set<Integer> nonExistingBrokers = new HashSet<>(inputBrokers);
        nonExistingBrokers.removeAll(clusterBrokers);

        if (!nonExistingBrokers.isEmpty()) {
            throw new TerseException(
                    String.format(
                            "ERROR: The given brokers do not exist from --broker-list: %s. Current existent brokers: %s",
                            commaDelimitedStringFromIntegerSet(nonExistingBrokers),
                            commaDelimitedStringFromIntegerSet(clusterBrokers)));
        } else {
            System.out.println("Querying brokers for log directories information");
            DescribeLogDirsResult describeLogDirsResult = adminClient.describeLogDirs(existingBrokers);
            Map<Integer, Map<String, LogDirDescription>> logDirInfosByBroker = describeLogDirsResult.allDescriptions().get();

            System.out.printf(
                    "Received log directory information from brokers %s%n",
                    commaDelimitedStringFromIntegerSet(existingBrokers));
            System.out.println(formatAsJson(logDirInfosByBroker, topics));
        }
    }

    private static String commaDelimitedStringFromIntegerSet(Set<Integer> set) {
        return set.stream().map(String::valueOf).collect(Collectors.joining(","));
    }

    private static List<Map<String, Object>> fromReplicasInfoToPrintableRepresentation(Map<TopicPartition, ReplicaInfo> replicasInfo) {
        return replicasInfo.entrySet().stream().map(entry -> {
            TopicPartition topicPartition = entry.getKey();
            return new HashMap<String, Object>() {{
                    put("partition", topicPartition.toString());
                    put("size", entry.getValue().size());
                    put("offsetLag", entry.getValue().offsetLag());
                    put("isFuture", entry.getValue().isFuture());
                }};
        }).collect(Collectors.toList());
    }

    private static List<Map<String, Object>> fromLogDirInfosToPrintableRepresentation(Map<String, LogDirDescription> logDirInfos, Set<String> topicSet) {
        return logDirInfos.entrySet().stream().map(entry -> {
            String logDir = entry.getKey();
            return new HashMap<String, Object>() {{
                    put("logDir", logDir);
                    put("error", entry.getValue().error() != null ? entry.getValue().error().getClass().getName() : null);
                    put("partitions", fromReplicasInfoToPrintableRepresentation(
                            entry.getValue().replicaInfos().entrySet().stream().filter(entry -> {
                                TopicPartition topicPartition = entry.getKey();
                                return topicSet.isEmpty() || topicSet.contains(topicPartition.topic());
                            }).collect(Collectors.toMap(Entry::getKey, Entry::getValue))
                    ));
                }};
        }).collect(Collectors.toList());
    }

    private static String formatAsJson(Map<Integer, Map<String, LogDirDescription>> logDirInfosByBroker, Set<String> topicSet) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(new HashMap<String, Object>() {{
                put("version", 1);
                put("brokers", logDirInfosByBroker.entrySet().stream().map(entry -> {
                    int broker = entry.getKey();
                    Map<String, LogDirDescription> logDirInfos = entry.getValue();
                    return new HashMap<String, Object>() {{
                            put("broker", broker);
                            put("logDirs", fromLogDirInfosToPrintableRepresentation(logDirInfos, topicSet));
                        }};
                }).toList());
            }});
    }

    private static Admin createAdminClient(LogDirsCommandOptions options) throws IOException {
        Properties props = new Properties();
        if (options.hasCommandConfig()) {
            props.putAll(Utils.loadProps(options.commandConfig()));
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, options.bootstrapServers());
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "log-dirs-tool");
        return Admin.create(props);
    }

    // Visible for testing
    static class LogDirsCommandOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> commandConfigOpt;
        private final OptionSpecBuilder describeOpt;
        private final OptionSpec<String> topicListOpt;
        private final OptionSpec<String> brokerListOpt;

        public LogDirsCommandOptions(String... args) {
            super(args);

            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: the server(s) to use for bootstrapping")
                    .withRequiredArg()
                    .describedAs("The server(s) to use for bootstrapping")
                    .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                    .withRequiredArg()
                    .describedAs("Admin client property file")
                    .ofType(String.class);
            describeOpt = parser.accepts("describe", "Describe the specified log directories on the specified brokers.");
            topicListOpt = parser.accepts("topic-list", "The list of topics to be queried in the form \"topic1,topic2,topic3\". " +
                            "All topics will be queried if no topic list is specified")
                    .withRequiredArg()
                    .describedAs("Topic list")
                    .defaultsTo("")
                    .ofType(String.class);
            brokerListOpt = parser.accepts("broker-list", "The list of brokers to be queried in the form \"0,1,2\". " +
                            "All brokers in the cluster will be queried if no broker list is specified")
                    .withRequiredArg()
                    .describedAs("Broker list")
                    .ofType(String.class)
                    .defaultsTo("");

            options = parser.parse(args);

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to query log directory usage on the specified brokers.");

            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, describeOpt);
        }

        private Stream<String> splitAtCommasAndFilterOutEmpty(OptionSpec<String> option) {
            return Arrays.stream(options.valueOf(option).split(",")).filter(x -> !x.isEmpty());
        }

        private String bootstrapServers() {
            return options.valueOf(bootstrapServerOpt);
        }

        private boolean hasCommandConfig() {
            return options.has(commandConfigOpt);
        }

        private String commandConfig() {
            return options.valueOf(commandConfigOpt);
        }

        private Set<String> topics() {
            return splitAtCommasAndFilterOutEmpty(topicListOpt).collect(Collectors.toSet());
        }

        private Set<Integer> brokers() {
            return splitAtCommasAndFilterOutEmpty(brokerListOpt).map(Integer::valueOf).collect(Collectors.toSet());
        }
    }
}
