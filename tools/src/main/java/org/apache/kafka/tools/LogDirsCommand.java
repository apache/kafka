package org.apache.kafka.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LogDirsCommand {

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            LogDirsCommandOptions logDirsCommandOptions = new LogDirsCommandOptions(args);
            try (Admin adminClient = createAdminClient(logDirsCommandOptions)) {
                execute(logDirsCommandOptions, adminClient);
            }
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

    static void execute(LogDirsCommandOptions logDirsCommandOptions, Admin adminClient) throws TerseException, JsonProcessingException, ExecutionException, InterruptedException {
        Set<String> topicList = Arrays.stream(logDirsCommandOptions.options.valueOf(logDirsCommandOptions.topicListOpt).split(",")).filter(x -> !x.isEmpty()).collect(Collectors.toSet());
        Set<Integer> clusterBrokers = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toSet());
        Optional<String> brokers = Optional.ofNullable(logDirsCommandOptions.options.valueOf(logDirsCommandOptions.brokerListOpt));
        Set<Integer> inputBrokers = Arrays.stream(brokers.orElse("").split(",")).filter(x -> !x.isEmpty()).map(Integer::valueOf).collect(Collectors.toSet());
        Set<Integer> existingBrokers = inputBrokers.isEmpty() ? new HashSet<>(clusterBrokers): new HashSet<>(inputBrokers);
        existingBrokers.retainAll(clusterBrokers);
        Set<Integer> nonExistingBrokers = new HashSet<>(inputBrokers);
        nonExistingBrokers.removeAll(clusterBrokers);

        if (!nonExistingBrokers.isEmpty()) {
            throw new TerseException(
                    String.format(
                            "ERROR: The given brokers do not exist from --broker-list: %s. Current existent brokers: %s\n",
                            nonExistingBrokers.stream().map(String::valueOf).collect(Collectors.joining(",")),
                            clusterBrokers.stream().map(String::valueOf).collect(Collectors.joining(","))));
        } else {
            System.out.println("Querying brokers for log directories information");
            DescribeLogDirsResult describeLogDirsResult = adminClient.describeLogDirs(existingBrokers);
            Map<Integer, Map<String, LogDirDescription>> logDirInfosByBroker = describeLogDirsResult.allDescriptions().get();

            System.out.printf(
                    "Received log directory information from brokers %s\n",
                    existingBrokers.stream().map(String::valueOf).collect(Collectors.joining(",")));
            System.out.println(formatAsJson(logDirInfosByBroker, topicList));
        }
    }

    private static List<Map<String, Object>> fromReplicasInfoToPrintableRepresentation(Map<TopicPartition, ReplicaInfo> replicasInfo) {
        return replicasInfo.entrySet().stream().map(entry -> {
            TopicPartition topicPartition = entry.getKey();
            ReplicaInfo replicaInfo = entry.getValue();
            return new HashMap<String, Object>() {{
                put("partition", topicPartition.toString());
                put("size", replicaInfo.size());
                put("offsetLag", replicaInfo.offsetLag());
                put("isFuture", replicaInfo.isFuture());
            }};
        }).collect(Collectors.toList());
    }

    private static List<Map<String, Object>> fromLogDirInfosToPrintableRepresentation(Map<String, LogDirDescription> logDirInfos, Set<String> topicSet) {
        return logDirInfos.entrySet().stream().map(entry -> {
            String logDir = entry.getKey();
            LogDirDescription logDirInfo = entry.getValue();
            return new HashMap<String, Object>() {{
                put("logDir", logDir);
                put("error", Optional.ofNullable(logDirInfo.error()).map(ex -> ex.getClass().getName()));
                put("partitions", fromReplicasInfoToPrintableRepresentation(
                        logDirInfo.replicaInfos().entrySet().stream().filter(entry -> {
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
            }).collect(Collectors.toList()));
        }});
    }

    private static Admin createAdminClient(LogDirsCommandOptions logDirsCommandOptions) throws IOException {
        Properties props = new Properties();
        if (logDirsCommandOptions.options.has(logDirsCommandOptions.commandConfigOpt)) {
            Utils.loadProps(logDirsCommandOptions.options.valueOf(logDirsCommandOptions.commandConfigOpt));
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, logDirsCommandOptions.options.valueOf(logDirsCommandOptions.bootstrapServerOpt));
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
                    .describedAs("Admin client property file");
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
                    .ofType(String.class);

            options = parser.parse(args);

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to query log directory usage on the specified brokers.");

            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, describeOpt);
        }
    }
}
