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

import joptsimple.OptionException;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.PartitionFilter;
import org.apache.kafka.server.util.PartitionFilter.PartitionRangeFilter;
import org.apache.kafka.server.util.PartitionFilter.PartitionsSetFilter;
import org.apache.kafka.server.util.PartitionFilter.UniquePartitionFilter;
import org.apache.kafka.server.util.TopicFilter.IncludeList;
import org.apache.kafka.server.util.TopicPartitionFilter;
import org.apache.kafka.server.util.TopicPartitionFilter.CompositeTopicPartitionFilter;
import org.apache.kafka.server.util.TopicPartitionFilter.TopicFilterAndPartitionFilter;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GetOffsetShell {
    private static final Pattern TOPIC_PARTITION_PATTERN = Pattern.compile("([^:,]*)(?::(?:([0-9]*)|(?:([0-9]*)-([0-9]*))))?");

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (TerseException e) {
            System.err.println("Error occurred: " + e.getMessage());
            return 1;
        } catch (Throwable e) {
            System.err.println("Error occurred: " + e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws IOException, ExecutionException, InterruptedException, TerseException {
        GetOffsetShell getOffsetShell = new GetOffsetShell();

        GetOffsetShellOptions options = new GetOffsetShellOptions(args);

        Map<TopicPartition, Long> partitionOffsets = getOffsetShell.fetchOffsets(options);

        for (Map.Entry<TopicPartition, Long> entry : partitionOffsets.entrySet()) {
            TopicPartition topic = entry.getKey();

            System.out.println(String.join(":", new String[]{topic.topic(), String.valueOf(topic.partition()), entry.getValue().toString()}));
        }
    }

    private static class GetOffsetShellOptions extends CommandDefaultOptions {
        private final OptionSpec<String> brokerListOpt;
        private final OptionSpec<String> bootstrapServerOpt;
        private final OptionSpec<String> topicPartitionsOpt;
        private final OptionSpec<String> topicOpt;
        private final OptionSpec<String> partitionsOpt;
        private final OptionSpec<String> timeOpt;
        private final OptionSpec<String> commandConfigOpt;
        private final OptionSpec<String> effectiveBrokerListOpt;
        private final OptionSpecBuilder excludeInternalTopicsOpt;

        public GetOffsetShellOptions(String[] args) throws TerseException {
            super(args);

            brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified. The server(s) to connect to in the form HOST1:PORT1,HOST2:PORT2.")
                    .withRequiredArg()
                    .describedAs("HOST1:PORT1,...,HOST3:PORT3")
                    .ofType(String.class);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED. The server(s) to connect to in the form HOST1:PORT1,HOST2:PORT2.")
                    .requiredUnless("broker-list")
                    .withRequiredArg()
                    .describedAs("HOST1:PORT1,...,HOST3:PORT3")
                    .ofType(String.class);
            topicPartitionsOpt = parser.accepts("topic-partitions", "Comma separated list of topic-partition patterns to get the offsets for, with the format of '" + TOPIC_PARTITION_PATTERN + "'." +
                            " The first group is an optional regex for the topic name, if omitted, it matches any topic name." +
                            " The section after ':' describes a 'partition' pattern, which can be: a number, a range in the format of 'NUMBER-NUMBER' (lower inclusive, upper exclusive), an inclusive lower bound in the format of 'NUMBER-', an exclusive upper bound in the format of '-NUMBER' or may be omitted to accept all partitions.")
                    .withRequiredArg()
                    .describedAs("topic1:1,topic2:0-3,topic3,topic4:5-,topic5:-3")
                    .ofType(String.class);
            topicOpt = parser.accepts("topic", "The topic to get the offsets for. It also accepts a regular expression. If not present, all authorized topics are queried. Cannot be used if --topic-partitions is present.")
                    .withRequiredArg()
                    .describedAs("topic")
                    .ofType(String.class);
            partitionsOpt = parser.accepts("partitions", "Comma separated list of partition ids to get the offsets for. If not present, all partitions of the authorized topics are queried. Cannot be used if --topic-partitions is present.")
                    .withRequiredArg()
                    .describedAs("partition ids")
                    .ofType(String.class);
            timeOpt = parser.accepts("time", "timestamp of the offsets before that. [Note: No offset is returned, if the timestamp greater than recently committed record timestamp is given.]")
                    .withRequiredArg()
                    .describedAs("<timestamp> / -1 or latest / -2 or earliest / -3 or max-timestamp")
                    .ofType(String.class)
                    .defaultsTo("latest");
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                    .withRequiredArg()
                    .describedAs("config file")
                    .ofType(String.class);
            excludeInternalTopicsOpt = parser.accepts("exclude-internal-topics", "By default, internal topics are included. If specified, internal topics are excluded.");

            if (args.length == 0) {
                CommandLineUtils.printUsageAndExit(parser, "An interactive shell for getting topic-partition offsets.");
            }

            try {
                options = parser.parse(args);
            } catch (OptionException e) {
                throw new TerseException(e.getMessage());
            }

            if (options.has(bootstrapServerOpt)) {
                effectiveBrokerListOpt = bootstrapServerOpt;
            } else {
                effectiveBrokerListOpt = brokerListOpt;
            }

            CommandLineUtils.checkRequiredArgs(parser, options, effectiveBrokerListOpt);

            String brokerList = options.valueOf(effectiveBrokerListOpt);

            try {
                ToolsUtils.validateBootstrapServer(brokerList);
            } catch (IllegalArgumentException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }
        }

        public boolean hasTopicPartitionsOpt() {
            return options.has(topicPartitionsOpt);
        }

        public String topicPartitionsOpt() {
            return options.valueOf(topicPartitionsOpt);
        }

        public boolean hasTopicOpt() {
            return options.has(topicOpt);
        }

        public String topicOpt() {
            return options.valueOf(topicOpt);
        }

        public boolean hasPartitionsOpt() {
            return options.has(partitionsOpt);
        }

        public String partitionsOpt() {
            return options.valueOf(partitionsOpt);
        }

        public String timeOpt() {
            return options.valueOf(timeOpt);
        }

        public boolean hasCommandConfigOpt() {
            return options.has(commandConfigOpt);
        }

        public String commandConfigOpt() {
            return options.valueOf(commandConfigOpt);
        }

        public String effectiveBrokerListOpt() {
            return options.valueOf(effectiveBrokerListOpt);
        }

        public boolean hasExcludeInternalTopicsOpt() {
            return options.has(excludeInternalTopicsOpt);
        }
    }

    public Map<TopicPartition, Long> fetchOffsets(GetOffsetShellOptions options) throws IOException, ExecutionException, InterruptedException, TerseException {
        String clientId = "GetOffsetShell";
        String brokerList = options.effectiveBrokerListOpt();

        if (options.hasTopicPartitionsOpt() && (options.hasTopicOpt() || options.hasPartitionsOpt())) {
            throw new TerseException("--topic-partitions cannot be used with --topic or --partitions");
        }

        boolean excludeInternalTopics = options.hasExcludeInternalTopicsOpt();
        OffsetSpec offsetSpec = parseOffsetSpec(options.timeOpt());

        TopicPartitionFilter topicPartitionFilter;

        if (options.hasTopicPartitionsOpt()) {
            topicPartitionFilter = createTopicPartitionFilterWithPatternList(options.topicPartitionsOpt());
        } else {
            topicPartitionFilter = createTopicPartitionFilterWithTopicAndPartitionPattern(options.topicOpt(), options.partitionsOpt());
        }

        Properties config = options.hasCommandConfigOpt() ? Utils.loadProps(options.commandConfigOpt()) : new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        config.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, clientId);

        try (Admin adminClient = Admin.create(config)) {
            List<TopicPartition> partitionInfos = listPartitionInfos(adminClient, topicPartitionFilter, excludeInternalTopics);

            if (partitionInfos.isEmpty()) {
                throw new TerseException("Could not match any topic-partitions with the specified filters");
            }

            Map<TopicPartition, OffsetSpec> timestampsToSearch = partitionInfos.stream().collect(Collectors.toMap(tp -> tp, tp -> offsetSpec));

            ListOffsetsResult listOffsetsResult = adminClient.listOffsets(timestampsToSearch);

            TreeMap<TopicPartition, Long> partitionOffsets = new TreeMap<>(Comparator.comparing(TopicPartition::toString));

            for (TopicPartition partition : partitionInfos) {
                ListOffsetsResultInfo partitionInfo;

                try {
                    partitionInfo = listOffsetsResult.partitionResult(partition).get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof KafkaException) {
                        System.err.println("Skip getting offsets for topic-partition " + partition.toString() + " due to error: " + e.getMessage());
                    } else {
                        throw e;
                    }

                    continue;
                }

                if (partitionInfo.offset() != ListOffsetsResponse.UNKNOWN_OFFSET) {
                    partitionOffsets.put(partition, partitionInfo.offset());
                }
            }

            return partitionOffsets;
        }
    }

    private OffsetSpec parseOffsetSpec(String listOffsetsTimestamp) throws TerseException {
        switch (listOffsetsTimestamp) {
            case "earliest":
                return OffsetSpec.earliest();
            case "latest":
                return OffsetSpec.latest();
            case "max-timestamp":
                return OffsetSpec.maxTimestamp();
            default:
                long timestamp;

                try {
                    timestamp = Long.parseLong(listOffsetsTimestamp);
                } catch (NumberFormatException e) {
                    throw new TerseException("Malformed time argument " + listOffsetsTimestamp + ". " +
                            "Please use -1 or latest / -2 or earliest / -3 or max-timestamp, or a specified long format timestamp");
                }

                if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
                    return OffsetSpec.earliest();
                } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
                    return OffsetSpec.latest();
                } else if (timestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
                    return OffsetSpec.maxTimestamp();
                } else {
                    return OffsetSpec.forTimestamp(timestamp);
                }
        }
    }

    /**
     * Creates a topic-partition filter based on a list of patterns.
     * Expected format:
     * List: TopicPartitionPattern(, TopicPartitionPattern)*
     * TopicPartitionPattern: TopicPattern(:PartitionPattern)? | :PartitionPattern
     * TopicPattern: REGEX
     * PartitionPattern: NUMBER | NUMBER-(NUMBER)? | -NUMBER
     */
    public TopicPartitionFilter createTopicPartitionFilterWithPatternList(String topicPartitions) {
        List<String> ruleSpecs = Arrays.asList(topicPartitions.split(","));
        List<TopicPartitionFilter> rules = ruleSpecs.stream().map(ruleSpec -> {
            try {
                return parseRuleSpec(ruleSpec);
            } catch (TerseException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toList());

        return new CompositeTopicPartitionFilter(rules);
    }

    /**
     * Creates a topic-partition filter based on a topic pattern and a set of partition ids.
     */
    public TopicPartitionFilter createTopicPartitionFilterWithTopicAndPartitionPattern(String topicOpt, String partitionIds) throws TerseException {
        return new TopicFilterAndPartitionFilter(
                new IncludeList(topicOpt != null ? topicOpt : ".*"),
                new PartitionsSetFilter(createPartitionSet(partitionIds))
        );
    }

    private Set<Integer> createPartitionSet(String partitionsString) throws TerseException {
        Set<Integer> partitions;

        if (partitionsString == null || partitionsString.isEmpty()) {
            partitions = Collections.emptySet();
        } else {
            try {
                partitions = Arrays.stream(partitionsString.split(",")).map(Integer::parseInt).collect(Collectors.toSet());
            } catch (NumberFormatException e) {
                throw new TerseException("--partitions expects a comma separated list of numeric " +
                        "partition ids, but received: " + partitionsString);
            }
        }

        return partitions;
    }

    /**
     * Return the partition infos. Filter them with topicPartitionFilter.
     */
    private List<TopicPartition> listPartitionInfos(
            Admin client,
            TopicPartitionFilter topicPartitionFilter,
            boolean excludeInternalTopics
    ) throws ExecutionException, InterruptedException {
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions().listInternal(!excludeInternalTopics);
        Set<String> topics = client.listTopics(listTopicsOptions).names().get();
        Set<String> filteredTopics = topics.stream().filter(topicPartitionFilter::isTopicAllowed).collect(Collectors.toSet());

        return client.describeTopics(filteredTopics).allTopicNames().get().entrySet().stream().flatMap(
                topic -> topic.getValue().partitions().stream().map(
                        tp -> new TopicPartition(topic.getKey(), tp.partition())
                ).filter(topicPartitionFilter::isTopicPartitionAllowed)
        ).collect(Collectors.toList());
    }

    private TopicPartitionFilter parseRuleSpec(String ruleSpec) throws TerseException, RuntimeException {
        Matcher matcher = TOPIC_PARTITION_PATTERN.matcher(ruleSpec);

        if (!matcher.matches())
            throw new TerseException("Invalid rule specification: " + ruleSpec);

        IntFunction<String> group = (int g) -> (matcher.group(g) != null && !matcher.group(g).isEmpty()) ? matcher.group(g) : null;

        IncludeList topicFilter = group.apply(1) != null ? new IncludeList(group.apply(1)) : new IncludeList(".*");

        PartitionFilter partitionFilter;

        if (group.apply(2) != null) {
            partitionFilter = new UniquePartitionFilter(Integer.parseInt(group.apply(2)));
        } else {
            int lowerRange = group.apply(3) != null ? Integer.parseInt(group.apply(3)) : 0;
            int upperRange = group.apply(4) != null ? Integer.parseInt(group.apply(4)) : Integer.MAX_VALUE;

            partitionFilter = new PartitionRangeFilter(lowerRange, upperRange);
        }

        return new TopicPartitionFilter.TopicFilterAndPartitionFilter(topicFilter, partitionFilter);
    }
}
