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

import com.fasterxml.jackson.databind.JsonMappingException;
import joptsimple.AbstractOptionSpec;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpecBuilder;
import joptsimple.util.EnumConverter;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.server.util.Json;
import org.apache.kafka.server.util.json.DecodeJson;
import org.apache.kafka.server.util.json.JsonObject;
import org.apache.kafka.server.util.json.JsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class LeaderElectionCommand {
    private static final Logger LOG = LoggerFactory.getLogger(LeaderElectionCommand.class);
    private static final DecodeJson.DecodeString STRING = new DecodeJson.DecodeString();
    private static final DecodeJson.DecodeInteger INT = new DecodeJson.DecodeInteger();

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    static int mainNoExit(String... args) {
        try {
            run(Duration.ofMillis(30000), args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void run(Duration timeoutMs, String... args) throws Exception {
        LeaderElectionCommandOptions commandOptions = new LeaderElectionCommandOptions(args);

        commandOptions.maybePrintHelpOrVersion();

        commandOptions.validate();
        ElectionType electionType = commandOptions.getElectionType();
        Optional<Set<TopicPartition>> jsonFileTopicPartitions =
            Optional.ofNullable(commandOptions.getPathToJsonFile())
                .map(LeaderElectionCommand::parseReplicaElectionData);

        Optional<String> topicOption = Optional.ofNullable(commandOptions.getTopic());
        Optional<Integer> partitionOption = Optional.ofNullable(commandOptions.getPartition());
        final Optional<Set<TopicPartition>> singleTopicPartition =
            (topicOption.isPresent() && partitionOption.isPresent()) ?
                Optional.of(Collections.singleton(new TopicPartition(topicOption.get(), partitionOption.get()))) :
                Optional.empty();

        /* Note: No need to look at --all-topic-partitions as we want this to be null if it is use.
         * The validate function should be checking that this option is required if the --topic and --path-to-json-file
         * are not specified.
         */
        Optional<Set<TopicPartition>> topicPartitions = jsonFileTopicPartitions.map(Optional::of).orElse(singleTopicPartition);

        Properties props = new Properties();
        if (commandOptions.hasAdminClientConfig()) {
            props.putAll(Utils.loadProps(commandOptions.getAdminClientConfig()));
        }
        props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, commandOptions.getBootstrapServer());
        if (!props.containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
            props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.toString((int) timeoutMs.toMillis()));
        }
        if (!props.containsKey(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG)) {
            props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString((int) (timeoutMs.toMillis() / 2)));
        }

        try (Admin adminClient = Admin.create(props)) {
            electLeaders(adminClient, electionType, topicPartitions);
        }
    }

    private static void electLeaders(Admin client, ElectionType electionType, Optional<Set<TopicPartition>> partitions) {
        LOG.debug("Calling AdminClient.electLeaders({}, {})", electionType, partitions.orElse(null));
        Map<TopicPartition, Optional<Throwable>> electionResults;
        try {
            electionResults = client.electLeaders(electionType, partitions.orElse(null)).partitions().get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TimeoutException) {
                String message = "Timeout waiting for election results";
                System.out.println(message);
                throw new AdminCommandFailedException(message, e.getCause());
            } else if (e.getCause() instanceof ClusterAuthorizationException) {
                String message = "Not authorized to perform leader election";
                System.out.println(message);
                throw new AdminCommandFailedException(message, e.getCause().getCause());
            } else {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            System.out.println("Error while making request");
            throw new RuntimeException(e);
        }

        Set<TopicPartition> succeeded = new HashSet<>();
        Set<TopicPartition> noop = new HashSet<>();
        Map<TopicPartition, Throwable> failed = new HashMap<>();

        electionResults.forEach((key, error) -> {
            if (error.isPresent()) {
                if (error.get() instanceof ElectionNotNeededException) {
                    noop.add(key);
                } else {
                    failed.put(key, error.get());
                }
            } else {
                succeeded.add(key);
            }
        });

        if (!succeeded.isEmpty()) {
            String partitionsAsString = succeeded.stream()
                .map(TopicPartition::toString)
                .collect(Collectors.joining(", "));
            System.out.println(String.format("Successfully completed leader election (%s) for partitions %s",
                electionType, partitionsAsString));
        }

        if (!noop.isEmpty()) {
            String partitionsAsString = noop.stream()
                .map(TopicPartition::toString)
                .collect(Collectors.joining(", "));
            System.out.println(String.format("Valid replica already elected for partitions %s", partitionsAsString));
        }

        if (!failed.isEmpty()) {
            AdminCommandFailedException rootException =
                new AdminCommandFailedException(String.format("%s replica(s) could not be elected", failed.size()));
            failed.forEach((key, value) -> {
                System.out.println(
                        String.format(
                                "Error completing leader election (%s) for partition: %s: %s",
                                electionType,
                                key,
                                value
                        )
                );
                rootException.addSuppressed(value);
            });
            throw rootException;
        }
    }

    private static Set<TopicPartition> parseReplicaElectionData(String path) {
        Optional<JsonValue> jsonFile;
        try {
            jsonFile = Json.parseFull(Utils.readFileAsString(path));
            return jsonFile.map(js -> {
                try {
                    return topicPartitions(js);
                } catch (JsonMappingException e) {
                    throw new RuntimeException(e);
                }
            }).orElseThrow(() -> new AdminOperationException("Replica election data is empty"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Set<TopicPartition> topicPartitions(JsonValue js) throws JsonMappingException {
        return js.asJsonObject().get("partitions")
            .map(partitionsList -> {
                try {
                    return toTopicPartition(partitionsList);
                } catch (JsonMappingException e) {
                    throw new RuntimeException(e);
                }
            })
            .orElseThrow(() -> new AdminOperationException("Replica election data is missing \"partitions\" field"));
    }

    private static Set<TopicPartition> toTopicPartition(JsonValue partitionsList) throws JsonMappingException {
        List<TopicPartition> partitions = new ArrayList<>();
        Iterator<JsonValue> iterator = partitionsList.asJsonArray().iterator();

        while (iterator.hasNext()) {
            JsonObject partitionJs = iterator.next().asJsonObject();
            String topic = partitionJs.apply("topic").to(STRING);
            int partition = partitionJs.apply("partition").to(INT);
            partitions.add(new TopicPartition(topic, partition));
        }

        Set<TopicPartition> duplicatePartitions  = partitions.stream()
            .filter(i -> Collections.frequency(partitions, i) > 1)
            .collect(Collectors.toSet());

        if (duplicatePartitions.size() > 0) {
            throw new AdminOperationException(String.format(
                "Replica election data contains duplicate partitions: %s", String.join(",", duplicatePartitions.toString()))
            );
        }
        return new HashSet<>(partitions);
    }

    static class LeaderElectionCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServer;
        private final ArgumentAcceptingOptionSpec<String> adminClientConfig;
        private final ArgumentAcceptingOptionSpec<String> pathToJsonFile;
        private final ArgumentAcceptingOptionSpec<String> topic;
        private final ArgumentAcceptingOptionSpec<Integer> partition;
        private final OptionSpecBuilder allTopicPartitions;
        private final ArgumentAcceptingOptionSpec<ElectionType> electionType;
        public LeaderElectionCommandOptions(String[] args) {
            super(args);
            bootstrapServer = parser
                .accepts(
                    "bootstrap-server",
                    "A hostname and port for the broker to connect to, in the form host:port. Multiple comma separated URLs can be given. REQUIRED.")
                .withRequiredArg()
                .describedAs("host:port")
                .ofType(String.class);
            adminClientConfig = parser
                .accepts(
                    "admin.config",
                    "Configuration properties files to pass to the admin client")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
            pathToJsonFile = parser
                .accepts(
                    "path-to-json-file",
                    "The JSON file with the list  of partition for which leader elections should be performed. This is an example format. \n{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\nNot allowed if --all-topic-partitions or --topic flags are specified.")
                .withRequiredArg()
                .describedAs("Path to JSON file")
                .ofType(String.class);
            topic = parser
                .accepts(
                    "topic",
                    "Name of topic for which to perform an election. Not allowed if --path-to-json-file or --all-topic-partitions is specified.")
                .withRequiredArg()
                .describedAs("topic name")
                .ofType(String.class);

            partition = parser
                .accepts(
                    "partition",
                    "Partition id for which to perform an election. REQUIRED if --topic is specified.")
                .withRequiredArg()
                .describedAs("partition id")
                .ofType(Integer.class);

            allTopicPartitions = parser
                .accepts(
                    "all-topic-partitions",
                    "Perform election on all of the eligible topic partitions based on the type of election (see the --election-type flag). Not allowed if --topic or --path-to-json-file is specified.");
            electionType = parser
                .accepts(
                    "election-type",
                    "Type of election to attempt. Possible values are \"preferred\" for preferred leader election or \"unclean\" for unclean leader election. If preferred election is selection, the election is only performed if the current leader is not the preferred leader for the topic partition. If unclean election is selected, the election is only performed if there are no leader for the topic partition. REQUIRED.")
                .withRequiredArg()
                .describedAs("election type")
                .withValuesConvertedBy(new ElectionTypeConverter());

            options = parser.parse(args);
        }

        public boolean hasAdminClientConfig() {
            return options.has(adminClientConfig);
        }

        public ElectionType getElectionType() {
            return options.valueOf(electionType);
        }

        public String getPathToJsonFile() {
            return options.valueOf(pathToJsonFile);
        }

        public String getBootstrapServer() {
            return options.valueOf(bootstrapServer);
        }

        public String getAdminClientConfig() {
            return options.valueOf(adminClientConfig);
        }

        public String getTopic() {
            return options.valueOf(topic);
        }

        public Integer getPartition() {
            return options.valueOf(partition);
        }

        public void validate() {
            // required options: --bootstrap-server and --election-type
            List<String> missingOptions = new ArrayList<>();

            if (!options.has(bootstrapServer)) {
                missingOptions.add(bootstrapServer.options().get(0));
            }
            if (!options.has(electionType)) {
                missingOptions.add(electionType.options().get(0));
            }
            if (!missingOptions.isEmpty()) {
                throw new AdminCommandFailedException("Missing required option(s): " + String.join(", ", missingOptions));
            }

            // One and only one is required: --topic, --all-topic-partitions or --path-to-json-file
            List<AbstractOptionSpec<?>> mutuallyExclusiveOptions = Arrays.asList(
                topic,
                allTopicPartitions,
                pathToJsonFile
            );

            long mutuallyExclusiveOptionsCount = mutuallyExclusiveOptions.stream()
                .filter(abstractOptionSpec -> options.has(abstractOptionSpec))
                .count();
            // 1 is the only correct configuration, don't throw an exception
            if (mutuallyExclusiveOptionsCount != 1) {
                throw new AdminCommandFailedException(
                    "One and only one of the following options is required: " +
                        mutuallyExclusiveOptions.stream().map(opt -> opt.options().get(0)).collect(Collectors.joining(", "))
                );
            }
            // --partition if and only if --topic is used
            if (options.has(topic) && !options.has(partition)) {
                throw new AdminCommandFailedException(String.format("Missing required option(s): %s",
                    partition.options().get(0)));
            }

            if (!options.has(topic) && options.has(partition)) {
                throw new AdminCommandFailedException(String.format("Option %s is only allowed if %s is used",
                    partition.options().get(0),
                    topic.options().get(0)
                ));
            }
        }

        public void maybePrintHelpOrVersion() {
            CommandLineUtils.maybePrintHelpOrVersion(
                this,
                "This tool attempts to elect a new leader for a set of topic partitions. The type of elections supported are preferred replicas and unclean replicas."
            );
        }

    }

    static class ElectionTypeConverter extends EnumConverter<ElectionType> {
        public ElectionTypeConverter() {
            super(ElectionType.class);
        }
    }
}
