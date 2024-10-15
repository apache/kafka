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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.AdminCommandFailedException;
import org.apache.kafka.server.common.AdminOperationException;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.tools.filter.TopicFilter.IncludeList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

public abstract class TopicCommand {
    private static final Logger LOG = LoggerFactory.getLogger(TopicCommand.class);

    public static void main(String... args) {
        Exit.exit(mainNoExit(args));
    }

    private static int mainNoExit(String... args) {
        try {
            execute(args);
            return 0;
        } catch (Throwable e) {
            System.err.println(e.getMessage());
            System.err.println(Utils.stackTrace(e));
            return 1;
        }
    }

    static void execute(String... args) throws Exception {
        TopicCommandOptions opts = new TopicCommandOptions(args);
        TopicService topicService = new TopicService(opts.commandConfig(), opts.bootstrapServer());
        int exitCode = 0;
        try {
            if (opts.hasCreateOption()) {
                topicService.createTopic(opts);
            } else if (opts.hasAlterOption()) {
                topicService.alterTopic(opts);
            } else if (opts.hasListOption()) {
                topicService.listTopics(opts);
            } else if (opts.hasDescribeOption()) {
                topicService.describeTopic(opts);
            } else if (opts.hasDeleteOption()) {
                topicService.deleteTopic(opts);
            }
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause != null) {
                printException(cause);
            } else {
                printException(e);
            }
            exitCode = 1;
        } catch (Throwable e) {
            printException(e);
            exitCode = 1;
        } finally {
            topicService.close();
            Exit.exit(exitCode);
        }
    }

    private static void printException(Throwable e) {
        System.out.println("Error while executing topic command : " + e.getMessage());
        LOG.error(Utils.stackTrace(e));
    }

    static Map<Integer, List<Integer>> parseReplicaAssignment(String replicaAssignmentList) {
        String[] partitionList = replicaAssignmentList.split(",");
        Map<Integer, List<Integer>> ret = new LinkedHashMap<>();
        for (int i = 0; i < partitionList.length; i++) {
            List<Integer> brokerList = Arrays.stream(partitionList[i].split(":"))
                .map(String::trim)
                .mapToInt(Integer::parseInt)
                .boxed()
                .collect(Collectors.toList());
            Collection<Integer> duplicateBrokers = ToolsUtils.duplicates(brokerList);
            if (!duplicateBrokers.isEmpty()) {
                throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: " +
                    duplicateBrokers.stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(","))
                );
            }
            ret.put(i, brokerList);
            if (ret.get(i).size() != ret.get(0).size()) {
                throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList);
            }
        }
        return ret;
    }

    @SuppressWarnings("deprecation")
    private static Properties parseTopicConfigsToBeAdded(TopicCommandOptions opts) {
        List<List<String>> configsToBeAdded = opts.topicConfig().orElse(Collections.emptyList())
            .stream()
            .map(s -> Arrays.asList(s.split("\\s*=\\s*")))
            .collect(Collectors.toList());

        if (!configsToBeAdded.stream().allMatch(config -> config.size() == 2)) {
            throw new IllegalArgumentException("requirement failed: Invalid topic config: all configs to be added must be in the format \"key=val\".");
        }

        Properties props = new Properties();
        configsToBeAdded.stream()
            .forEach(pair -> props.setProperty(pair.get(0).trim(), pair.get(1).trim()));
        LogConfig.validate(props);
        if (props.containsKey(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)) {
            System.out.println("WARNING: The configuration ${TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG}=${props.getProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)} is specified. " +
                "This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker or " +
                "if the inter.broker.protocol.version is 3.0 or newer. This configuration is deprecated and it will be removed in Apache Kafka 4.0.");
        }
        return props;
    }

    // It is possible for a reassignment to complete between the time we have fetched its state and the time
    // we fetch partition metadata. In this case, we ignore the reassignment when determining replication factor.
    public static boolean isReassignmentInProgress(TopicPartitionInfo tpi, PartitionReassignment ra) {
        // Reassignment is still in progress as long as the removing and adding replicas are still present
        Set<Integer> allReplicaIds = tpi.replicas().stream().map(Node::id).collect(Collectors.toSet());
        Set<Integer> changingReplicaIds = new HashSet<>();
        if (ra != null) {
            changingReplicaIds.addAll(ra.removingReplicas());
            changingReplicaIds.addAll(ra.addingReplicas());
        }
        return allReplicaIds.stream().anyMatch(changingReplicaIds::contains);

    }

    private static Integer getReplicationFactor(TopicPartitionInfo tpi, PartitionReassignment reassignment) {
        return isReassignmentInProgress(tpi, reassignment) ?
            reassignment.replicas().size() - reassignment.addingReplicas().size() :
            tpi.replicas().size();
    }

    /**
     * ensures topic existence and throws exception if topic doesn't exist
     *
     * @param foundTopics        Topics that were found to match the requested topic name.
     * @param requestedTopic     Name of the topic that was requested.
     * @param requireTopicExists Indicates if the topic needs to exist for the operation to be successful.
     *                           If set to true, the command will throw an exception if the topic with the
     *                           requested name does not exist.
     */
    private static void ensureTopicExists(List<String> foundTopics, Optional<String> requestedTopic, Boolean requireTopicExists) {
        // If no topic name was mentioned, do not need to throw exception.
        if (requestedTopic.isPresent() && !requestedTopic.get().isEmpty() && requireTopicExists && foundTopics.isEmpty()) {
            // If given topic doesn't exist then throw exception
            throw new IllegalArgumentException(String.format("Topic '%s' does not exist as expected", requestedTopic));
        }
    }

    private static List<String> doGetTopics(List<String> allTopics, Optional<String> topicIncludeList, Boolean excludeInternalTopics) {
        if (topicIncludeList.isPresent()) {
            IncludeList topicsFilter = new IncludeList(topicIncludeList.get());
            return allTopics.stream()
                .filter(topic -> topicsFilter.isTopicAllowed(topic, excludeInternalTopics))
                .collect(Collectors.toList());
        } else {
            return allTopics.stream()
                .filter(topic -> !(Topic.isInternal(topic) && excludeInternalTopics))
                .collect(Collectors.toList());
        }
    }

    /**
     * ensures topic existence and throws exception if topic doesn't exist
     *
     * @param foundTopicIds        Topics that were found to match the requested topic id.
     * @param requestedTopicId     Id of the topic that was requested.
     * @param requireTopicIdExists Indicates if the topic needs to exist for the operation to be successful.
     *                             If set to true, the command will throw an exception if the topic with the
     *                             requested id does not exist.
     */
    private static void ensureTopicIdExists(List<Uuid> foundTopicIds, Uuid requestedTopicId, Boolean requireTopicIdExists) {
        // If no topic id was mentioned, do not need to throw exception.
        if (requestedTopicId != null && requireTopicIdExists && foundTopicIds.isEmpty()) {
            // If given topicId doesn't exist then throw exception
            throw new IllegalArgumentException(String.format("TopicId '%s' does not exist as expected", requestedTopicId));
        }
    }

    static class CommandTopicPartition {
        private final String name;
        private final Optional<Integer> partitions;
        private final Optional<Integer> replicationFactor;
        private final Map<Integer, List<Integer>> replicaAssignment;
        private final Properties configsToAdd;

        private final TopicCommandOptions opts;

        public CommandTopicPartition(TopicCommandOptions options) {
            opts = options;
            name = options.topic().get();
            partitions = options.partitions();
            replicationFactor = options.replicationFactor();
            replicaAssignment = options.replicaAssignment().orElse(Collections.emptyMap());
            configsToAdd = parseTopicConfigsToBeAdded(options);
        }

        public Boolean hasReplicaAssignment() {
            return !replicaAssignment.isEmpty();
        }

        public Boolean ifTopicDoesntExist() {
            return opts.ifNotExists();
        }
    }

    static class TopicDescription {
        private final String topic;
        private final Uuid topicId;
        private final Integer numPartitions;
        private final Integer replicationFactor;
        private final Config config;
        private final Boolean markedForDeletion;

        public TopicDescription(String topic, Uuid topicId, Integer numPartitions, Integer replicationFactor, Config config, Boolean markedForDeletion) {
            this.topic = topic;
            this.topicId = topicId;
            this.numPartitions = numPartitions;
            this.replicationFactor = replicationFactor;
            this.config = config;
            this.markedForDeletion = markedForDeletion;
        }

        public void printDescription() {
            String configsAsString = config.entries().stream()
                .filter(config -> !config.isDefault())
                .map(ce -> ce.name() + "=" + ce.value())
                .collect(Collectors.joining(","));
            System.out.print("Topic: " +  topic);
            if (!topicId.equals(Uuid.ZERO_UUID))
                System.out.print("\tTopicId: " + topicId);
            System.out.print("\tPartitionCount: " + numPartitions);
            System.out.print("\tReplicationFactor: " + replicationFactor);
            System.out.print("\tConfigs: " + configsAsString);
            System.out.print(markedForDeletion ? "\tMarkedForDeletion: true" : "");
            System.out.println();
        }
    }

    static class PartitionDescription {
        private final String topic;
        private final TopicPartitionInfo info;
        private final Config config;
        private final Boolean markedForDeletion;
        private final PartitionReassignment reassignment;

        PartitionDescription(String topic,
                             TopicPartitionInfo info,
                             Config config,
                             Boolean markedForDeletion,
                             PartitionReassignment reassignment) {
            this.topic = topic;
            this.info = info;
            this.config = config;
            this.markedForDeletion = markedForDeletion;
            this.reassignment = reassignment;
        }

        public Integer minIsrCount() {
            return Integer.parseInt(config.get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value());
        }

        public Boolean isUnderReplicated() {
            return getReplicationFactor(info, reassignment) - info.isr().size() > 0;
        }

        public boolean hasLeader() {
            return info.leader() != null;
        }

        public Boolean isUnderMinIsr() {
            return !hasLeader() ||  info.isr().size() < minIsrCount();
        }

        public Boolean isAtMinIsrPartitions() {
            return minIsrCount() == info.isr().size();
        }

        public Boolean hasUnavailablePartitions(Set<Integer> liveBrokers) {
            return !hasLeader() || !liveBrokers.contains(info.leader().id());
        }

        public void printDescription() {
            System.out.print("\tTopic: " + topic);
            System.out.print("\tPartition: " + info.partition());
            System.out.print("\tLeader: " + (hasLeader() ? info.leader().id() : "none"));
            System.out.print("\tReplicas: " + info.replicas().stream()
                .map(node -> Integer.toString(node.id()))
                .collect(Collectors.joining(",")));
            System.out.print("\tIsr: " + info.isr().stream()
                .map(node -> Integer.toString(node.id()))
                .collect(Collectors.joining(",")));
            if (reassignment != null) {
                System.out.print("\tAdding Replicas: " + reassignment.addingReplicas().stream()
                    .map(node -> node.toString())
                    .collect(Collectors.joining(",")));
                System.out.print("\tRemoving Replicas: " + reassignment.removingReplicas().stream()
                    .map(node -> node.toString())
                    .collect(Collectors.joining(",")));
            }

            if (info.elr() != null) {
                System.out.print("\tElr: " + info.elr().stream()
                    .map(node -> Integer.toString(node.id()))
                    .collect(Collectors.joining(",")));
            } else {
                System.out.print("\tElr: N/A");
            }

            if (info.lastKnownElr() != null) {
                System.out.print("\tLastKnownElr: " + info.lastKnownElr().stream()
                    .map(node -> Integer.toString(node.id()))
                    .collect(Collectors.joining(",")));
            } else {
                System.out.print("\tLastKnownElr: N/A");
            }
            System.out.print(markedForDeletion ? "\tMarkedForDeletion: true" : "");
            System.out.println();
        }
    }

    static class DescribeOptions {
        private final TopicCommandOptions opts;
        private final Set<Integer> liveBrokers;
        private final boolean describeConfigs;
        private final boolean describePartitions;

        public DescribeOptions(TopicCommandOptions opts, Set<Integer> liveBrokers) {
            this.opts = opts;
            this.liveBrokers = liveBrokers;
            this.describeConfigs = !opts.reportUnavailablePartitions() &&
                !opts.reportUnderReplicatedPartitions() &&
                !opts.reportUnderMinIsrPartitions() &&
                !opts.reportAtMinIsrPartitions();
            this.describePartitions = !opts.reportOverriddenConfigs();
        }

        private boolean shouldPrintUnderReplicatedPartitions(PartitionDescription partitionDescription) {
            return opts.reportUnderReplicatedPartitions() && partitionDescription.isUnderReplicated();
        }

        private boolean shouldPrintUnavailablePartitions(PartitionDescription partitionDescription) {
            return opts.reportUnavailablePartitions() && partitionDescription.hasUnavailablePartitions(liveBrokers);
        }

        private boolean shouldPrintUnderMinIsrPartitions(PartitionDescription partitionDescription) {
            return opts.reportUnderMinIsrPartitions() && partitionDescription.isUnderMinIsr();
        }

        private boolean shouldPrintAtMinIsrPartitions(PartitionDescription partitionDescription) {
            return opts.reportAtMinIsrPartitions() && partitionDescription.isAtMinIsrPartitions();
        }

        private boolean shouldPrintTopicPartition(PartitionDescription partitionDesc) {
            return describeConfigs ||
                shouldPrintUnderReplicatedPartitions(partitionDesc) ||
                shouldPrintUnavailablePartitions(partitionDesc) ||
                shouldPrintUnderMinIsrPartitions(partitionDesc) ||
                shouldPrintAtMinIsrPartitions(partitionDesc);
        }

        public void maybePrintPartitionDescription(PartitionDescription desc) {
            if (shouldPrintTopicPartition(desc)) {
                desc.printDescription();
            }
        }
    }

    public static class TopicService implements AutoCloseable {
        private final Admin adminClient;

        public TopicService(Properties commandConfig, Optional<String> bootstrapServer) {
            this.adminClient = createAdminClient(commandConfig, bootstrapServer);
        }

        public TopicService(Admin admin) {
            this.adminClient = admin;
        }

        private static Admin createAdminClient(Properties commandConfig, Optional<String> bootstrapServer) {
            if (bootstrapServer.isPresent()) {
                commandConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer.get());
            }
            return Admin.create(commandConfig);
        }

        public void createTopic(TopicCommandOptions opts) throws Exception {
            CommandTopicPartition topic = new CommandTopicPartition(opts);
            if (Topic.hasCollisionChars(topic.name)) {
                System.out.println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could " +
                    "collide. To avoid issues it is best to use either, but not both.");
            }
            createTopic(topic);
        }

        public void createTopic(CommandTopicPartition topic) throws Exception {
            if (topic.replicationFactor.filter(rf -> rf > Short.MAX_VALUE || rf < 1).isPresent()) {
                throw new IllegalArgumentException("The replication factor must be between 1 and " + Short.MAX_VALUE + " inclusive");
            }
            if (topic.partitions.filter(p -> p < 1).isPresent()) {
                throw new IllegalArgumentException("The partitions must be greater than 0");
            }

            try {
                NewTopic newTopic;
                if (topic.hasReplicaAssignment()) {
                    newTopic = new NewTopic(topic.name, topic.replicaAssignment);
                } else {
                    newTopic = new NewTopic(topic.name, topic.partitions, topic.replicationFactor.map(Integer::shortValue));
                }

                Map<String, String> configsMap = topic.configsToAdd.stringPropertyNames().stream()
                    .collect(Collectors.toMap(name -> name, name -> topic.configsToAdd.getProperty(name)));

                newTopic.configs(configsMap);
                CreateTopicsResult createResult = adminClient.createTopics(Collections.singleton(newTopic),
                    new CreateTopicsOptions().retryOnQuotaViolation(false));
                createResult.all().get();
                System.out.println("Created topic " + topic.name + ".");
            } catch (ExecutionException e) {
                if (e.getCause() == null) {
                    throw e;
                }
                if (!(e.getCause() instanceof TopicExistsException && topic.ifTopicDoesntExist())) {
                    throw (Exception) e.getCause();
                }
            }
        }

        public void listTopics(TopicCommandOptions opts) throws ExecutionException, InterruptedException {
            String results = getTopics(opts.topic(), opts.excludeInternalTopics())
                .stream()
                .collect(Collectors.joining("\n"));
            System.out.println(results);
        }

        public void alterTopic(TopicCommandOptions opts) throws ExecutionException, InterruptedException {
            CommandTopicPartition topic = new CommandTopicPartition(opts);
            List<String> topics = getTopics(opts.topic(), opts.excludeInternalTopics());
            ensureTopicExists(topics, opts.topic(), !opts.ifExists());

            if (!topics.isEmpty()) {
                Map<String, KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> topicsInfo = adminClient.describeTopics(topics).topicNameValues();
                Map<String, NewPartitions> newPartitions = topics.stream()
                    .map(topicName -> topicNewPartitions(topic, topicsInfo, topicName))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                adminClient.createPartitions(newPartitions, new CreatePartitionsOptions().retryOnQuotaViolation(false)).all().get();
            }
        }

        private AbstractMap.SimpleEntry<String, NewPartitions> topicNewPartitions(
            CommandTopicPartition topic,
            Map<String, KafkaFuture<org.apache.kafka.clients.admin.TopicDescription>> topicsInfo,
            String topicName) {
            if (topic.hasReplicaAssignment()) {
                try {
                    Integer startPartitionId = topicsInfo.get(topicName).get().partitions().size();
                    Map<Integer, List<Integer>> replicaMap = topic.replicaAssignment.entrySet().stream()
                        .skip(startPartitionId)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    List<List<Integer>> newAssignment = new ArrayList<>(replicaMap.values());
                    return new AbstractMap.SimpleEntry<>(topicName, NewPartitions.increaseTo(topic.partitions.get(), newAssignment));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            return new AbstractMap.SimpleEntry<>(topicName, NewPartitions.increaseTo(topic.partitions.get()));
        }

        public Map<TopicPartition, PartitionReassignment> listAllReassignments(Set<TopicPartition> topicPartitions) {
            try {
                return adminClient.listPartitionReassignments(topicPartitions).reassignments().get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof UnsupportedVersionException || cause instanceof ClusterAuthorizationException) {
                    LOG.debug("Couldn't query reassignments through the AdminClient API: " + cause.getMessage(), cause);
                    return Collections.emptyMap();
                } else {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void describeTopic(TopicCommandOptions opts) throws ExecutionException, InterruptedException {
            // If topicId is provided and not zero, will use topicId regardless of topic name
            Optional<Uuid> inputTopicId = opts.topicId()
                .map(Uuid::fromString).filter(uuid -> !uuid.equals(Uuid.ZERO_UUID));
            Boolean useTopicId = inputTopicId.isPresent();

            List<Uuid> topicIds;
            List<String> topics;
            if (useTopicId) {
                topicIds = getTopicIds(inputTopicId.get(), opts.excludeInternalTopics());
                topics = Collections.emptyList();
            } else {
                topicIds = Collections.emptyList();
                topics = getTopics(opts.topic(), opts.excludeInternalTopics());
            }

            // Only check topic name when topicId is not provided
            if (useTopicId) {
                ensureTopicIdExists(topicIds, inputTopicId.get(), !opts.ifExists());
            } else {
                ensureTopicExists(topics, opts.topic(), !opts.ifExists());
            }
            List<org.apache.kafka.clients.admin.TopicDescription> topicDescriptions = new ArrayList<>();

            if (!topicIds.isEmpty()) {
                Map<Uuid, org.apache.kafka.clients.admin.TopicDescription> descTopics =
                    adminClient.describeTopics(TopicCollection.ofTopicIds(topicIds)).allTopicIds().get();
                topicDescriptions = new ArrayList<>(descTopics.values());
            }

            if (!topics.isEmpty()) {
                Map<String, org.apache.kafka.clients.admin.TopicDescription> descTopics =
                    adminClient.describeTopics(TopicCollection.ofTopicNames(topics),
                        new DescribeTopicsOptions()
                            .partitionSizeLimitPerResponse(opts.partitionSizeLimitPerResponse().orElse(2000))).allTopicNames().get();
                topicDescriptions = new ArrayList<>(descTopics.values());
            }

            List<String> topicNames = topicDescriptions.stream()
                .map(org.apache.kafka.clients.admin.TopicDescription::name)
                .collect(Collectors.toList());
            Map<ConfigResource, KafkaFuture<Config>> allConfigs = adminClient.describeConfigs(
                topicNames.stream()
                    .map(name -> new ConfigResource(ConfigResource.Type.TOPIC, name))
                    .collect(Collectors.toList())
            ).values();
            List<Integer> liveBrokers = adminClient.describeCluster().nodes().get().stream()
                .map(Node::id)
                .collect(Collectors.toList());
            DescribeOptions describeOptions = new DescribeOptions(opts, new HashSet<>(liveBrokers));
            Set<TopicPartition> topicPartitions = topicDescriptions
                .stream()
                .flatMap(td -> td.partitions().stream()
                    .map(p -> new TopicPartition(td.name(), p.partition())))
                .collect(Collectors.toSet());
            Map<TopicPartition, PartitionReassignment> reassignments = listAllReassignments(topicPartitions);
            for (org.apache.kafka.clients.admin.TopicDescription td : topicDescriptions) {
                String topicName = td.name();
                Uuid topicId = td.topicId();
                Config config = allConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, topicName)).get();
                ArrayList<TopicPartitionInfo> sortedPartitions = new ArrayList<>(td.partitions());
                sortedPartitions.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
                printDescribeConfig(opts, describeOptions, reassignments, td, topicName, topicId, config, sortedPartitions);
                printPartitionDescription(describeOptions, reassignments, td, topicName, config, sortedPartitions);
            }
        }

        private void printPartitionDescription(DescribeOptions describeOptions, Map<TopicPartition, PartitionReassignment> reassignments, org.apache.kafka.clients.admin.TopicDescription td, String topicName, Config config, ArrayList<TopicPartitionInfo> sortedPartitions) {
            if (describeOptions.describePartitions) {
                for (TopicPartitionInfo partition : sortedPartitions) {
                    PartitionReassignment reassignment =
                        reassignments.get(new TopicPartition(td.name(), partition.partition()));
                    PartitionDescription partitionDesc = new PartitionDescription(topicName,
                        partition, config, false, reassignment);
                    describeOptions.maybePrintPartitionDescription(partitionDesc);
                }
            }
        }

        private void printDescribeConfig(TopicCommandOptions opts, DescribeOptions describeOptions, Map<TopicPartition, PartitionReassignment> reassignments, org.apache.kafka.clients.admin.TopicDescription td, String topicName, Uuid topicId, Config config, ArrayList<TopicPartitionInfo> sortedPartitions) {
            if (describeOptions.describeConfigs) {
                List<ConfigEntry> entries = new ArrayList<>(config.entries());
                boolean hasNonDefault = entries.stream().anyMatch(e -> !e.isDefault());
                if (!opts.reportOverriddenConfigs() || hasNonDefault) {
                    int numPartitions = td.partitions().size();
                    TopicPartitionInfo firstPartition = sortedPartitions.get(0);
                    PartitionReassignment reassignment =
                        reassignments.get(new TopicPartition(td.name(), firstPartition.partition()));
                    TopicDescription topicDesc = new TopicDescription(topicName, topicId,
                        numPartitions, getReplicationFactor(firstPartition, reassignment),
                        config, false);
                    topicDesc.printDescription();
                }
            }
        }

        public void deleteTopic(TopicCommandOptions opts) throws ExecutionException, InterruptedException {
            List<String> topics = getTopics(opts.topic(), opts.excludeInternalTopics());
            ensureTopicExists(topics, opts.topic(), !opts.ifExists());
            adminClient.deleteTopics(Collections.unmodifiableList(topics),
                new DeleteTopicsOptions().retryOnQuotaViolation(false)
            ).all().get();
        }

        public List<String> getTopics(Optional<String> topicIncludeList, boolean excludeInternalTopics) throws ExecutionException, InterruptedException {
            ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
            if (!excludeInternalTopics) {
                listTopicsOptions.listInternal(true);
            }

            Set<String> allTopics = adminClient.listTopics(listTopicsOptions).names().get();
            return doGetTopics(allTopics.stream().sorted().collect(Collectors.toList()), topicIncludeList, excludeInternalTopics);
        }

        public List<Uuid> getTopicIds(Uuid topicIdIncludeList, boolean excludeInternalTopics) throws ExecutionException, InterruptedException {
            ListTopicsResult allTopics = excludeInternalTopics ? adminClient.listTopics() :
                adminClient.listTopics(new ListTopicsOptions().listInternal(true));
            List<Uuid> allTopicIds = allTopics.listings().get().stream()
                .map(TopicListing::topicId)
                .sorted()
                .collect(Collectors.toList());
            return allTopicIds.contains(topicIdIncludeList) ?
                Collections.singletonList(topicIdIncludeList) :
                Collections.emptyList();
        }

        @Override
        public void close() throws Exception {
            adminClient.close();
        }
    }

    public static final class TopicCommandOptions extends CommandDefaultOptions {
        private final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;

        private final ArgumentAcceptingOptionSpec<String> commandConfigOpt;

        private final OptionSpecBuilder listOpt;

        private final OptionSpecBuilder createOpt;

        private final OptionSpecBuilder deleteOpt;

        private final OptionSpecBuilder alterOpt;

        private final OptionSpecBuilder describeOpt;

        private final ArgumentAcceptingOptionSpec<String> topicOpt;

        private final ArgumentAcceptingOptionSpec<String> topicIdOpt;

        private final String nl;

        private static final String KAFKA_CONFIGS_CLI_SUPPORTS_ALTERING_TOPIC_CONFIGS =
                " (To alter topic configurations, the kafka-configs tool can be used.)";

        private final ArgumentAcceptingOptionSpec<String> configOpt;

        /**
         * @deprecated since 4.0 and should not be used any longer.
         */
        @Deprecated
        private final ArgumentAcceptingOptionSpec<String> deleteConfigOpt;

        private final ArgumentAcceptingOptionSpec<Integer> partitionsOpt;

        private final ArgumentAcceptingOptionSpec<Integer> replicationFactorOpt;

        private final ArgumentAcceptingOptionSpec<String> replicaAssignmentOpt;

        private final OptionSpecBuilder reportUnderReplicatedPartitionsOpt;

        private final OptionSpecBuilder reportUnavailablePartitionsOpt;

        private final OptionSpecBuilder reportUnderMinIsrPartitionsOpt;

        private final OptionSpecBuilder reportAtMinIsrPartitionsOpt;

        private final OptionSpecBuilder topicsWithOverridesOpt;

        private final OptionSpecBuilder ifExistsOpt;

        private final OptionSpecBuilder ifNotExistsOpt;

        private final OptionSpecBuilder excludeInternalTopicOpt;

        private final ArgumentAcceptingOptionSpec<Integer> partitionSizeLimitPerResponseOpt;

        private final Set<OptionSpec<?>> allTopicLevelOpts;

        private final Set<OptionSpecBuilder> allReplicationReportOpts;

        public TopicCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
            commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                .withRequiredArg()
                .describedAs("command config property file")
                .ofType(String.class);

            listOpt = parser.accepts("list", "List all available topics.");
            createOpt = parser.accepts("create", "Create a new topic.");
            deleteOpt = parser.accepts("delete", "Delete a topic.");
            alterOpt = parser.accepts("alter", "Alter the number of partitions and replica assignment." +
                    KAFKA_CONFIGS_CLI_SUPPORTS_ALTERING_TOPIC_CONFIGS);
            describeOpt = parser.accepts("describe", "List details for the given topics.");
            topicOpt = parser.accepts("topic", "The topic to create, alter, describe or delete. It also accepts a regular " +
                            "expression, except for --create option. Put topic name in double quotes and use the '\\' prefix " +
                            "to escape regular expression symbols; e.g. \"test\\.topic\".")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
            topicIdOpt = parser.accepts("topic-id", "The topic-id to describe.")
                .withRequiredArg()
                .describedAs("topic-id")
                .ofType(String.class);
            nl = System.lineSeparator();

            String logConfigNames = LogConfig.configNames().stream().map(config -> "\t" + config).collect(Collectors.joining(nl));
            configOpt = parser.accepts("config",  "A topic configuration override for the topic being created." +
                            " The following is a list of valid configurations: " + nl + logConfigNames + nl +
                            "See the Kafka documentation for full details on the topic configs." +
                            " It is supported only in combination with --create." +
                            KAFKA_CONFIGS_CLI_SUPPORTS_ALTERING_TOPIC_CONFIGS)
                .withRequiredArg()
                .describedAs("name=value")
                .ofType(String.class);

            deleteConfigOpt = parser.accepts("delete-config", "This option is no longer supported and has been deprecated since 4.0")
                .withRequiredArg()
                .describedAs("name")
                .ofType(String.class);
            partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
                    "altered. If not supplied with --create, the topic uses the cluster default. (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected).")
                .withRequiredArg()
                .describedAs("# of partitions")
                .ofType(java.lang.Integer.class);
            replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created. If not supplied, the topic uses the cluster default.")
                .withRequiredArg()
                .describedAs("replication factor")
                .ofType(java.lang.Integer.class);
            replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")
                    .withRequiredArg()
                    .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                            "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                .ofType(String.class);
            reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                "If set when describing topics, only show under-replicated partitions.");
            reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                "If set when describing topics, only show partitions whose leader is not available.");
            reportUnderMinIsrPartitionsOpt = parser.accepts("under-min-isr-partitions",
                "If set when describing topics, only show partitions whose isr count is less than the configured minimum.");
            reportAtMinIsrPartitionsOpt = parser.accepts("at-min-isr-partitions",
                "If set when describing topics, only show partitions whose isr count is equal to the configured minimum.");
            topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                "If set when describing topics, only show topics that have overridden configs.");
            ifExistsOpt = parser.accepts("if-exists",
                "If set when altering or deleting or describing topics, the action will only execute if the topic exists.");
            ifNotExistsOpt = parser.accepts("if-not-exists",
                "If set when creating topics, the action will only execute if the topic does not already exist.");
            excludeInternalTopicOpt = parser.accepts("exclude-internal",
                "Exclude internal topics when listing or describing topics. By default, the internal topics are included.");
            partitionSizeLimitPerResponseOpt = parser.accepts("partition-size-limit-per-response",
                "The maximum partition size to be included in one DescribeTopicPartitions response.")
                    .withRequiredArg()
                    .describedAs("maximum number of partitions per response")
                    .ofType(java.lang.Integer.class);
            options = parser.parse(args);

            allTopicLevelOpts = new HashSet<>(Arrays.asList(alterOpt, createOpt, describeOpt, listOpt, deleteOpt));
            allReplicationReportOpts = new HashSet<>(Arrays.asList(reportUnderReplicatedPartitionsOpt, reportUnderMinIsrPartitionsOpt, reportAtMinIsrPartitionsOpt, reportUnavailablePartitionsOpt));

            checkArgs();
        }

        public Boolean has(OptionSpec<?> builder) {
            return options.has(builder);
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option) {
            return valueAsOption(option, Optional.empty());
        }

        public <A> Optional<List<A>> valuesAsOption(OptionSpec<A> option) {
            return valuesAsOption(option, Collections.emptyList());
        }

        public <A> Optional<A> valueAsOption(OptionSpec<A> option, Optional<A> defaultValue) {
            if (has(option)) {
                return Optional.of(options.valueOf(option));
            } else {
                return defaultValue;
            }
        }

        public <A> Optional<List<A>> valuesAsOption(OptionSpec<A> option, List<A> defaultValue) {
            return options.has(option) ? Optional.of(options.valuesOf(option)) : Optional.of(defaultValue);
        }

        public Boolean hasCreateOption() {
            return has(createOpt);
        }

        public Boolean hasAlterOption() {
            return has(alterOpt);
        }

        public Boolean hasListOption() {
            return has(listOpt);
        }

        public Boolean hasDescribeOption() {
            return has(describeOpt);
        }

        public Boolean hasDeleteOption() {
            return has(deleteOpt);
        }

        public Optional<String> bootstrapServer() {
            return valueAsOption(bootstrapServerOpt);
        }

        public Properties commandConfig() throws IOException {
            if (has(commandConfigOpt)) {
                return Utils.loadProps(options.valueOf(commandConfigOpt));
            } else {
                return new Properties();
            }
        }

        public Optional<String> topic() {
            return valueAsOption(topicOpt);
        }

        public Optional<String> topicId() {
            return valueAsOption(topicIdOpt);
        }

        public Optional<Integer> partitions() {
            return valueAsOption(partitionsOpt);
        }

        public Optional<Integer> replicationFactor() {
            return valueAsOption(replicationFactorOpt);
        }

        public Optional<Map<Integer, List<Integer>>> replicaAssignment() {
            if (has(replicaAssignmentOpt) && !Optional.of(options.valueOf(replicaAssignmentOpt)).orElse("").isEmpty())
                return Optional.of(parseReplicaAssignment(options.valueOf(replicaAssignmentOpt)));
            else
                return Optional.empty();
        }

        public Boolean reportUnderReplicatedPartitions() {
            return has(reportUnderReplicatedPartitionsOpt);
        }

        public Boolean reportUnavailablePartitions() {
            return has(reportUnavailablePartitionsOpt);
        }

        public Boolean reportUnderMinIsrPartitions() {
            return has(reportUnderMinIsrPartitionsOpt);
        }

        public Boolean reportAtMinIsrPartitions() {
            return has(reportAtMinIsrPartitionsOpt);
        }

        public Boolean reportOverriddenConfigs() {
            return has(topicsWithOverridesOpt);
        }

        public Boolean ifExists() {
            return has(ifExistsOpt);
        }

        public Boolean ifNotExists() {
            return has(ifNotExistsOpt);
        }

        public Boolean excludeInternalTopics() {
            return has(excludeInternalTopicOpt);
        }

        public Optional<Integer> partitionSizeLimitPerResponse() {
            return valueAsOption(partitionSizeLimitPerResponseOpt);
        }

        public Optional<List<String>> topicConfig() {
            return valuesAsOption(configOpt);
        }

        public void checkArgs() {
            if (args.length == 0)
                CommandLineUtils.printUsageAndExit(parser, "Create, delete, describe, or change a topic.");

            CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to create, delete, describe, or change a topic.");

            // should have exactly one action
            long actions =
                Arrays.asList(createOpt, listOpt, alterOpt, describeOpt, deleteOpt)
                    .stream().filter(options::has)
                    .count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete");

            if (has(deleteConfigOpt)) {
                System.err.println("delete-config option is no longer supported and deprecated since version 4.0. The config will be fully removed in future releases.");
            }

            checkRequiredArgs();
            checkInvalidArgs();
        }

        private void checkRequiredArgs() {
            // check required args
            if (!has(bootstrapServerOpt))
                throw new IllegalArgumentException("--bootstrap-server must be specified");
            if (has(describeOpt) && has(ifExistsOpt)) {
                if (!has(topicOpt) && !has(topicIdOpt))
                    CommandLineUtils.printUsageAndExit(parser, "--topic or --topic-id is required to describe a topic");
                if (has(topicOpt) && has(topicIdOpt))
                    System.out.println("Only topic id will be used when both --topic and --topic-id are specified and topicId is not Uuid.ZERO_UUID");
            }
            if (!has(listOpt) && !has(describeOpt))
                CommandLineUtils.checkRequiredArgs(parser, options, topicOpt);
            if (has(alterOpt)) {
                Set<OptionSpec<?>> usedOptions = new HashSet<>(Arrays.asList(bootstrapServerOpt, configOpt));
                Set<OptionSpec<?>> invalidOptions = new HashSet<>(Arrays.asList(alterOpt));
                CommandLineUtils.checkInvalidArgsSet(parser, options, usedOptions, invalidOptions, Optional.of(KAFKA_CONFIGS_CLI_SUPPORTS_ALTERING_TOPIC_CONFIGS));
                CommandLineUtils.checkRequiredArgs(parser, options, partitionsOpt);
            }
        }

        private void checkInvalidArgs() {
            // check invalid args
            CommandLineUtils.checkInvalidArgs(parser, options, configOpt, invalidOptions(Arrays.asList(alterOpt, createOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, partitionsOpt, invalidOptions(Arrays.asList(alterOpt, createOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, replicationFactorOpt, invalidOptions(Arrays.asList(createOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, invalidOptions(Arrays.asList(alterOpt, createOpt)));
            if (options.has(createOpt)) {
                CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, partitionsOpt, replicationFactorOpt);
            }


            CommandLineUtils.checkInvalidArgs(parser, options, reportUnderReplicatedPartitionsOpt,
                invalidOptions(Collections.singleton(topicsWithOverridesOpt), Arrays.asList(describeOpt, reportUnderReplicatedPartitionsOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, reportUnderMinIsrPartitionsOpt,
                invalidOptions(Collections.singleton(topicsWithOverridesOpt), Arrays.asList(describeOpt, reportUnderMinIsrPartitionsOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, reportAtMinIsrPartitionsOpt,
                invalidOptions(Collections.singleton(topicsWithOverridesOpt), Arrays.asList(describeOpt, reportAtMinIsrPartitionsOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, reportUnavailablePartitionsOpt,
                invalidOptions(Collections.singleton(topicsWithOverridesOpt), Arrays.asList(describeOpt, reportUnavailablePartitionsOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, topicsWithOverridesOpt,
                invalidOptions(new HashSet<>(allReplicationReportOpts), Arrays.asList(describeOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, ifExistsOpt,
                invalidOptions(Arrays.asList(alterOpt, deleteOpt, describeOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, ifNotExistsOpt, invalidOptions(Arrays.asList(createOpt)));
            CommandLineUtils.checkInvalidArgs(parser, options, excludeInternalTopicOpt, invalidOptions(Arrays.asList(listOpt, describeOpt)));
        }

        private Set<OptionSpec<?>> invalidOptions(List<OptionSpec<?>> removeOptions) {
            return invalidOptions(new HashSet<>(), removeOptions);
        }

        private LinkedHashSet<OptionSpec<?>> invalidOptions(Set<OptionSpec<?>> addOptions, List<OptionSpec<?>> removeOptions) {
            LinkedHashSet<OptionSpec<?>> finalOptions = new LinkedHashSet<>(allTopicLevelOpts);
            finalOptions.removeAll(removeOptions);
            finalOptions.addAll(addOptions);
            return finalOptions;
        }
    }
}
