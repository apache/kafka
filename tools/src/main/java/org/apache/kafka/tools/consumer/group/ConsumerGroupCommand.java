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
package org.apache.kafka.tools.consumer.group;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import joptsimple.OptionException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.ToolsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConsumerGroupCommand {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupCommand.class);

    static final String MISSING_COLUMN_VALUE = "-";

    public static void main(String[] args) {
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(args);
        try {
            // should have exactly one action
            long actions = Stream.of(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).filter(opts.options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets");

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    static void run(ConsumerGroupCommandOptions opts) {
        try (ConsumerGroupService consumerGroupService = new ConsumerGroupService(opts, Collections.emptyMap())) {
            if (opts.options.has(opts.listOpt))
                consumerGroupService.listGroups();
            else if (opts.options.has(opts.describeOpt))
                consumerGroupService.describeGroups();
            else if (opts.options.has(opts.deleteOpt))
                consumerGroupService.deleteGroups();
            else if (opts.options.has(opts.resetOffsetsOpt)) {
                Map<String, Map<TopicPartition, OffsetAndMetadata>> offsetsToReset = consumerGroupService.resetOffsets();
                if (opts.options.has(opts.exportOpt)) {
                    String exported = consumerGroupService.exportOffsetsToCsv(offsetsToReset);
                    System.out.println(exported);
                } else
                    printOffsetsToReset(offsetsToReset);
            } else if (opts.options.has(opts.deleteOffsetsOpt)) {
                consumerGroupService.deleteOffsets();
            }
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing consumer group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static Set<ConsumerGroupState> consumerGroupStatesFromString(String input) {
        Set<ConsumerGroupState> parsedStates = Arrays.stream(input.split(",")).map(s -> ConsumerGroupState.parse(s.trim())).collect(Collectors.toSet());
        if (parsedStates.contains(ConsumerGroupState.UNKNOWN)) {
            Collection<ConsumerGroupState> validStates = Arrays.stream(ConsumerGroupState.values()).filter(s -> s != ConsumerGroupState.UNKNOWN).collect(Collectors.toList());
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " + validStates.stream().map(ConsumerGroupState::toString).collect(Collectors.joining(", ")));
        }
        return parsedStates;
    }

    @SuppressWarnings("Regexp")
    static Set<GroupType> consumerGroupTypesFromString(String input) {
        Set<GroupType> parsedTypes = Stream.of(input.toLowerCase().split(",")).map(s -> GroupType.parse(s.trim())).collect(Collectors.toSet());
        if (parsedTypes.contains(GroupType.UNKNOWN)) {
            List<String> validTypes = Arrays.stream(GroupType.values()).filter(t -> t != GroupType.UNKNOWN).map(Object::toString).collect(Collectors.toList());
            throw new IllegalArgumentException("Invalid types list '" + input + "'. Valid types are: " + String.join(", ", validTypes));
        }
        return parsedTypes;
    }

    static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    static void printOffsetsToReset(Map<String, Map<TopicPartition, OffsetAndMetadata>> groupAssignmentsToReset) {
        String format = "%-30s %-30s %-10s %-15s";
        if (!groupAssignmentsToReset.isEmpty())
            System.out.printf("\n" + format, "GROUP", "TOPIC", "PARTITION", "NEW-OFFSET");

        groupAssignmentsToReset.forEach((groupId, assignment) ->
            assignment.forEach((consumerAssignment, offsetAndMetadata) ->
                System.out.printf(format,
                    groupId,
                    consumerAssignment.topic(),
                    consumerAssignment.partition(),
                    offsetAndMetadata.offset())));
    }

    @SuppressWarnings("ClassFanOutComplexity")
    static class ConsumerGroupService implements AutoCloseable {
        final ConsumerGroupCommandOptions opts;
        final Map<String, String> configOverrides;
        private final Admin adminClient;

        ConsumerGroupService(ConsumerGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            this.configOverrides = configOverrides;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Optional<Map<String, Map<TopicPartition, OffsetAndMetadata>>> resetPlanFromFile() {
            if (opts.options.has(opts.resetFromFileOpt)) {
                try {
                    String resetPlanPath = opts.options.valueOf(opts.resetFromFileOpt);
                    String resetPlanCsv = Utils.readFileAsString(resetPlanPath);
                    Map<String, Map<TopicPartition, OffsetAndMetadata>> resetPlan = parseResetPlan(resetPlanCsv);
                    return Optional.of(resetPlan);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else return Optional.empty();
        }

        void listGroups() throws ExecutionException, InterruptedException {
            boolean includeType = opts.options.has(opts.typeOpt);
            boolean includeState = opts.options.has(opts.stateOpt);

            if (includeType || includeState) {
                Set<GroupType> types = typeValues();
                Set<ConsumerGroupState> states = stateValues();
                List<ConsumerGroupListing> listings = listConsumerGroupsWithFilters(types, states);

                printGroupInfo(listings, includeType, includeState);
            } else {
                listConsumerGroups().forEach(System.out::println);
            }
        }

        private Set<ConsumerGroupState> stateValues() {
            String stateValue = opts.options.valueOf(opts.stateOpt);
            return (stateValue == null || stateValue.isEmpty())
                ? Collections.emptySet()
                : consumerGroupStatesFromString(stateValue);
        }

        private Set<GroupType> typeValues() {
            String typeValue = opts.options.valueOf(opts.typeOpt);
            return (typeValue == null || typeValue.isEmpty())
                ? Collections.emptySet()
                : consumerGroupTypesFromString(typeValue);
        }

        private void printGroupInfo(List<ConsumerGroupListing> groups, boolean includeType, boolean includeState) {
            Function<ConsumerGroupListing, String> groupId = ConsumerGroupListing::groupId;
            Function<ConsumerGroupListing, String> groupType = groupListing -> groupListing.type().orElse(GroupType.UNKNOWN).toString();
            Function<ConsumerGroupListing, String> groupState = groupListing -> groupListing.state().orElse(ConsumerGroupState.UNKNOWN).toString();

            OptionalInt maybeMax = groups.stream().mapToInt(groupListing -> Math.max(15, groupId.apply(groupListing).length())).max();
            int maxGroupLen = maybeMax.orElse(15) + 10;
            String format = "%-" + maxGroupLen + "s";
            List<String> header = new ArrayList<>();
            header.add("GROUP");
            List<Function<ConsumerGroupListing, String>> extractors = new ArrayList<>();
            extractors.add(groupId);

            if (includeType) {
                header.add("TYPE");
                extractors.add(groupType);
                format += " %-20s";
            }

            if (includeState) {
                header.add("STATE");
                extractors.add(groupState);
                format += " %-20s";
            }

            System.out.printf(format + "%n", header.toArray(new Object[0]));

            for (ConsumerGroupListing groupListing : groups) {
                Object[] info = extractors.stream().map(extractor -> extractor.apply(groupListing)).toArray(Object[]::new);
                System.out.printf(format + "%n", info);
            }
        }

        List<String> listConsumerGroups() {
            try {
                ListConsumerGroupsResult result = adminClient.listConsumerGroups(withTimeoutMs(new ListConsumerGroupsOptions()));
                Collection<ConsumerGroupListing> listings = result.all().get();
                return listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<ConsumerGroupListing> listConsumerGroupsWithFilters(Set<GroupType> types, Set<ConsumerGroupState> states) throws ExecutionException, InterruptedException {
            ListConsumerGroupsOptions listConsumerGroupsOptions = withTimeoutMs(new ListConsumerGroupsOptions());
            listConsumerGroupsOptions
                .inStates(states)
                .withTypes(types);
            ListConsumerGroupsResult result = adminClient.listConsumerGroups(listConsumerGroupsOptions);
            return new ArrayList<>(result.all().get());
        }

        private boolean shouldPrintMemberState(String group, Optional<ConsumerGroupState> state, Optional<Integer> numRows) {
            // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
            // if it's undefined or 0, there is no relevant group information to display.
            if (!numRows.isPresent()) {
                printError("The consumer group '" + group + "' does not exist.", Optional.empty());
                return false;
            }

            int num = numRows.get();

            ConsumerGroupState state0 = state.orElse(ConsumerGroupState.UNKNOWN);
            switch (state0) {
                case DEAD:
                    printError("Consumer group '" + group + "' does not exist.", Optional.empty());
                    break;
                case EMPTY:
                    System.err.println("\nConsumer group '" + group + "' has no active members.");
                    break;
                case PREPARING_REBALANCE:
                case COMPLETING_REBALANCE:
                case ASSIGNING:
                case RECONCILING:
                    System.err.println("\nWarning: Consumer group '" + group + "' is rebalancing.");
                    break;
                case STABLE:
                    break;
                default:
                    // the control should never reach here
                    throw new KafkaException("Expected a valid consumer group state, but found '" + state0 + "'.");
            }

            return !state0.equals(ConsumerGroupState.DEAD) && num > 0;
        }

        private Optional<Integer> size(Optional<? extends Collection<?>> colOpt) {
            return colOpt.map(Collection::size);
        }

        private void printOffsets(Map<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>>> offsets) {
            offsets.forEach((groupId, tuple) -> {
                Optional<ConsumerGroupState> state = tuple.getKey();
                Optional<Collection<PartitionAssignmentState>> assignments = tuple.getValue();

                if (shouldPrintMemberState(groupId, state, size(assignments))) {
                    String format = printOffsetFormat(assignments);

                    System.out.printf(format, "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID");

                    if (assignments.isPresent()) {
                        Collection<PartitionAssignmentState> consumerAssignments = assignments.get();
                        for (PartitionAssignmentState consumerAssignment : consumerAssignments) {
                            System.out.printf(format,
                                consumerAssignment.group,
                                consumerAssignment.topic.orElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                consumerAssignment.offset.map(Object::toString).orElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                                consumerAssignment.lag.map(Object::toString).orElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.orElse(MISSING_COLUMN_VALUE),
                                consumerAssignment.host.orElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.orElse(MISSING_COLUMN_VALUE)
                            );
                        }
                    }
                }
            });
        }

        private static String printOffsetFormat(Optional<Collection<PartitionAssignmentState>> assignments) {
            // find proper columns width
            int maxGroupLen = 15, maxTopicLen = 15, maxConsumerIdLen = 15, maxHostLen = 15;
            if (assignments.isPresent()) {
                Collection<PartitionAssignmentState> consumerAssignments = assignments.get();
                for (PartitionAssignmentState consumerAssignment : consumerAssignments) {
                    maxGroupLen = Math.max(maxGroupLen, consumerAssignment.group.length());
                    maxTopicLen = Math.max(maxTopicLen, consumerAssignment.topic.orElse(MISSING_COLUMN_VALUE).length());
                    maxConsumerIdLen = Math.max(maxConsumerIdLen, consumerAssignment.consumerId.orElse(MISSING_COLUMN_VALUE).length());
                    maxHostLen = Math.max(maxHostLen, consumerAssignment.host.orElse(MISSING_COLUMN_VALUE).length());

                }
            }

            return "\n%" + (-maxGroupLen) + "s %" + (-maxTopicLen) + "s %-10s %-15s %-15s %-15s %" + (-maxConsumerIdLen) + "s %" + (-maxHostLen) + "s %s";
        }

        private void printMembers(Map<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>>> members, boolean verbose) {
            members.forEach((groupId, tuple) -> {
                Optional<ConsumerGroupState> state = tuple.getKey();
                Optional<Collection<MemberAssignmentState>> assignments = tuple.getValue();
                int maxGroupLen = 15, maxConsumerIdLen = 15, maxGroupInstanceIdLen = 17, maxHostLen = 15, maxClientIdLen = 15;
                boolean includeGroupInstanceId = false;

                if (shouldPrintMemberState(groupId, state, size(assignments))) {
                    // find proper columns width
                    if (assignments.isPresent()) {
                        for (MemberAssignmentState memberAssignment : assignments.get()) {
                            maxGroupLen = Math.max(maxGroupLen, memberAssignment.group.length());
                            maxConsumerIdLen = Math.max(maxConsumerIdLen, memberAssignment.consumerId.length());
                            maxGroupInstanceIdLen =  Math.max(maxGroupInstanceIdLen, memberAssignment.groupInstanceId.length());
                            maxHostLen = Math.max(maxHostLen, memberAssignment.host.length());
                            maxClientIdLen = Math.max(maxClientIdLen, memberAssignment.clientId.length());
                            includeGroupInstanceId = includeGroupInstanceId || !memberAssignment.groupInstanceId.isEmpty();
                        }
                    }
                }

                String format0 = "%" + -maxGroupLen + "s %" + -maxConsumerIdLen + "s %" + -maxGroupInstanceIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-15s ";
                String format1 = "%" + -maxGroupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-15s ";

                if (includeGroupInstanceId) {
                    System.out.printf("\n" + format0, "GROUP", "CONSUMER-ID", "GROUP-INSTANCE-ID", "HOST", "CLIENT-ID", "#PARTITIONS");
                } else {
                    System.out.printf("\n" + format1, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS");
                }
                if (verbose)
                    System.out.printf("%s", "ASSIGNMENT");
                System.out.println();

                if (assignments.isPresent()) {
                    for (MemberAssignmentState memberAssignment : assignments.get()) {
                        if (includeGroupInstanceId) {
                            System.out.printf(format0, memberAssignment.group, memberAssignment.consumerId,
                                memberAssignment.groupInstanceId, memberAssignment.host, memberAssignment.clientId,
                                memberAssignment.numPartitions);
                        } else {
                            System.out.printf(format1, memberAssignment.group, memberAssignment.consumerId,
                                memberAssignment.host, memberAssignment.clientId, memberAssignment.numPartitions);
                        }
                        if (verbose) {
                            String partitions;

                            if (memberAssignment.assignment.isEmpty())
                                partitions = MISSING_COLUMN_VALUE;
                            else {
                                Map<String, List<TopicPartition>> grouped = new HashMap<>();
                                memberAssignment.assignment.forEach(
                                    tp -> grouped.computeIfAbsent(tp.topic(), key -> new ArrayList<>()).add(tp));
                                partitions = grouped.values().stream().map(topicPartitions ->
                                    topicPartitions.stream().map(TopicPartition::partition).map(Object::toString).sorted().collect(Collectors.joining(",", "(", ")"))
                                ).sorted().collect(Collectors.joining(", "));
                            }
                            System.out.printf("%s", partitions);
                        }
                        System.out.println();
                    }
                }
            });
        }

        private void printStates(Map<String, GroupState> states) {
            states.forEach((groupId, state) -> {
                if (shouldPrintMemberState(groupId, Optional.of(state.state), Optional.of(1))) {
                    String coordinator = state.coordinator.host() + ":" + state.coordinator.port() + "  (" + state.coordinator.idString() + ")";
                    int coordinatorColLen = Math.max(25, coordinator.length());

                    String format = "\n%" + -coordinatorColLen + "s %-25s %-20s %-15s %s";

                    System.out.printf(format, "GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS");
                    System.out.printf(format, state.group, coordinator, state.assignmentStrategy, state.state.toString(), state.numMembers);
                    System.out.println();
                }
            });
        }

        void describeGroups() throws Exception {
            Collection<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);
            boolean membersOptPresent = opts.options.has(opts.membersOpt);
            boolean stateOptPresent = opts.options.has(opts.stateOpt);
            boolean offsetsOptPresent = opts.options.has(opts.offsetsOpt);
            long subActions = Stream.of(membersOptPresent, offsetsOptPresent, stateOptPresent).filter(x -> x).count();

            if (subActions == 0 || offsetsOptPresent) {
                TreeMap<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>>> offsets
                    = collectGroupsOffsets(groupIds);
                printOffsets(offsets);
            } else if (membersOptPresent) {
                TreeMap<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>>> members
                    = collectGroupsMembers(groupIds, opts.options.has(opts.verboseOpt));
                printMembers(members, opts.options.has(opts.verboseOpt));
            } else {
                TreeMap<String, GroupState> states = collectGroupsState(groupIds);
                printStates(states);
            }
        }

        private Collection<PartitionAssignmentState> collectConsumerAssignment(
            String group,
            Optional<Node> coordinator,
            Collection<TopicPartition> topicPartitions,
            Function<TopicPartition, Optional<Long>> getPartitionOffset,
            Optional<String> consumerIdOpt,
            Optional<String> hostOpt,
            Optional<String> clientIdOpt
        ) {
            if (topicPartitions.isEmpty()) {
                return Collections.singleton(
                    new PartitionAssignmentState(group, coordinator, Optional.empty(), Optional.empty(), Optional.empty(),
                        getLag(Optional.empty(), Optional.empty()), consumerIdOpt, hostOpt, clientIdOpt, Optional.empty())
                );
            } else {
                List<TopicPartition> topicPartitionsSorted = topicPartitions.stream().sorted(Comparator.comparingInt(TopicPartition::partition)).collect(Collectors.toList());
                return describePartitions(group, coordinator, topicPartitionsSorted, getPartitionOffset, consumerIdOpt, hostOpt, clientIdOpt);
            }
        }

        private Optional<Long> getLag(Optional<Long> offset, Optional<Long> logEndOffset) {
            return offset.filter(o -> o != -1).flatMap(offset0 -> logEndOffset.map(end -> end - offset0));
        }

        private Collection<PartitionAssignmentState> describePartitions(String group,
                                                              Optional<Node> coordinator,
                                                              List<TopicPartition> topicPartitions,
                                                              Function<TopicPartition, Optional<Long>> getPartitionOffset,
                                                              Optional<String> consumerIdOpt,
                                                              Optional<String> hostOpt,
                                                              Optional<String> clientIdOpt) {
            BiFunction<TopicPartition, Optional<Long>, PartitionAssignmentState> getDescribePartitionResult = (topicPartition, logEndOffsetOpt) -> {
                Optional<Long> offset = getPartitionOffset.apply(topicPartition);
                return new PartitionAssignmentState(group, coordinator, Optional.of(topicPartition.topic()),
                    Optional.of(topicPartition.partition()), offset, getLag(offset, logEndOffsetOpt),
                    consumerIdOpt, hostOpt, clientIdOpt, logEndOffsetOpt);
            };

            return getLogEndOffsets(topicPartitions).entrySet().stream().map(logEndOffsetResult -> {
                if (logEndOffsetResult.getValue() instanceof LogOffset)
                    return getDescribePartitionResult.apply(
                        logEndOffsetResult.getKey(),
                        Optional.of(((LogOffset) logEndOffsetResult.getValue()).value)
                    );
                else if (logEndOffsetResult.getValue() instanceof Unknown)
                    return getDescribePartitionResult.apply(logEndOffsetResult.getKey(), Optional.empty());
                else if (logEndOffsetResult.getValue() instanceof Ignore)
                    return null;

                throw new IllegalStateException("Unknown LogOffset subclass: " + logEndOffsetResult.getValue());
            }).collect(Collectors.toList());
        }

        Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsets() {
            List<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);

            Map<String, KafkaFuture<ConsumerGroupDescription>> consumerGroups = adminClient.describeConsumerGroups(
                groupIds,
                withTimeoutMs(new DescribeConsumerGroupsOptions())
            ).describedGroups();

            Map<String, Map<TopicPartition, OffsetAndMetadata>> result = new HashMap<>();

            consumerGroups.forEach((groupId, groupDescription) -> {
                try {
                    String state = groupDescription.get().state().toString();
                    switch (state) {
                        case "Empty":
                        case "Dead":
                            Collection<TopicPartition> partitionsToReset = getPartitionsToReset(groupId);
                            Map<TopicPartition, OffsetAndMetadata> preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset);

                            // Dry-run is the default behavior if --execute is not specified
                            boolean dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt);
                            if (!dryRun) {
                                adminClient.alterConsumerGroupOffsets(
                                    groupId,
                                    preparedOffsets,
                                    withTimeoutMs(new AlterConsumerGroupOffsetsOptions())
                                ).all().get();
                            }

                            result.put(groupId, preparedOffsets);

                            break;
                        default:
                            printError("Assignments can only be reset if the group '" + groupId + "' is inactive, but the current state is " + state + ".", Optional.empty());
                            result.put(groupId, Collections.emptyMap());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            return result;
        }

        Entry<Errors, Map<TopicPartition, Throwable>> deleteOffsets(String groupId, List<String> topics) {
            Map<TopicPartition, Throwable> partitionLevelResult = new HashMap<>();
            Set<String> topicWithPartitions = new HashSet<>();
            Set<String> topicWithoutPartitions = new HashSet<>();

            for (String topic : topics) {
                if (topic.contains(":"))
                    topicWithPartitions.add(topic);
                else
                    topicWithoutPartitions.add(topic);
            }

            List<TopicPartition> knownPartitions = topicWithPartitions.stream().flatMap(this::parseTopicsWithPartitions).collect(Collectors.toList());

            // Get the partitions of topics that the user did not explicitly specify the partitions
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(
                topicWithoutPartitions,
                withTimeoutMs(new DescribeTopicsOptions()));

            Iterator<TopicPartition> unknownPartitions = describeTopicsResult.topicNameValues().entrySet().stream().flatMap(e -> {
                String topic = e.getKey();
                try {
                    return e.getValue().get().partitions().stream().map(partition ->
                        new TopicPartition(topic, partition.partition()));
                } catch (ExecutionException | InterruptedException err) {
                    partitionLevelResult.put(new TopicPartition(topic, -1), err);
                    return Stream.empty();
                }
            }).iterator();

            Set<TopicPartition> partitions = new HashSet<>(knownPartitions);

            unknownPartitions.forEachRemaining(partitions::add);

            DeleteConsumerGroupOffsetsResult deleteResult = adminClient.deleteConsumerGroupOffsets(
                groupId,
                partitions,
                withTimeoutMs(new DeleteConsumerGroupOffsetsOptions())
            );

            Errors topLevelException = Errors.NONE;

            try {
                deleteResult.all().get();
            } catch (ExecutionException | InterruptedException e) {
                topLevelException = Errors.forException(e.getCause());
            }

            partitions.forEach(partition -> {
                try {
                    deleteResult.partitionResult(partition).get();
                    partitionLevelResult.put(partition, null);
                } catch (ExecutionException | InterruptedException e) {
                    partitionLevelResult.put(partition, e);
                }
            });

            return new SimpleImmutableEntry<>(topLevelException, partitionLevelResult);
        }

        void deleteOffsets() {
            String groupId = opts.options.valueOf(opts.groupOpt);
            List<String> topics = opts.options.valuesOf(opts.topicOpt);

            Entry<Errors, Map<TopicPartition, Throwable>> res = deleteOffsets(groupId, topics);

            Errors topLevelResult = res.getKey();
            Map<TopicPartition, Throwable> partitionLevelResult = res.getValue();

            switch (topLevelResult) {
                case NONE:
                    System.out.println("Request succeed for deleting offsets with topic " + String.join(", ", topics) + " group " + groupId);
                    break;
                case INVALID_GROUP_ID:
                    printError("'" + groupId + "' is not valid.", Optional.empty());
                    break;
                case GROUP_ID_NOT_FOUND:
                    printError("'" + groupId + "' does not exist.", Optional.empty());
                    break;
                case GROUP_AUTHORIZATION_FAILED:
                    printError("Access to '" + groupId + "' is not authorized.", Optional.empty());
                    break;
                case NON_EMPTY_GROUP:
                    printError("Deleting offsets of a consumer group '" + groupId + "' is forbidden if the group is not empty.", Optional.empty());
                    break;
                case GROUP_SUBSCRIBED_TO_TOPIC:
                case TOPIC_AUTHORIZATION_FAILED:
                case UNKNOWN_TOPIC_OR_PARTITION:
                    printError("Encounter some partition level error, see the follow-up details:", Optional.empty());
                    break;
                default:
                    printError("Encounter some unknown error: " + topLevelResult, Optional.empty());
            }

            String format = "%-30s %-15s %-15s";

            System.out.printf("\n" + format, "TOPIC", "PARTITION", "STATUS");
            partitionLevelResult.entrySet().stream()
                .sorted(Comparator.comparing(e -> e.getKey().topic() + e.getKey().partition()))
                .forEach(e -> {
                    TopicPartition tp = e.getKey();
                    Throwable error = e.getValue();
                    System.out.printf(format,
                        tp.topic(),
                        tp.partition() >= 0 ? tp.partition() : "Not Provided",
                        error != null ? "Error: :" + error.getMessage() : "Successful"
                    );
                });
        }

        Map<String, ConsumerGroupDescription> describeConsumerGroups(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> res = new HashMap<>();
            Map<String, KafkaFuture<ConsumerGroupDescription>> stringKafkaFutureMap = adminClient.describeConsumerGroups(
                groupIds,
                withTimeoutMs(new DescribeConsumerGroupsOptions())
            ).describedGroups();

            for (Entry<String, KafkaFuture<ConsumerGroupDescription>> e : stringKafkaFutureMap.entrySet()) {
                res.put(e.getKey(), e.getValue().get());
            }
            return res;
        }

        /**
         * Returns the state of the specified consumer group and partition assignment states
         */
        Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>> collectGroupOffsets(String groupId) throws Exception {
            return collectGroupsOffsets(Collections.singletonList(groupId)).getOrDefault(groupId, new SimpleImmutableEntry<>(Optional.empty(), Optional.empty()));
        }

        /**
         * Returns states of the specified consumer groups and partition assignment states
         */
        TreeMap<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>>> collectGroupsOffsets(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
            TreeMap<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<PartitionAssignmentState>>>> groupOffsets = new TreeMap<>();

            consumerGroups.forEach((groupId, consumerGroup) -> {
                ConsumerGroupState state = consumerGroup.state();
                Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommittedOffsets(groupId);
                // The admin client returns `null` as a value to indicate that there is not committed offset for a partition.
                Function<TopicPartition, Optional<Long>> getPartitionOffset = tp -> Optional.ofNullable(committedOffsets.get(tp)).map(OffsetAndMetadata::offset);
                List<TopicPartition> assignedTopicPartitions = new ArrayList<>();
                Comparator<MemberDescription> comparator =
                    Comparator.<MemberDescription>comparingInt(m -> m.assignment().topicPartitions().size()).reversed();
                List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
                consumerGroup.members().stream().filter(m -> !m.assignment().topicPartitions().isEmpty())
                    .sorted(comparator)
                    .forEach(consumerSummary -> {
                        Set<TopicPartition> topicPartitions = consumerSummary.assignment().topicPartitions();
                        assignedTopicPartitions.addAll(topicPartitions);
                        rowsWithConsumer.addAll(collectConsumerAssignment(
                            groupId,
                            Optional.of(consumerGroup.coordinator()),
                            topicPartitions,
                            getPartitionOffset,
                            Optional.of(consumerSummary.consumerId()),
                            Optional.of(consumerSummary.host()),
                            Optional.of(consumerSummary.clientId()))
                        );
                    });
                Map<TopicPartition, OffsetAndMetadata> unassignedPartitions = new HashMap<>();
                committedOffsets.entrySet().stream().filter(e -> !assignedTopicPartitions.contains(e.getKey()))
                    .forEach(e -> unassignedPartitions.put(e.getKey(), e.getValue()));
                Collection<PartitionAssignmentState> rowsWithoutConsumer = !unassignedPartitions.isEmpty()
                    ? collectConsumerAssignment(
                        groupId,
                        Optional.of(consumerGroup.coordinator()),
                        unassignedPartitions.keySet(),
                        getPartitionOffset,
                        Optional.of(MISSING_COLUMN_VALUE),
                        Optional.of(MISSING_COLUMN_VALUE),
                        Optional.of(MISSING_COLUMN_VALUE))
                    : Collections.emptyList();

                rowsWithConsumer.addAll(rowsWithoutConsumer);

                groupOffsets.put(groupId, new SimpleImmutableEntry<>(Optional.of(state), Optional.of(rowsWithConsumer)));
            });

            return groupOffsets;
        }

        Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>> collectGroupMembers(String groupId, boolean verbose) throws Exception {
            return collectGroupsMembers(Collections.singleton(groupId), verbose).get(groupId);
        }

        TreeMap<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>>> collectGroupsMembers(Collection<String> groupIds, boolean verbose) throws Exception {
            Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
            TreeMap<String, Entry<Optional<ConsumerGroupState>, Optional<Collection<MemberAssignmentState>>>> res = new TreeMap<>();

            consumerGroups.forEach((groupId, consumerGroup) -> {
                ConsumerGroupState state = consumerGroup.state();
                List<MemberAssignmentState> memberAssignmentStates = consumerGroup.members().stream().map(consumer ->
                    new MemberAssignmentState(
                        groupId,
                        consumer.consumerId(),
                        consumer.host(),
                        consumer.clientId(),
                        consumer.groupInstanceId().orElse(""),
                        consumer.assignment().topicPartitions().size(),
                        new ArrayList<>(verbose ? consumer.assignment().topicPartitions() : Collections.emptySet())
                )).collect(Collectors.toList());
                res.put(groupId, new SimpleImmutableEntry<>(Optional.of(state), Optional.of(memberAssignmentStates)));
            });
            return res;
        }

        GroupState collectGroupState(String groupId) throws Exception {
            return collectGroupsState(Collections.singleton(groupId)).get(groupId);
        }

        TreeMap<String, GroupState> collectGroupsState(Collection<String> groupIds) throws Exception {
            Map<String, ConsumerGroupDescription> consumerGroups = describeConsumerGroups(groupIds);
            TreeMap<String, GroupState> res = new TreeMap<>();
            consumerGroups.forEach((groupId, groupDescription) ->
                res.put(groupId, new GroupState(
                    groupId,
                    groupDescription.coordinator(),
                    groupDescription.partitionAssignor(),
                    groupDescription.state(),
                    groupDescription.members().size()
            )));
            return res;
        }

        private Map<TopicPartition, LogOffsetResult> getLogEndOffsets(Collection<TopicPartition> topicPartitions) {
            return getLogOffsets(topicPartitions, OffsetSpec.latest());
        }

        private Map<TopicPartition, LogOffsetResult> getLogStartOffsets(Collection<TopicPartition> topicPartitions) {
            return getLogOffsets(topicPartitions, OffsetSpec.earliest());
        }

        private Map<TopicPartition, LogOffsetResult> getLogOffsets(Collection<TopicPartition> topicPartitions, OffsetSpec offsetSpec) {
            try {
                Map<TopicPartition, OffsetSpec> startOffsets = topicPartitions.stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> offsetSpec));

                Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(
                    startOffsets,
                    withTimeoutMs(new ListOffsetsOptions())
                ).all().get();

                return topicPartitions.stream().collect(Collectors.toMap(
                    Function.identity(),
                    tp -> offsets.containsKey(tp)
                        ? new LogOffset(offsets.get(tp).offset())
                        : new Unknown()
                ));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        private Map<TopicPartition, LogOffsetResult> getLogTimestampOffsets(Collection<TopicPartition> topicPartitions, long timestamp) {
            try {
                Map<TopicPartition, OffsetSpec> timestampOffsets = topicPartitions.stream()
                    .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.forTimestamp(timestamp)));

                Map<TopicPartition, ListOffsetsResultInfo> offsets = adminClient.listOffsets(
                    timestampOffsets,
                    withTimeoutMs(new ListOffsetsOptions())
                ).all().get();

                Map<TopicPartition, ListOffsetsResultInfo> successfulOffsetsForTimes = new HashMap<>();
                Map<TopicPartition, ListOffsetsResultInfo> unsuccessfulOffsetsForTimes = new HashMap<>();

                offsets.forEach((tp, offsetsResultInfo) -> {
                    if (offsetsResultInfo.offset() != ListOffsetsResponse.UNKNOWN_OFFSET)
                        successfulOffsetsForTimes.put(tp, offsetsResultInfo);
                    else
                        unsuccessfulOffsetsForTimes.put(tp, offsetsResultInfo);
                });

                Map<TopicPartition, LogOffsetResult> successfulLogTimestampOffsets = successfulOffsetsForTimes.entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> new LogOffset(e.getValue().offset())));

                unsuccessfulOffsetsForTimes.forEach((tp, offsetResultInfo) ->
                    System.out.println("\nWarn: Partition " + tp.partition() + " from topic " + tp.topic() +
                    " is empty. Falling back to latest known offset."));

                successfulLogTimestampOffsets.putAll(getLogEndOffsets(unsuccessfulOffsetsForTimes.keySet()));

                return successfulLogTimestampOffsets;
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            adminClient.close();
        }

        // Visibility for testing
        protected Admin createAdminClient(Map<String, String> configOverrides) throws IOException {
            Properties props = opts.options.has(opts.commandConfigOpt) ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) : new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            props.putAll(configOverrides);
            return Admin.create(props);
        }

        private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
            int t = opts.options.valueOf(opts.timeoutMsOpt).intValue();
            return options.timeoutMs(t);
        }

        private Stream<TopicPartition> parseTopicsWithPartitions(String topicArg) {
            ToIntFunction<String> partitionNum = partition -> {
                try {
                    return Integer.parseInt(partition);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid partition '" + partition + "' specified in topic arg '" + topicArg + "''");
                }
            };

            String[] arr = topicArg.split(":");

            if (arr.length != 2)
                throw new IllegalArgumentException("Invalid topic arg '" + topicArg + "', expected topic name and partitions");

            String topic = arr[0];
            String partitions = arr[1];

            return Arrays.stream(partitions.split(",")).
                map(partition -> new TopicPartition(topic, partitionNum.applyAsInt(partition)));
        }

        private List<TopicPartition> parseTopicPartitionsToReset(List<String> topicArgs) throws ExecutionException, InterruptedException {
            List<String> topicsWithPartitions = new ArrayList<>();
            List<String> topics = new ArrayList<>();

            topicArgs.forEach(topicArg -> {
                if (topicArg.contains(":"))
                    topicsWithPartitions.add(topicArg);
                else
                    topics.add(topicArg);
            });

            List<TopicPartition> specifiedPartitions = topicsWithPartitions.stream().flatMap(this::parseTopicsWithPartitions).collect(Collectors.toList());

            List<TopicPartition> unspecifiedPartitions = new ArrayList<>();

            if (!topics.isEmpty()) {
                Map<String, TopicDescription> descriptionMap = adminClient.describeTopics(
                    topics,
                    withTimeoutMs(new DescribeTopicsOptions())
                ).allTopicNames().get();

                descriptionMap.forEach((topic, description) ->
                    description.partitions().forEach(tpInfo -> unspecifiedPartitions.add(new TopicPartition(topic, tpInfo.partition())))
                );
            }

            specifiedPartitions.addAll(unspecifiedPartitions);

            return specifiedPartitions;
        }

        private Collection<TopicPartition> getPartitionsToReset(String groupId) throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.allTopicsOpt)) {
                return getCommittedOffsets(groupId).keySet();
            } else if (opts.options.has(opts.topicOpt)) {
                List<String> topics = opts.options.valuesOf(opts.topicOpt);
                return parseTopicPartitionsToReset(topics);
            } else {
                if (!opts.options.has(opts.resetFromFileOpt))
                    CommandLineUtils.printUsageAndExit(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.");

                return Collections.emptyList();
            }
        }

        private Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(String groupId) {
            try {
                return adminClient.listConsumerGroupOffsets(
                    Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec()),
                    withTimeoutMs(new ListConsumerGroupOffsetsOptions())
                ).partitionsToOffsetAndMetadata(groupId).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        private Map<String, Map<TopicPartition, OffsetAndMetadata>> parseResetPlan(String resetPlanCsv) {
            ObjectReader csvReader = CsvUtils.readerFor(CsvUtils.CsvRecordNoGroup.class);
            String[] lines = resetPlanCsv.split("\n");
            boolean isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1;
            boolean isOldCsvFormat = false;
            try {
                if (lines.length > 0) {
                    csvReader.readValue(lines[0], CsvUtils.CsvRecordNoGroup.class);
                    isOldCsvFormat = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
                // Ignore.
            }

            Map<String, Map<TopicPartition, OffsetAndMetadata>> dataMap = new HashMap<>();

            try {
                // Single group CSV format: "topic,partition,offset"
                if (isSingleGroupQuery && isOldCsvFormat) {
                    String group = opts.options.valueOf(opts.groupOpt);
                    for (String line : lines) {
                        CsvUtils.CsvRecordNoGroup rec = csvReader.readValue(line, CsvUtils.CsvRecordNoGroup.class);
                        dataMap.computeIfAbsent(group, k -> new HashMap<>())
                            .put(new TopicPartition(rec.getTopic(), rec.getPartition()), new OffsetAndMetadata(rec.getOffset()));
                    }
                } else {
                    csvReader = CsvUtils.readerFor(CsvUtils.CsvRecordWithGroup.class);
                    for (String line : lines) {
                        CsvUtils.CsvRecordWithGroup rec = csvReader.readValue(line, CsvUtils.CsvRecordWithGroup.class);
                        dataMap.computeIfAbsent(rec.getGroup(), k -> new HashMap<>())
                            .put(new TopicPartition(rec.getTopic(), rec.getPartition()), new OffsetAndMetadata(rec.getOffset()));
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return dataMap;
        }

        @SuppressWarnings("CyclomaticComplexity")
        private Map<TopicPartition, OffsetAndMetadata> prepareOffsetsToReset(String groupId, Collection<TopicPartition> partitionsToReset) {
            if (opts.options.has(opts.resetToOffsetOpt)) {
                long offset = opts.options.valueOf(opts.resetToOffsetOpt);
                return checkOffsetsRange(partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), tp -> offset)))
                    .entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
            } else if (opts.options.has(opts.resetToEarliestOpt)) {
                Map<TopicPartition, LogOffsetResult> logStartOffsets = getLogStartOffsets(partitionsToReset);
                return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
                    LogOffsetResult logOffsetResult = logStartOffsets.get(topicPartition);

                    if (!(logOffsetResult instanceof LogOffset)) {
                        ToolsUtils.printUsageAndExit(opts.parser, "Error getting starting offset of topic partition: " + topicPartition);
                        return null;
                    }

                    return new OffsetAndMetadata(((LogOffset) logOffsetResult).value);
                }));
            } else if (opts.options.has(opts.resetToLatestOpt)) {
                Map<TopicPartition, LogOffsetResult> logEndOffsets = getLogEndOffsets(partitionsToReset);
                return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
                    LogOffsetResult logOffsetResult = logEndOffsets.get(topicPartition);

                    if (!(logOffsetResult instanceof LogOffset)) {
                        ToolsUtils.printUsageAndExit(opts.parser, "Error getting ending offset of topic partition: " + topicPartition);
                        return null;
                    }

                    return new OffsetAndMetadata(((LogOffset) logOffsetResult).value);
                }));
            } else if (opts.options.has(opts.resetShiftByOpt)) {
                Map<TopicPartition, OffsetAndMetadata> currentCommittedOffsets = getCommittedOffsets(groupId);
                Map<TopicPartition, Long> requestedOffsets = partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
                    long shiftBy = opts.options.valueOf(opts.resetShiftByOpt);
                    OffsetAndMetadata currentOffset = currentCommittedOffsets.get(topicPartition);

                    if (currentOffset == null) {
                        throw new IllegalArgumentException("Cannot shift offset for partition " + topicPartition + " since there is no current committed offset");
                    }

                    return currentOffset.offset() + shiftBy;
                }));
                return checkOffsetsRange(requestedOffsets).entrySet().stream()
                    .collect(Collectors.toMap(Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
            } else if (opts.options.has(opts.resetToDatetimeOpt)) {
                try {
                    long timestamp = Utils.getDateTime(opts.options.valueOf(opts.resetToDatetimeOpt));
                    Map<TopicPartition, LogOffsetResult> logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp);
                    return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
                        LogOffsetResult logTimestampOffset = logTimestampOffsets.get(topicPartition);

                        if (!(logTimestampOffset instanceof LogOffset)) {
                            ToolsUtils.printUsageAndExit(opts.parser, "Error getting offset by timestamp of topic partition: " + topicPartition);
                            return null;
                        }

                        return new OffsetAndMetadata(((LogOffset) logTimestampOffset).value);
                    }));
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            } else if (opts.options.has(opts.resetByDurationOpt)) {
                String duration = opts.options.valueOf(opts.resetByDurationOpt);
                Duration durationParsed = Duration.parse(duration);
                Instant now = Instant.now();
                durationParsed.negated().addTo(now);
                long timestamp = now.minus(durationParsed).toEpochMilli();
                Map<TopicPartition, LogOffsetResult> logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp);
                return partitionsToReset.stream().collect(Collectors.toMap(Function.identity(), topicPartition -> {
                    LogOffsetResult logTimestampOffset = logTimestampOffsets.get(topicPartition);

                    if (!(logTimestampOffset instanceof LogOffset)) {
                        ToolsUtils.printUsageAndExit(opts.parser, "Error getting offset by timestamp of topic partition: " + topicPartition);
                        return null;
                    }

                    return new OffsetAndMetadata(((LogOffset) logTimestampOffset).value);
                }));
            } else if (resetPlanFromFile().isPresent()) {
                return resetPlanFromFile().map(resetPlan -> {
                    Map<TopicPartition, OffsetAndMetadata> resetPlanForGroup = resetPlan.get(groupId);

                    if (resetPlanForGroup == null) {
                        printError("No reset plan for group " + groupId + " found", Optional.empty());
                        return Collections.<TopicPartition, OffsetAndMetadata>emptyMap();
                    }

                    Map<TopicPartition, Long> requestedOffsets = resetPlanForGroup.keySet().stream().collect(Collectors.toMap(
                        Function.identity(),
                        topicPartition -> resetPlanForGroup.get(topicPartition).offset()));

                    return checkOffsetsRange(requestedOffsets).entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey, e -> new OffsetAndMetadata(e.getValue())));
                }).orElseGet(Collections::emptyMap);
            } else if (opts.options.has(opts.resetToCurrentOpt)) {
                Map<TopicPartition, OffsetAndMetadata> currentCommittedOffsets = getCommittedOffsets(groupId);
                Collection<TopicPartition> partitionsToResetWithCommittedOffset = new ArrayList<>();
                Collection<TopicPartition> partitionsToResetWithoutCommittedOffset = new ArrayList<>();

                for (TopicPartition topicPartition : partitionsToReset) {
                    if (currentCommittedOffsets.containsKey(topicPartition))
                        partitionsToResetWithCommittedOffset.add(topicPartition);
                    else
                        partitionsToResetWithoutCommittedOffset.add(topicPartition);
                }

                Map<TopicPartition, OffsetAndMetadata> preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.stream()
                    .collect(Collectors.toMap(Function.identity(), topicPartition -> {
                        OffsetAndMetadata committedOffset = currentCommittedOffsets.get(topicPartition);

                        if (committedOffset == null) {
                            throw new IllegalStateException("Expected a valid current offset for topic partition: " + topicPartition);
                        }

                        return new OffsetAndMetadata(committedOffset.offset());
                    }));

                Map<TopicPartition, OffsetAndMetadata> preparedOffsetsForPartitionsWithoutCommittedOffset = getLogEndOffsets(partitionsToResetWithoutCommittedOffset)
                    .entrySet().stream().collect(Collectors.toMap(Entry::getKey, e -> {
                        if (!(e.getValue() instanceof LogOffset)) {
                            ToolsUtils.printUsageAndExit(opts.parser, "Error getting ending offset of topic partition: " + e.getKey());
                            return null;
                        }

                        return new OffsetAndMetadata(((LogOffset) e.getValue()).value);
                    }));

                preparedOffsetsForPartitionsWithCommittedOffset.putAll(preparedOffsetsForPartitionsWithoutCommittedOffset);

                return preparedOffsetsForPartitionsWithCommittedOffset;
            }

            ToolsUtils.printUsageAndExit(opts.parser, String.format("Option '%s' requires one of the following scenarios: %s", opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts));
            return null;
        }

        private Map<TopicPartition, Long> checkOffsetsRange(Map<TopicPartition, Long> requestedOffsets) {
            Map<TopicPartition, LogOffsetResult> logStartOffsets = getLogStartOffsets(requestedOffsets.keySet());
            Map<TopicPartition, LogOffsetResult> logEndOffsets = getLogEndOffsets(requestedOffsets.keySet());

            Map<TopicPartition, Long> res = new HashMap<>();

            requestedOffsets.forEach((topicPartition, offset) -> {
                LogOffsetResult logEndOffset = logEndOffsets.get(topicPartition);

                if (logEndOffset != null) {
                    if (logEndOffset instanceof LogOffset && offset > ((LogOffset) logEndOffset).value) {
                        long endOffset = ((LogOffset) logEndOffset).value;
                        LOGGER.warn("New offset (" + offset + ") is higher than latest offset for topic partition " + topicPartition + ". Value will be set to " + endOffset);
                        res.put(topicPartition, endOffset);
                    } else {
                        LogOffsetResult logStartOffset = logStartOffsets.get(topicPartition);

                        if (logStartOffset instanceof LogOffset && offset < ((LogOffset) logStartOffset).value) {
                            long startOffset = ((LogOffset) logStartOffset).value;
                            LOGGER.warn("New offset (" + offset + ") is lower than earliest offset for topic partition " + topicPartition + ". Value will be set to " + startOffset);
                            res.put(topicPartition, startOffset);
                        } else
                            res.put(topicPartition, offset);
                    }
                } else {
                    // the control should not reach here
                    throw new IllegalStateException("Unexpected non-existing offset value for topic partition " + topicPartition);
                }
            });

            return res;
        }

        String exportOffsetsToCsv(Map<String, Map<TopicPartition, OffsetAndMetadata>> assignments) {
            boolean isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1;
            ObjectWriter csvWriter = isSingleGroupQuery
                ? CsvUtils.writerFor(CsvUtils.CsvRecordNoGroup.class)
                : CsvUtils.writerFor(CsvUtils.CsvRecordWithGroup.class);

            return assignments.entrySet().stream().flatMap(e -> {
                String groupId = e.getKey();
                Map<TopicPartition, OffsetAndMetadata> partitionInfo = e.getValue();

                return partitionInfo.entrySet().stream().map(e1 -> {
                    TopicPartition k = e1.getKey();
                    OffsetAndMetadata v = e1.getValue();
                    Object csvRecord = isSingleGroupQuery
                        ? new CsvUtils.CsvRecordNoGroup(k.topic(), k.partition(), v.offset())
                        : new CsvUtils.CsvRecordWithGroup(groupId, k.topic(), k.partition(), v.offset());

                    try {
                        return csvWriter.writeValueAsString(csvRecord);
                    } catch (JsonProcessingException err) {
                        throw new RuntimeException(err);
                    }
                });
            }).collect(Collectors.joining());
        }

        Map<String, Throwable> deleteGroups() {
            List<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);

            Map<String, KafkaFuture<Void>> groupsToDelete = adminClient.deleteConsumerGroups(
                groupIds,
                withTimeoutMs(new DeleteConsumerGroupsOptions())
            ).deletedGroups();

            Map<String, Throwable> success = new HashMap<>();
            Map<String, Throwable> failed = new HashMap<>();

            groupsToDelete.forEach((g, f) -> {
                try {
                    f.get();
                    success.put(g, null);
                } catch (ExecutionException | InterruptedException e) {
                    failed.put(g, e);
                }
            });

            if (failed.isEmpty())
                System.out.println("Deletion of requested consumer groups (" + "'" + success.keySet().stream().map(Object::toString).collect(Collectors.joining(", ")) + "'" + ") was successful.");
            else {
                printError("Deletion of some consumer groups failed:", Optional.empty());
                failed.forEach((group, error) -> System.out.println("* Group '" + group + "' could not be deleted due to: " + error));

                if (!success.isEmpty())
                    System.out.println("\nThese consumer groups were deleted successfully: " + "'" + success.keySet().stream().map(Object::toString).collect(Collectors.joining("'")) + "', '");
            }

            failed.putAll(success);

            return failed;
        }
    }

    interface LogOffsetResult { }

    private static class LogOffset implements LogOffsetResult {
        final long value;

        LogOffset(long value) {
            this.value = value;
        }
    }

    private static class Unknown implements LogOffsetResult { }

    private static class Ignore implements LogOffsetResult { }
}
