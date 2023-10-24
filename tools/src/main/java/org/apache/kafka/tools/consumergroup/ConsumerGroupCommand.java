/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.tools.consumergroup;

import joptsimple.OptionException;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListConsumerGroupsOptions;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    public static final String MISSING_COLUMN_VALUE = "-";

    public static void main(String[] args) {
        ConsumerGroupCommandOptions opts = new ConsumerGroupCommandOptions(args);
        try {
            opts.checkArgs();
            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all consumer groups, describe a consumer group, delete consumer group info, or reset consumer group offsets.");

            // should have exactly one action
            long actions = Stream.of(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).filter(opts.options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets");

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    public static void run(ConsumerGroupCommandOptions opts) {
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
            }
            else if (opts.options.has(opts.deleteOffsetsOpt)) {
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
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " + Utils.join(validStates));
        }
        return parsedStates;
    }

    public static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    public static void printOffsetsToReset(Map<String, Map<TopicPartition, OffsetAndMetadata>> groupAssignmentsToReset) {
        if (!groupAssignmentsToReset.isEmpty())
            System.out.printf("\n%-30s %-30s %-10s %-15s", "GROUP", "TOPIC", "PARTITION", "NEW-OFFSET");

        groupAssignmentsToReset.forEach((groupId, assignment) -> {
            assignment.forEach((consumerAssignment, offsetAndMetadata) -> {
                System.out.printf("%-30s %-30s %-10s %-15s",
                    groupId,
                    consumerAssignment.topic(),
                    consumerAssignment.partition(),
                    offsetAndMetadata.offset());
            });
        });
    }

    static class ConsumerGroupService implements AutoCloseable {
        final ConsumerGroupCommandOptions opts;
        final Map<String, String> configOverrides;
        private final Admin adminClient;

        public ConsumerGroupService(ConsumerGroupCommandOptions opts, Map<String, String> configOverrides) throws IOException {
            this.opts = opts;
            this.configOverrides = configOverrides;
            this.adminClient = createAdminClient(configOverrides);
        }

        Optional<Map<String, Map<TopicPartition, OffsetAndMetadata>>> resetPlanFromFile() throws IOException {
            if (opts.options.has(opts.resetFromFileOpt)) {
                String resetPlanPath = opts.options.valueOf(opts.resetFromFileOpt);
                String resetPlanCsv = Utils.readFileAsString(resetPlanPath);
                Map<String, Map<TopicPartition, OffsetAndMetadata>> resetPlan = parseResetPlan(resetPlanCsv);
                return Optional.of(resetPlan);
            } else return Optional.empty();
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<ConsumerGroupState> states = (stateValue == null || stateValue.isEmpty())
                    ? Collections.emptySet()
                    : consumerGroupStatesFromString(stateValue);
                List<ConsumerGroupListing> listings = listConsumerGroupsWithState(states);
                printGroupStates(listings.stream().map(e -> new Tuple2<>(e.groupId(), e.state().toString())).collect(Collectors.toList()));
            } else
                listConsumerGroups().forEach(System.out::println);
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

        List<ConsumerGroupListing> listConsumerGroupsWithState(Set<ConsumerGroupState> states) throws ExecutionException, InterruptedException {
            ListConsumerGroupsOptions listConsumerGroupsOptions = withTimeoutMs(new ListConsumerGroupsOptions());
            listConsumerGroupsOptions.inStates(states);
            ListConsumerGroupsResult result = adminClient.listConsumerGroups(listConsumerGroupsOptions);
            return new ArrayList<>(result.all().get());
        }

        private void printGroupStates(List<Tuple2<String, String>> groupsAndStates) {
            // find proper columns width
            int maxGroupLen = 15;
            for (Tuple2<String, String> tuple : groupsAndStates) {
                String groupId = tuple.v1;
                maxGroupLen = Math.max(maxGroupLen, groupId.length());
            }
            System.out.printf("%" + (-maxGroupLen) + "s %s", "GROUP", "STATE");
            for (Tuple2<String, String> tuple : groupsAndStates) {
                String groupId = tuple.v1;
                String state = tuple.v2;
                System.out.printf("%" + (-maxGroupLen) + "s %s", groupId, state);
            }
        }

        private boolean shouldPrintMemberState(String group, Optional<String> state, Optional<Integer> numRows) {
            // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
            // if it's undefined or 0, there is no relevant group information to display.
            if (!numRows.isPresent()) {
                printError("The consumer group '" + group + "' does not exist.", Optional.empty());
                return false;
            }

            int num = numRows.get();

            String state0 = state.orElse("NONE");
            switch (state0) {
                case "Dead":
                    printError("Consumer group '" + group + "' does not exist.", Optional.empty());
                    break;
                case "Empty":
                    System.err.println("\nConsumer group '" + group + "' has no active members.");
                    break;
                case "PreparingRebalance":
                case "CompletingRebalance":
                    System.err.println("\nWarning: Consumer group '" + group + "' is rebalancing.");
                    break;
                case "Stable":
                default:
                    // the control should never reach here
                    throw new KafkaException("Expected a valid consumer group state, but found '" + state0 + "'.");
            }

            return !state0.contains("Dead") && num > 0;
        }

        private Optional<Integer> size(Optional<? extends Collection<?>> colOpt) { return colOpt.map(Collection::size); }

        private void printOffsets(Map<String, Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>>> offsets) {
            offsets.forEach((groupId, tuple) -> {
                Optional<String> state = tuple.v1;
                Optional<Collection<PartitionAssignmentState>> assignments = tuple.v2;

                if (shouldPrintMemberState(groupId, state, size(assignments))) {
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

                    String format = "\n%" + (-maxGroupLen) + "s %" + (-maxTopicLen) + "s %-10s %-15s %-15s %-15s %" + (-maxConsumerIdLen) + "s %" + (-maxHostLen) + "s %s";

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

        private void printMembers(Map<String, Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>>> members, boolean verbose) {
            members.forEach((groupId, tuple) -> {
                Optional<String> state = tuple.v1;
                Optional<Collection<MemberAssignmentState>> assignments = tuple.v2;
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
                                partitions = grouped.entrySet().stream().map(e ->
                                    e.getValue().stream().map(TopicPartition::partition).map(Object::toString).sorted().collect(Collectors.joining(",", "(", ")"))
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
                    System.out.printf(format, state.group, coordinator, state.assignmentStrategy, state.state, state.numMembers);
                    System.out.println();
                }
            });
        }

        void describeGroups() {
            Collection<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listConsumerGroups()
                : opts.options.valuesOf(opts.groupOpt);
            boolean membersOptPresent = opts.options.has(opts.membersOpt);
            boolean stateOptPresent = opts.options.has(opts.stateOpt);
            boolean offsetsOptPresent = opts.options.has(opts.offsetsOpt);
            long subActions = Stream.of(membersOptPresent, offsetsOptPresent, stateOptPresent).filter(x -> x).count();

            if (subActions == 0 || offsetsOptPresent) {
                TreeMap<String, Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>>> offsets
                    = collectGroupsOffsets(groupIds);
                printOffsets(offsets);
            } else if (membersOptPresent) {
                TreeMap<String, Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>>> members
                    = collectGroupsMembers(groupIds, opts.options.has(opts.verboseOpt));
                printMembers(members, opts.options.has(opts.verboseOpt));
            } else {
                TreeMap<String, GroupState> states = collectGroupsState(groupIds);
                printStates(states);
            }
        }

        private PartitionAssignmentState[] collectConsumerAssignment(
            String group,
            Optional<Node> coordinator,
            Collection<TopicPartition> topicPartitions,
            Function<TopicPartition, Optional<Long>> getPartitionOffset,
            Optional<String> consumerIdOpt,
            Optional<String> hostOpt,
            Optional<String> clientIdOpt
        ) {
            if (topicPartitions.isEmpty()) {
                return new PartitionAssignmentState[] {
                    new PartitionAssignmentState(group, coordinator, Optional.empty(), Optional.empty(), Optional.empty(),
                        getLag(Optional.empty(), Optional.empty()), consumerIdOpt, hostOpt, clientIdOpt, Optional.empty())
                };
            } else {
                List<TopicPartition> topicPartitionsSorted = topicPartitions.stream().sorted(Comparator.comparingInt(TopicPartition::partition)).collect(Collectors.toList());
                return describePartitions(group, coordinator, topicPartitionsSorted, getPartitionOffset, consumerIdOpt, hostOpt, clientIdOpt);
            }
        }

        private Optional<Long> getLag(Optional<Long> offset, Optional<Long> logEndOffset) {
            return offset.filter(o -> o != -1).flatMap(offset0 -> logEndOffset.map(end -> end - offset0));
        }

        private PartitionAssignmentState[] describePartitions(String group,
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

            return getLogEndOffset(topicPartitions).entrySet().stream().map(logEndOffsetResult -> {
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
            }).collect(Collectors.toList()).toArray(new PartitionAssignmentState[0]);
        }

        Map<String, Map<TopicPartition, OffsetAndMetadata>> resetOffsets() {
            return null;
        }

        Tuple2<Errors, Map<TopicPartition, Throwable>> deleteOffsets(String groupId, List<String> topics) {
            return null;
        }

        void deleteOffsets() {

        }

        Map<String, ConsumerGroupDescription> describeConsumerGroups(Collection<String> groupIds) {
            return null;
        }

        /**
         * Returns the state of the specified consumer group and partition assignment states
         */
        Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>> collectGroupOffsets(String groupId) {
            return null;
        }

        /**
         * Returns states of the specified consumer groups and partition assignment states
         */
        TreeMap<String, Tuple2<Optional<String>, Optional<Collection<PartitionAssignmentState>>>> collectGroupsOffsets(Collection<String> groupIds) {
            return null;
        }

        TreeMap<String, Tuple2<Optional<String>, Optional<Collection<MemberAssignmentState>>>> collectGroupsMembers(Collection<String> groupIds, boolean verbose) {
            return null;
        }

        TreeMap<String, GroupState> collectGroupsState(Collection<String> groupIds) {
            return null;
        }

        private Map<TopicPartition, LogOffsetResult> getLogEndOffset(Collection<TopicPartition> topicPartitions) {
            return null;
        }

        private Map<TopicPartition, LogOffsetResult> getLogStartOffsets(Collection<TopicPartition> topicPartitions) {
            return null;
        }

        private Map<TopicPartition, LogOffsetResult> getLogTimestampOffsets(Collection<TopicPartition> topicPartitions, long timestamp) {
            return null;
        }

        @Override
        public void close() throws Exception {
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

        private Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(String groupId) throws ExecutionException, InterruptedException {
            return adminClient.listConsumerGroupOffsets(
                Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec()),
                withTimeoutMs(new ListConsumerGroupOffsetsOptions())
            ).partitionsToOffsetAndMetadata(groupId).get();
        }

        private Map<String, Map<TopicPartition, OffsetAndMetadata>> parseResetPlan(String resetPlanCsv) {
            return null;
        }

        private Map<TopicPartition, OffsetAndMetadata> prepareOffsetsToReset(String groupId, Collection<TopicPartition> partitionsToReset) {
            return null;
        }

        private Map<TopicPartition, Long> checkOffsetsRange(Map<TopicPartition, Long> requestedOffsets) {
            return null;
        }

        String exportOffsetsToCsv(Map<String, Map<TopicPartition, OffsetAndMetadata>> assignments) {
            return null;
        }

        Map<String, Throwable> deleteGroups() {
            return null;
        }
    }

    interface LogOffsetResult { }

    private static class LogOffset {
        public final long value;

        public LogOffset(long value) {
            this.value = value;
        }
    }

    private static class Unknown implements LogOffsetResult { }

    private class Ignore implements LogOffsetResult { }
}
