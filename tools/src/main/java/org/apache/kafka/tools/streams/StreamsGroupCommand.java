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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteStreamsGroupsOptions;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionException;

public class StreamsGroupCommand {

    public static void main(String[] args) {
        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(args);
        try {
            opts.checkArgs();

            // should have exactly one action
            long numberOfActions = Stream.of(opts.listOpt, opts.describeOpt, opts.deleteOpt).filter(opts.options::has).count();
            if (numberOfActions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, or --delete.");

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    public static void run(StreamsGroupCommandOptions opts) {
        try (StreamsGroupService streamsGroupService = new StreamsGroupService(opts, Map.of())) {
            if (opts.options.has(opts.listOpt)) {
                streamsGroupService.listGroups();
            } else if (opts.options.has(opts.describeOpt)) {
                streamsGroupService.describeGroups();
            } else if (opts.options.has(opts.deleteOpt)) {
                streamsGroupService.deleteGroups();
            } else {
                throw new IllegalArgumentException("Unknown action!");
            }
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing streams group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static Set<GroupState> groupStatesFromString(String input) {
        Set<GroupState> parsedStates =
            Arrays.stream(input.split(",")).map(s -> GroupState.parse(s.trim())).collect(Collectors.toSet());
        Set<GroupState> validStates = GroupState.groupStatesForType(GroupType.STREAMS);
        if (!validStates.containsAll(parsedStates)) {
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " +
                validStates.stream().map(GroupState::toString).collect(Collectors.joining(", ")));
        }
        return parsedStates;
    }

    public static void printError(String msg, Optional<Throwable> e) {
        System.out.println("\nError: " + msg);
        e.ifPresent(Throwable::printStackTrace);
    }

    // Visibility for testing
    static class StreamsGroupService implements AutoCloseable {
        final StreamsGroupCommandOptions opts;
        private final Admin adminClient;

        public StreamsGroupService(StreamsGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public StreamsGroupService(StreamsGroupCommandOptions opts, Admin adminClient) {
            this.opts = opts;
            this.adminClient = adminClient;
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<GroupState> states = (stateValue == null || stateValue.isEmpty())
                    ? Set.of()
                    : groupStatesFromString(stateValue);
                List<GroupListing> listings = listStreamsGroupsInStates(states);
                printGroupInfo(listings);
            } else
                listStreamsGroups().forEach(System.out::println);
        }

        List<String> listStreamsGroups() {
            try {
                ListGroupsResult result = adminClient.listGroups(ListGroupsOptions.forStreamsGroups()
                    .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue()));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().map(GroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<GroupListing> listStreamsGroupsInStates(Set<GroupState> states) throws ExecutionException, InterruptedException {
            ListGroupsResult result = adminClient.listGroups(ListGroupsOptions.forStreamsGroups()
                .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
                .inGroupStates(states));
            return new ArrayList<>(result.all().get());
        }

        private void printGroupInfo(List<GroupListing> groups) {
            // find proper columns width
            int maxGroupLen = 15;
            for (GroupListing group : groups) {
                maxGroupLen = Math.max(maxGroupLen, group.groupId().length());
            }
            System.out.printf("%" + (-maxGroupLen) + "s %s\n", "GROUP", "STATE");
            for (GroupListing group : groups) {
                String groupId = group.groupId();
                String state = group.groupState().orElse(GroupState.UNKNOWN).toString();
                System.out.printf("%" + (-maxGroupLen) + "s %s\n", groupId, state);
            }
        }

        public void describeGroups() throws ExecutionException, InterruptedException {
            List<String> groups = listStreamsGroups();
            if (!groups.isEmpty()) {
                StreamsGroupDescription description = getDescribeGroup(groups.get(0));
                if (description == null)
                    return;
                boolean verbose = opts.options.has(opts.verboseOpt);
                if (opts.options.has(opts.membersOpt)) {
                    printMembers(description, verbose);
                } else if (opts.options.has(opts.stateOpt)) {
                    printStates(description, verbose);
                } else {
                    printOffsets(description, verbose);
                }
            }
        }

        StreamsGroupDescription getDescribeGroup(String group) throws ExecutionException, InterruptedException {
            DescribeStreamsGroupsResult result = adminClient.describeStreamsGroups(List.of(group));
            Map<String, StreamsGroupDescription> descriptionMap = result.all().get();
            return descriptionMap.get(group);
        }

        private void printMembers(StreamsGroupDescription description, boolean verbose) {
            final int groupLen = Math.max(15, description.groupId().length());
            int maxMemberIdLen = 15, maxHostLen = 15, maxClientIdLen = 15;
            Collection<StreamsGroupMemberDescription> members = description.members();
            if (isGroupStateValid(description.groupState(), description.members().size())) {
                maybePrintEmptyGroupState(description.groupId(), description.groupState());
                for (StreamsGroupMemberDescription member : members) {
                    maxMemberIdLen = Math.max(maxMemberIdLen, member.memberId().length());
                    maxHostLen = Math.max(maxHostLen, member.processId().length());
                    maxClientIdLen = Math.max(maxClientIdLen, member.clientId().length());
                }

                if (!verbose) {
                    String fmt = "%" + -groupLen + "s %" + -maxMemberIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %s\n";
                    System.out.printf(fmt, "GROUP", "MEMBER", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
                    for (StreamsGroupMemberDescription member : members) {
                        System.out.printf(fmt, description.groupId(), member.memberId(), member.processId(), member.clientId(),
                            getTasksForPrinting(member.assignment(), Optional.empty()));
                    }
                } else {
                    final int targetAssignmentEpochLen = 25, topologyEpochLen = 15, memberProtocolLen = 15, memberEpochLen = 15;
                    String fmt = "%" + -groupLen + "s %" + -targetAssignmentEpochLen + "s %" + -topologyEpochLen + "s%" + -maxMemberIdLen
                        + "s %" + -memberProtocolLen + "s %" + -memberEpochLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %s\n";
                    System.out.printf(fmt, "GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID", "ASSIGNMENTS");
                    for (StreamsGroupMemberDescription member : members) {
                        System.out.printf(fmt, description.groupId(), description.targetAssignmentEpoch(), description.topologyEpoch(), member.memberId(),
                            member.isClassic() ? "classic" : "streams", member.memberEpoch(), member.processId(), member.clientId(), getTasksForPrinting(member.assignment(), Optional.of(member.targetAssignment())));
                    }
                }
            }
        }

        private String prepareTaskType(List<StreamsGroupMemberAssignment.TaskIds> tasks, String taskType) {
            if (tasks.isEmpty()) {
                return "";
            }
            StringBuilder builder = new StringBuilder(taskType).append(": ");
            for (StreamsGroupMemberAssignment.TaskIds taskIds : tasks) {
                builder.append(taskIds.subtopologyId()).append(":[");
                builder.append(taskIds.partitions().stream().map(String::valueOf).collect(Collectors.joining(",")));
                builder.append("]; ");
            }
            return builder.toString();
        }

        private String getTasksForPrinting(StreamsGroupMemberAssignment assignment, Optional<StreamsGroupMemberAssignment> targetAssignment) {
            StringBuilder builder = new StringBuilder();
            builder.append(prepareTaskType(assignment.activeTasks(), "ACTIVE"))
                .append(prepareTaskType(assignment.standbyTasks(), "STANDBY"))
                .append(prepareTaskType(assignment.warmupTasks(), "WARMUP"));
            targetAssignment.ifPresent(target -> builder.append(prepareTaskType(target.activeTasks(), "TARGET-ACTIVE"))
                .append(prepareTaskType(target.standbyTasks(), "TARGET-STANDBY"))
                .append(prepareTaskType(target.warmupTasks(), "TARGET-WARMUP")));
            return builder.toString();
        }

        private void printStates(StreamsGroupDescription description, boolean verbose) {
            maybePrintEmptyGroupState(description.groupId(), description.groupState());

            final int groupLen = Math.max(15, description.groupId().length());
            String coordinator = description.coordinator().host() + ":" + description.coordinator().port() + " (" + description.coordinator().idString() + ")";

            final int coordinatorLen = Math.max(25, coordinator.length());
            final int stateLen = 25;
            if (!verbose) {
                String fmt = "%" + -groupLen + "s %" + -coordinatorLen + "s %" + -stateLen + "s %s\n";
                System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "#MEMBERS");
                System.out.printf(fmt, description.groupId(), coordinator, description.groupState().toString(), description.members().size());
            } else {
                final int groupEpochLen = 15, targetAssignmentEpochLen = 25;
                String fmt = "%" + -groupLen + "s %" + -coordinatorLen + "s %" + -stateLen + "s %" + -groupEpochLen + "s %" + -targetAssignmentEpochLen + "s %s\n";
                System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
                System.out.printf(fmt, description.groupId(), coordinator, description.groupState().toString(), description.groupEpoch(), description.targetAssignmentEpoch(), description.members().size());
            }
        }

        private void printOffsets(StreamsGroupDescription description, boolean verbose) throws ExecutionException, InterruptedException {
            Map<TopicPartition, OffsetsInfo> offsets = getOffsets(description);
            if (isGroupStateValid(description.groupState(), description.members().size())) {
                maybePrintEmptyGroupState(description.groupId(), description.groupState());
                final int groupLen = Math.max(15, description.groupId().length());
                int maxTopicLen = 15;
                for (TopicPartition topicPartition : offsets.keySet()) {
                    maxTopicLen = Math.max(maxTopicLen, topicPartition.topic().length());
                }
                final int maxPartitionLen = 10;
                if (!verbose) {
                    String fmt = "%" + -groupLen + "s %" + -maxTopicLen + "s %" + -maxPartitionLen + "s %s\n";
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "OFFSET-LAG");
                    for (Map.Entry<TopicPartition, OffsetsInfo> offset : offsets.entrySet()) {
                        System.out.printf(fmt, description.groupId(), offset.getKey().topic(), offset.getKey().partition(), offset.getValue().lag);
                    }
                } else {
                    String fmt = "%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %-15s %-15s %-15s %-15s%n";
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LEADER-EPOCH", "LOG-END-OFFSET", "OFFSET-LAG");
                    for (Map.Entry<TopicPartition, OffsetsInfo> offset : offsets.entrySet()) {
                        System.out.printf(fmt, description.groupId(), offset.getKey().topic(), offset.getKey().partition(),
                            offset.getValue().currentOffset.map(Object::toString).orElse("-"), offset.getValue().leaderEpoch.map(Object::toString).orElse("-"),
                            offset.getValue().logEndOffset, offset.getValue().lag);
                    }
                }
            }
        }

        Map<TopicPartition, OffsetsInfo> getOffsets(StreamsGroupDescription description) throws ExecutionException, InterruptedException {
            final Collection<StreamsGroupMemberDescription> members = description.members();
            Set<TopicPartition> allTp = new HashSet<>();
            for (StreamsGroupMemberDescription memberDescription : members) {
                allTp.addAll(getTopicPartitions(memberDescription.assignment().activeTasks(), description));
            }
            // fetch latest and earliest offsets
            Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();

            for (TopicPartition tp : allTp) {
                earliest.put(tp, OffsetSpec.earliest());
                latest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestResult = adminClient.listOffsets(earliest).all().get();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestResult = adminClient.listOffsets(latest).all().get();
            Map<TopicPartition, OffsetAndMetadata> committedOffsets = getCommittedOffsets(description.groupId());

            Map<TopicPartition, OffsetsInfo> output = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> tp : earliestResult.entrySet()) {
                final Optional<Long> currentOffset = committedOffsets.containsKey(tp.getKey()) ? Optional.of(committedOffsets.get(tp.getKey()).offset()) : Optional.empty();
                final Optional<Integer> leaderEpoch = committedOffsets.containsKey(tp.getKey()) ? committedOffsets.get(tp.getKey()).leaderEpoch() : Optional.empty();
                final long lag = currentOffset.map(current -> latestResult.get(tp.getKey()).offset() - current).orElseGet(() -> latestResult.get(tp.getKey()).offset() - earliestResult.get(tp.getKey()).offset());
                output.put(tp.getKey(),
                    new OffsetsInfo(
                        currentOffset,
                        leaderEpoch,
                        latestResult.get(tp.getKey()).offset(),
                        lag));
            }
            return output;
        }

        Map<TopicPartition, OffsetAndMetadata> getCommittedOffsets(String groupId) {
            try {
                return adminClient.listConsumerGroupOffsets(
                    Map.of(groupId, new ListConsumerGroupOffsetsSpec())).partitionsToOffsetAndMetadata(groupId).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Prints an error message if the group state indicates that the group is either dead or empty.
         *
         * @param group The ID of the group being checked.
         * @param state The current state of the group, represented as a `GroupState` object.
         *              Possible values include `DEAD` (indicating the group does not exist)
         *              and `EMPTY` (indicating the group has no active members).
         */
        private static void maybePrintEmptyGroupState(String group, GroupState state) {
            if (state == GroupState.DEAD) {
                printError("Streams group '" + group + "' does not exist.", Optional.empty());
            } else if (state == GroupState.EMPTY) {
                printError("Streams group '" + group + "' has no active members.", Optional.empty());
            }
        }

        /**
         * Checks if the group state is valid based on its state and the number of rows.
         *
         * @param state   The current state of the group, represented as a `GroupState` object.
         * @param numRows The number of rows associated with the group.
         * @return `true` if the group state is not `DEAD` and the number of rows is greater than 0; otherwise, `false`.
         */
        // Visibility for testing
        static boolean isGroupStateValid(GroupState state, int numRows) {
            return !state.equals(GroupState.DEAD) && numRows > 0;
        }

        private static Set<TopicPartition> getTopicPartitions(List<StreamsGroupMemberAssignment.TaskIds> taskIds, StreamsGroupDescription description) {
            Map<String, List<String>> allSourceTopics = new HashMap<>();
            for (StreamsGroupSubtopologyDescription subtopologyDescription : description.subtopologies()) {
                List<String> topics = new ArrayList<>(subtopologyDescription.sourceTopics());
                topics.addAll(subtopologyDescription.repartitionSourceTopics().keySet());
                allSourceTopics.put(subtopologyDescription.subtopologyId(), topics);
            }
            Set<TopicPartition> topicPartitions = new HashSet<>();

            for (StreamsGroupMemberAssignment.TaskIds task : taskIds) {
                List<String> sourceTopics = allSourceTopics.get(task.subtopologyId());
                if (sourceTopics == null) {
                    throw new IllegalArgumentException("Subtopology " + task.subtopologyId() + " not found in group description!");
                }
                for (String topic : sourceTopics) {
                    for (Integer partition : task.partitions()) {
                        topicPartitions.add(new TopicPartition(topic, partition));
                    }
                }
            }
            return topicPartitions;
        }

        public void close() {
            adminClient.close();
        }

        protected Admin createAdminClient(Map<String, String> configOverrides) throws IOException {
            Properties props = opts.options.has(opts.commandConfigOpt) ? Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) : new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
            props.putAll(configOverrides);
            return Admin.create(props);
        }

        Map<String, Throwable> deleteGroups() {
            List<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? new ArrayList<>(listStreamsGroups())
                : new ArrayList<>(opts.options.valuesOf(opts.groupOpt));

            // pre admin call checks
            Map<String, Throwable> failed = preAdminCallChecks(groupIds);

            groupIds.removeAll(failed.keySet());
            Map<String, Throwable> success = new HashMap<>();
            Map<String, List<String>> internalTopics = new HashMap<>();
            Map<String, Throwable> internalTopicsDeletionFailures = new HashMap<>();
            if (!groupIds.isEmpty()) {
                // retrieve internal topics before deleting groups
                internalTopics = retrieveInternalTopics(groupIds);

                // delete streams groups
                Map<String, KafkaFuture<Void>> groupsToDelete = adminClient.deleteStreamsGroups(
                    groupIds,
                    withTimeoutMs(new DeleteStreamsGroupsOptions())
                ).deletedGroups();

                groupsToDelete.forEach((g, f) -> {
                    try {
                        f.get();
                        success.put(g, null);
                    } catch (InterruptedException ie) {
                        failed.put(g, ie);
                    } catch (ExecutionException e) {
                        failed.put(g, e.getCause());
                    }
                });

                // delete internal topics
                if (!success.isEmpty()) {
                    for (String groupId : success.keySet()) {
                        List<String> internalTopicsToDelete = internalTopics.get(groupId);
                        if (internalTopicsToDelete != null && !internalTopicsToDelete.isEmpty()) {
                            DeleteTopicsResult deleteTopicsResult = null;
                            try {
                                deleteTopicsResult = adminClient.deleteTopics(internalTopicsToDelete);
                                deleteTopicsResult.all().get();
                            } catch (InterruptedException | ExecutionException e) {
                                if (deleteTopicsResult != null) {
                                    deleteTopicsResult.topicNameValues().forEach((topic, future) -> {
                                        try {
                                            future.get();
                                        } catch (Exception topicException) {
                                            System.out.println("Failed to delete internal topic: " + topic);
                                        }
                                    });
                                }
                                internalTopicsDeletionFailures.put(groupId, e.getCause());
                            }
                        }
                    }
                }
            }

            // display outcome messages based on the results
            if (failed.isEmpty()) {
                System.out.println("Deletion of requested streams groups (" + "'" + success.keySet().stream().map(Object::toString).collect(Collectors.joining("', '")) + "') was successful.");
            } else {
                printError("Deletion of some streams groups failed:", Optional.empty());
                failed.forEach((group, error) -> System.out.println("* Group '" + group + "' could not be deleted due to: " + error));

                if (!success.isEmpty()) {
                    System.out.println("\nThese streams groups were deleted successfully: " + "'" + success.keySet().stream().map(Object::toString).collect(Collectors.joining("', '")) + "'.");
                }
            }
            if (!internalTopics.keySet().isEmpty()) {
                printInternalTopicErrors(internalTopicsDeletionFailures, success.keySet(), internalTopics.keySet());
            }
            // for testing purpose: return all failures, including internal topics deletion failures
            failed.putAll(success);
            failed.putAll(internalTopicsDeletionFailures);
            return failed;
        }

        private Map<String, Throwable> preAdminCallChecks(List<String> groupIds) {
            List<GroupListing> streamsGroupIds = listDetailedStreamsGroups();
            LinkedHashSet<String> groupIdSet = new LinkedHashSet<>(groupIds);

            Map<String, Throwable> failed = new HashMap<>();

            for (String groupId : groupIdSet) {
                Optional<GroupListing> listing = streamsGroupIds.stream().filter(item -> item.groupId().equals(groupId)).findAny();
                if (listing.isEmpty()) {
                    failed.put(groupId, new IllegalArgumentException("Group '" + groupId + "' does not exist or is not a streams group."));
                } else {
                    Optional<GroupState> groupState = listing.get().groupState();
                    groupState.ifPresent(state -> {
                        if (state == GroupState.DEAD) {
                            failed.put(groupId, new IllegalStateException("Streams group '" + groupId + "' group state is DEAD."));
                        } else if (state != GroupState.EMPTY) {
                            failed.put(groupId, new GroupNotEmptyException("Streams group '" + groupId + "' is not EMPTY."));
                        }
                    });
                }
            }
            return failed;
        }

        List<GroupListing> listDetailedStreamsGroups() {
            try {
                ListGroupsResult result = adminClient.listGroups(new ListGroupsOptions()
                    .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
                    .withTypes(Set.of(GroupType.STREAMS)));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().toList();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        private void printInternalTopicErrors(Map<String, Throwable> internalTopicsDeletionFailures,
                                              Set<String> deletedGroupIds,
                                              Set<String> groupIdsWithInternalTopics) {
            if (!deletedGroupIds.isEmpty()) {
                if (internalTopicsDeletionFailures.isEmpty()) {
                    List<String> successfulGroups = deletedGroupIds.stream()
                        .filter(groupIdsWithInternalTopics::contains)
                        .collect(Collectors.toList());
                    System.out.println("Deletion of associated internal topics of the streams groups ('" +
                        String.join("', '", successfulGroups) + "') was successful.");
                } else {
                    System.out.println("Deletion of some associated internal topics failed:");
                    internalTopicsDeletionFailures.forEach((group, error) ->
                        System.out.println("* Internal topics of the streams group '" + group + "' could not be deleted due to: " + error));
                }
            }
        }

        // Visibility for testing
        Map<String, List<String>> retrieveInternalTopics(List<String> groupIds) {
            Map<String, List<String>> groupToInternalTopics = new HashMap<>();
            try {
                Map<String, StreamsGroupDescription> descriptionMap = adminClient.describeStreamsGroups(groupIds).all().get();
                for (StreamsGroupDescription description : descriptionMap.values()) {

                    List<String> sourceTopics = description.subtopologies().stream()
                        .flatMap(subtopology -> subtopology.sourceTopics().stream()).toList();

                    List<String> internalTopics = description.subtopologies().stream()
                        .flatMap(subtopology -> Stream.concat(
                            subtopology.repartitionSourceTopics().keySet().stream(),
                            subtopology.stateChangelogTopics().keySet().stream()))
                        .filter(topic -> !sourceTopics.contains(topic))
                        .collect(Collectors.toList());
                    internalTopics.removeIf(topic -> {
                        if (!isInferredInternalTopic(topic, description.groupId())) {
                            printError("The internal topic '" + topic + "' is not inferred as internal " +
                                "and thus will not be deleted with the group '" + description.groupId() + "'.", Optional.empty());
                            return true;
                        }
                        return false;
                    });
                    if (!internalTopics.isEmpty()) {
                        groupToInternalTopics.put(description.groupId(), internalTopics);
                    }
                }
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() instanceof UnsupportedVersionException) {
                    printError("Retrieving internal topics is not supported by the broker version. " +
                        "Use 'kafka-topics.sh' to list and delete the group's internal topics.", Optional.of(e.getCause()));
                } else {
                    printError("Retrieving internal topics failed due to " + e.getMessage(), Optional.of(e));
                }
            }
            return groupToInternalTopics;
        }

        private boolean isInferredInternalTopic(final String topicName, final String applicationId) {
            return topicName.startsWith(applicationId + "-") && matchesInternalTopicFormat(topicName);
        }

        public static boolean matchesInternalTopicFormat(final String topicName) {
            return topicName.endsWith("-changelog") || topicName.endsWith("-repartition")
                || topicName.endsWith("-subscription-registration-topic")
                || topicName.endsWith("-subscription-response-topic")
                || topicName.matches(".+-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-\\d+-topic")
                || topicName.matches(".+-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-\\d+-topic");
        }

        Collection<StreamsGroupMemberDescription> collectGroupMembers(String groupId) throws Exception {
            return getDescribeGroup(groupId).members();
        }

        GroupState collectGroupState(String groupId) throws Exception {
            return getDescribeGroup(groupId).groupState();
        }

        private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
            int t = opts.options.valueOf(opts.timeoutMsOpt).intValue();
            return options.timeoutMs(t);
        }
    }

    public record OffsetsInfo(Optional<Long> currentOffset, Optional<Integer> leaderEpoch, Long logEndOffset, Long lag) {
    }
}
