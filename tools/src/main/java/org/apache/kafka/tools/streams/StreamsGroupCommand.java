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
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeStreamsGroupsResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
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

            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all streams groups, or describe a streams group.");

            // should have exactly one action
            long actions = Stream.of(opts.listOpt, opts.describeOpt).filter(opts.options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, or --describe.");

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
                ListGroupsResult result = adminClient.listGroups(new ListGroupsOptions()
                    .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
                    .withTypes(Set.of(GroupType.STREAMS)));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().map(GroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<GroupListing> listStreamsGroupsInStates(Set<GroupState> states) throws ExecutionException, InterruptedException {
            ListGroupsResult result = adminClient.listGroups(new ListGroupsOptions()
                .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
                .withTypes(Set.of(GroupType.STREAMS))
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
            String group = opts.options.valueOf(opts.groupOpt);
            StreamsGroupDescription description = getDescribeGroup(group);
            if (description == null)
                return;
            boolean verbose =  opts.options.has(opts.verboseOpt);
            if (opts.options.has(opts.membersOpt)) {
                printMembers(description, verbose);
            } else if (opts.options.has(opts.stateOpt)) {
                printStates(description, verbose);
            } else {
                printOffsets(description, verbose);
            }
        }

        StreamsGroupDescription getDescribeGroup(String group) throws ExecutionException, InterruptedException {
            DescribeStreamsGroupsResult result = adminClient.describeStreamsGroups(List.of(group));
            Map<String, StreamsGroupDescription> descriptionMap = result.all().get();
            return descriptionMap.get(group);
        }

        private void printMembers(StreamsGroupDescription description, boolean verbose) {
            int groupLen = Math.max(15, description.groupId().length());
            int maxMemberIdLen = 15, maxHostLen = 15, maxClientIdLen = 15;
            Collection<StreamsGroupMemberDescription> members = description.members();
            if (maybePrintEmptyGroupState(description.groupId(), description.groupState(), description.members().size())) {
                for (StreamsGroupMemberDescription member : members) {
                    maxMemberIdLen = Math.max(maxMemberIdLen, member.memberId().length());
                    maxHostLen = Math.max(maxHostLen, member.processId().length());
                    maxClientIdLen = Math.max(maxClientIdLen, member.clientId().length());
                }

                if (!verbose) {
                    String fmt = "%" + -groupLen + "s %" + -maxMemberIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s\n";
                    for (StreamsGroupMemberDescription member : members) {
                        System.out.printf(fmt, "GROUP", "MEMBER", "PROCESS", "CLIENT-ID");
                        System.out.printf(fmt, description.groupId(), member.memberId(), member.processId(), member.clientId());
                        printTasks(member.assignment(), false);
                        System.out.println();
                    }
                } else {
                    String fmt = "%" + -groupLen + "s %s %-15s%" + -maxMemberIdLen + "s %s %15s %" + -maxHostLen + "s %" + -maxClientIdLen + "s\n";
                    for (StreamsGroupMemberDescription member : members) {
                        System.out.printf(fmt, "GROUP", "TARGET-ASSIGNMENT-EPOCH", "TOPOLOGY-EPOCH", "MEMBER", "MEMBER-PROTOCOL", "MEMBER-EPOCH", "PROCESS", "CLIENT-ID");
                        System.out.printf(fmt, description.groupId(), description.targetAssignmentEpoch(), description.topologyEpoch(), member.memberId(),
                            member.isClassic() ? "classic" : "streams", member.memberEpoch(), member.processId(), member.clientId());
                        printTasks(member.assignment(), false);
                        printTasks(member.targetAssignment(), true);
                        System.out.println();
                    }
                }
            }
        }

        private void printTaskType(List<StreamsGroupMemberAssignment.TaskIds> tasks, String taskType) {
            System.out.printf("%s%n", taskType + ": " + tasks.stream().map(taskId -> taskId.subtopologyId() + ": [" + taskId.partitions()).collect(Collectors.joining(",")) + "] ");
        }

        private void printTasks(StreamsGroupMemberAssignment assignment, boolean isTarget) {
            String typePrefix = isTarget ? "TARGET-" : "";
            printTaskType(assignment.activeTasks(), typePrefix + "ACTIVE-TASKS:");
            printTaskType(assignment.standbyTasks(), typePrefix + "STANDBY-TASKS:");
            printTaskType(assignment.warmupTasks(), typePrefix + "WARMUP-TASKS:");
        }

        private void printStates(StreamsGroupDescription description, boolean verbose) {
            maybePrintEmptyGroupState(description.groupId(), description.groupState(), 1);

            int groupLen = Math.max(15, description.groupId().length());
            String coordinator = description.coordinator().host() + ":" + description.coordinator().port() + "  (" + description.coordinator().idString() + ")";
            int coordinatorLen = Math.max(25, coordinator.length());

            if (!verbose) {
                String fmt = "%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %s%n";
                System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "#MEMBERS");
                System.out.printf(fmt, description.groupId(), coordinator, description.groupState().toString(), description.members().size());
            } else {
                String fmt = "%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %-15s %-15s %s%n";
                System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "GROUP-EPOCH", "TARGET-ASSIGNMENT-EPOCH", "#MEMBERS");
                System.out.printf(fmt, description.groupId(), coordinator, description.groupState().toString(), description.groupEpoch(), description.targetAssignmentEpoch(), description.members().size());
            }
        }

        private void printOffsets(StreamsGroupDescription description, boolean verbose) throws ExecutionException, InterruptedException {
            Map<TopicPartition, Long> offsets = getOffsets(description.members(), description);
            if (maybePrintEmptyGroupState(description.groupId(), description.groupState(), offsets.size())) {
                int groupLen = Math.max(15, description.groupId().length());
                int maxTopicLen = 15;
                for (TopicPartition topicPartition : offsets.keySet()) {
                    maxTopicLen = Math.max(maxTopicLen, topicPartition.topic().length());
                }

                if (!verbose) {
                    String fmt =  "%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %s%n";
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "OFFSET-LAG");
                    for (Map.Entry<TopicPartition, Long> offset : offsets.entrySet()) {
                        System.out.printf(fmt, description.groupId(), offset.getKey().topic(), offset.getKey().partition(), offset.getValue());
                    }
                } else {
                    String fmt =  "%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %-15s %s%n";
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "LEADER-EPOCH", "OFFSET-LAG");
                    for (Map.Entry<TopicPartition, Long> offset : offsets.entrySet()) {
                        System.out.printf(fmt, description.groupId(), offset.getKey().topic(), offset.getKey().partition(), "", offset.getValue());
                    }
                }
            }
        }

        Map<TopicPartition, Long> getOffsets(Collection<StreamsGroupMemberDescription> members, StreamsGroupDescription description) throws ExecutionException, InterruptedException {
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

            Map<TopicPartition, Long> lag = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> tp : earliestResult.entrySet()) {
                lag.put(tp.getKey(), latestResult.get(tp.getKey()).offset() - earliestResult.get(tp.getKey()).offset());
            }
            return lag;
        }


        /**
         * Prints a summary of the state for situations where the group is empty or dead.
         *
         * @return Whether the group detail should be printed
         */
        public static boolean maybePrintEmptyGroupState(String group, GroupState state, int numRows) {
            if (state == GroupState.DEAD) {
                printError("Streams group '" + group + "' does not exist.", Optional.empty());
            } else if (state == GroupState.EMPTY) {
                System.err.println("\nStreams group '" + group + "' has no active members.");
            }

            return !state.equals(GroupState.DEAD) && numRows > 0;
        }

        private static Set<TopicPartition> getTopicPartitions(List<StreamsGroupMemberAssignment.TaskIds> taskIds, StreamsGroupDescription description) {
            Map<String, List<String>> allSourceTopics = new HashMap<>();
            for (StreamsGroupSubtopologyDescription subtopologyDescription : description.subtopologies()) {
                allSourceTopics.put(subtopologyDescription.subtopologyId(), subtopologyDescription.sourceTopics());
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
    }
}
