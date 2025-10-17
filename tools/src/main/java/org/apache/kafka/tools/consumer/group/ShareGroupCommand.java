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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AbstractOptions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsOptions;
import org.apache.kafka.clients.admin.DeleteShareGroupOffsetsResult;
import org.apache.kafka.clients.admin.DeleteShareGroupsOptions;
import org.apache.kafka.clients.admin.DescribeShareGroupsOptions;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.admin.ListShareGroupOffsetsSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareMemberAssignment;
import org.apache.kafka.clients.admin.ShareMemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;
import org.apache.kafka.tools.OffsetsUtils;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import joptsimple.OptionException;

public class ShareGroupCommand {

    static final String MISSING_COLUMN_VALUE = "-";

    public static void main(String[] args) {
        ShareGroupCommandOptions opts = new ShareGroupCommandOptions(args);
        try {
            opts.checkArgs();
            CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all share groups, describe a share group, delete share group info, or reset share group offsets.");

            // should have exactly one action
            long actions = Stream.of(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).filter(opts.options::has).count();
            if (actions != 1)
                CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets.");

            run(opts);
        } catch (OptionException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        }
    }

    public static void run(ShareGroupCommandOptions opts) {
        try (ShareGroupService shareGroupService = new ShareGroupService(opts, Map.of())) {
            if (opts.options.has(opts.listOpt)) {
                shareGroupService.listGroups();
            } else if (opts.options.has(opts.describeOpt)) {
                shareGroupService.describeGroups();
            } else if (opts.options.has(opts.deleteOpt)) {
                shareGroupService.deleteShareGroups();
            } else if (opts.options.has(opts.resetOffsetsOpt)) {
                shareGroupService.resetOffsets();
            } else if (opts.options.has(opts.deleteOffsetsOpt)) {
                shareGroupService.deleteOffsets();
            }
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing share group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static Set<GroupState> groupStatesFromString(String input) {
        Set<GroupState> parsedStates =
            Arrays.stream(input.split(",")).map(s -> GroupState.parse(s.trim())).collect(Collectors.toSet());
        Set<GroupState> validStates = GroupState.groupStatesForType(GroupType.SHARE);
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
    static class ShareGroupService implements AutoCloseable {
        final ShareGroupCommandOptions opts;
        private final Admin adminClient;
        private final OffsetsUtils offsetsUtils;

        public ShareGroupService(ShareGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            this.offsetsUtils = new OffsetsUtils(adminClient, opts.parser, getOffsetsUtilsOptions(opts));
        }

        public ShareGroupService(ShareGroupCommandOptions opts, Admin adminClient) {
            this.opts = opts;
            this.adminClient = adminClient;
            this.offsetsUtils = new OffsetsUtils(adminClient, opts.parser, getOffsetsUtilsOptions(opts));
        }

        private OffsetsUtils.OffsetsUtilsOptions getOffsetsUtilsOptions(ShareGroupCommandOptions opts) {
            return
                new OffsetsUtils.OffsetsUtilsOptions(opts.options.valuesOf(opts.groupOpt),
                    opts.options.valuesOf(opts.resetToDatetimeOpt),
                    opts.options.valueOf(opts.timeoutMsOpt));
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<GroupState> states = (stateValue == null || stateValue.isEmpty())
                    ? Set.of()
                    : groupStatesFromString(stateValue);
                List<GroupListing> listings = listShareGroupsInStates(states);

                printGroupInfo(listings);
            } else
                listShareGroups().forEach(System.out::println);
        }

        List<String> listShareGroups() {
            try {
                ListGroupsResult result = adminClient.listGroups(ListGroupsOptions.forShareGroups()
                    .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue()));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().map(GroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<GroupListing> listDetailedShareGroups() {
            try {
                ListGroupsResult result = adminClient.listGroups(ListGroupsOptions.forShareGroups()
                    .timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue()));
                Collection<GroupListing> listings = result.all().get();
                return listings.stream().toList();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<GroupListing> listShareGroupsInStates(Set<GroupState> states) throws ExecutionException, InterruptedException {
            ListGroupsResult result = adminClient.listGroups(ListGroupsOptions.forShareGroups()
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

        /**
         * Prints a summary of the state for situations where the group is empty or dead.
         *
         * @return Whether the group detail should be printed
         */
        public static boolean maybePrintEmptyGroupState(String group, GroupState state, int numRows) {
            if (state == GroupState.DEAD) {
                printError("Share group '" + group + "' does not exist.", Optional.empty());
            } else if (state == GroupState.EMPTY) {
                System.err.println("\nShare group '" + group + "' has no active members.");
            }

            return !state.equals(GroupState.DEAD) && numRows > 0;
        }

        public void describeGroups() throws ExecutionException, InterruptedException {
            Collection<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? listShareGroups()
                : opts.options.valuesOf(opts.groupOpt);
            if (opts.options.has(opts.membersOpt)) {
                TreeMap<String, ShareGroupDescription> members = collectGroupsDescription(groupIds);
                printMembers(members, opts.options.has(opts.verboseOpt));
            } else if (opts.options.has(opts.stateOpt)) {
                TreeMap<String, ShareGroupDescription> states = collectGroupsDescription(groupIds);
                printStates(states, opts.options.has(opts.verboseOpt));
            } else {
                TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> offsets
                    = collectGroupsOffsets(groupIds);
                printOffsets(offsets, opts.options.has(opts.verboseOpt));
            }
        }

        Map<String, Throwable> deleteShareGroups() {
            List<GroupListing> shareGroupIds = listDetailedShareGroups();
            List<String> groupIds = opts.options.has(opts.allGroupsOpt)
                ? shareGroupIds.stream().map(GroupListing::groupId).toList()
                : opts.options.valuesOf(opts.groupOpt);

            // Pre admin call checks
            LinkedHashSet<String> groupIdSet = new LinkedHashSet<>(groupIds);
            Map<String, Exception> errGroups = new HashMap<>();
            for (String groupId : groupIdSet) {
                Optional<GroupListing> listing = shareGroupIds.stream().filter(item -> item.groupId().equals(groupId)).findAny();
                if (listing.isEmpty()) {
                    errGroups.put(groupId, new IllegalArgumentException("Group '" + groupId + "' is not a share group."));
                } else {
                    Optional<GroupState> groupState = listing.get().groupState();
                    groupState.ifPresent(state -> {
                        if (state == GroupState.DEAD) {
                            errGroups.put(groupId, new IllegalStateException("Share group '" + groupId + "' group state is DEAD."));
                        } else if (state != GroupState.EMPTY) {
                            errGroups.put(groupId, new GroupNotEmptyException("Share group '" + groupId + "' is not EMPTY."));
                        }
                    });
                }
            }

            groupIdSet.removeAll(errGroups.keySet());

            Map<String, KafkaFuture<Void>> groupsToDelete = groupIdSet.isEmpty() ? Map.of() : adminClient.deleteShareGroups(
                groupIdSet.stream().toList(),
                withTimeoutMs(new DeleteShareGroupsOptions())
            ).deletedGroups();

            Map<String, Throwable> success = new HashMap<>();
            Map<String, Throwable> failed = new HashMap<>(errGroups);

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

            if (failed.isEmpty())
                System.out.println("Deletion of requested share groups (" + success.keySet().stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")) + ") was successful.");
            else {
                printError("Deletion of some share groups failed:", Optional.empty());
                failed.forEach((group, error) -> System.out.println("* Group '" + group + "' could not be deleted due to: " + error));

                if (!success.isEmpty())
                    System.out.println("\nThese share groups were deleted successfully: " + success.keySet().stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")));
            }

            failed.putAll(success);

            return failed;
        }

        void deleteOffsets() {
            String groupId = opts.options.valueOf(opts.groupOpt);
            List<String> topics = opts.options.valuesOf(opts.topicOpt);

            Entry<Throwable, Map<String, Throwable>> res = sendDeleteShareGroupOffsetsRequest(groupId, new HashSet<>(topics));

            Throwable topLevelResult = res.getKey();
            Map<String, Throwable> topicLevelResult = res.getValue();

            if (topLevelResult != null) {
                Errors topLevelError = Errors.forException(topLevelResult);
                switch (topLevelError) {
                    case INVALID_GROUP_ID:
                    case GROUP_ID_NOT_FOUND:
                    case GROUP_AUTHORIZATION_FAILED:
                    case NON_EMPTY_GROUP:
                        printError(topLevelResult.getMessage(), Optional.empty());
                        break;
                    case TOPIC_AUTHORIZATION_FAILED:
                    case UNKNOWN_TOPIC_OR_PARTITION:
                        // These are expected topic-level errors which will be reported in the topic-level results
                        break;
                    default:
                        printError("Encounter some unknown error: " + topLevelResult, Optional.empty());
                }
            }

            if (topicLevelResult != null && !topicLevelResult.isEmpty()) {
                int maxTopicLen = 15;
                for (String topic : topicLevelResult.keySet()) {
                    maxTopicLen = Math.max(maxTopicLen, topic.length());
                }

                String format = "%n%" + (-maxTopicLen) + "s %s";

                System.out.printf(format, "TOPIC", "STATUS");
                topicLevelResult.entrySet().stream()
                    .sorted(Entry.comparingByKey())
                    .forEach(e -> {
                        String topic = e.getKey();
                        Throwable error = e.getValue();
                        System.out.printf(format,
                            topic,
                            error != null ? "Error: " + error.getMessage() : "Successful"
                        );
                    });
            }

            System.out.println();
        }

        Entry<Throwable, Map<String, Throwable>> sendDeleteShareGroupOffsetsRequest(String groupId, Set<String> topics) {
            Map<String, Throwable> topicLevelResult = new HashMap<>();

            DeleteShareGroupOffsetsResult deleteResult = adminClient.deleteShareGroupOffsets(
                groupId,
                new HashSet<>(topics),
                withTimeoutMs(new DeleteShareGroupOffsetsOptions()));

            Throwable topLevelException = null;

            try {
                deleteResult.all().get();
            } catch (ExecutionException | InterruptedException e) {
                topLevelException = e.getCause();
            }

            topics.forEach(topic -> {
                try {
                    deleteResult.topicResult(topic).get();
                    topicLevelResult.put(topic, null);
                } catch (ExecutionException | InterruptedException e) {
                    topicLevelResult.put(topic, e.getCause());
                }
            });

            return new SimpleImmutableEntry<>(topLevelException, topicLevelResult);
        }

        void resetOffsets() {
            String groupId = opts.options.valueOf(opts.groupOpt);
            try {
                ShareGroupDescription shareGroupDescription = describeShareGroups(List.of(groupId)).get(groupId);
                if (!(GroupState.EMPTY.equals(shareGroupDescription.groupState()) || GroupState.DEAD.equals(shareGroupDescription.groupState()))) {
                    CommandLineUtils.printErrorAndExit(String.format("Share group '%s' is not empty.", groupId));
                }
                resetOffsetsForInactiveGroup(groupId);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause instanceof GroupIdNotFoundException) {
                    resetOffsetsForInactiveGroup(groupId);
                } else if (cause instanceof KafkaException) {
                    CommandLineUtils.printErrorAndExit(cause.getMessage());
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }

        private void resetOffsetsForInactiveGroup(String groupId) {
            try {
                Collection<TopicPartition> partitionsToReset = getPartitionsToReset(groupId);
                Map<TopicPartition, OffsetAndMetadata> offsetsToReset = prepareOffsetsToReset(partitionsToReset);
                boolean dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt);
                if (!dryRun) {
                    adminClient.alterShareGroupOffsets(groupId,
                        offsetsToReset.entrySet().stream()
                            .collect(Collectors.toMap(
                                Entry::getKey, entry -> entry.getValue().offset()
                            ))
                    ).all().get();
                }
                OffsetsUtils.printOffsetsToReset(Map.of(groupId, offsetsToReset));
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause();
                if (cause instanceof KafkaException) {
                    throw (KafkaException) cause;
                } else {
                    throw new RuntimeException(cause);
                }
            }
        }

        private Collection<TopicPartition> getPartitionsToReset(String groupId) throws ExecutionException, InterruptedException {
            Collection<TopicPartition> partitionsToReset;

            if (opts.options.has(opts.topicOpt)) {
                partitionsToReset = offsetsUtils.parseTopicPartitionsToReset(opts.options.valuesOf(opts.topicOpt));
            } else {
                Map<String, ListShareGroupOffsetsSpec> groupSpecs = Map.of(groupId, new ListShareGroupOffsetsSpec());
                Map<TopicPartition, OffsetAndMetadata> offsetsByTopicPartitions = adminClient.listShareGroupOffsets(groupSpecs).all().get().get(groupId);
                partitionsToReset = offsetsByTopicPartitions.keySet();
            }

            return partitionsToReset;
        }

        private Map<TopicPartition, OffsetAndMetadata> prepareOffsetsToReset(Collection<TopicPartition> partitionsToReset) {
            offsetsUtils.checkAllTopicPartitionsValid(partitionsToReset);
            if (opts.options.has(opts.resetToEarliestOpt)) {
                return offsetsUtils.resetToEarliest(partitionsToReset);
            } else if (opts.options.has(opts.resetToLatestOpt)) {
                return offsetsUtils.resetToLatest(partitionsToReset);
            } else if (opts.options.has(opts.resetToDatetimeOpt)) {
                return offsetsUtils.resetToDateTime(partitionsToReset);
            }
            CommandLineUtils
                .printUsageAndExit(opts.parser, String.format("Option '%s' requires one of the following scenarios: %s", opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts));
            return null;
        }

        private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
            int t = opts.options.valueOf(opts.timeoutMsOpt).intValue();
            return options.timeoutMs(t);
        }

        Map<String, ShareGroupDescription> describeShareGroups(Collection<String> groupIds) throws ExecutionException, InterruptedException {
            Map<String, ShareGroupDescription> res = new HashMap<>();
            Map<String, KafkaFuture<ShareGroupDescription>> stringKafkaFutureMap = adminClient.describeShareGroups(
                groupIds,
                new DescribeShareGroupsOptions().timeoutMs(opts.options.valueOf(opts.timeoutMsOpt).intValue())
            ).describedGroups();

            for (Entry<String, KafkaFuture<ShareGroupDescription>> e : stringKafkaFutureMap.entrySet()) {
                res.put(e.getKey(), e.getValue().get());
            }
            return res;
        }

        TreeMap<String, ShareGroupDescription> collectGroupsDescription(Collection<String> groupIds) throws ExecutionException, InterruptedException {
            Map<String, ShareGroupDescription> shareGroups = describeShareGroups(groupIds);
            TreeMap<String, ShareGroupDescription> res = new TreeMap<>();
            res.putAll(shareGroups);
            return res;
        }

        TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> collectGroupsOffsets(Collection<String> groupIds) throws ExecutionException, InterruptedException {
            Map<String, ShareGroupDescription> shareGroups = describeShareGroups(groupIds);
            TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> groupOffsets = new TreeMap<>();

            shareGroups.forEach((groupId, shareGroup) -> {
                Map<String, ListShareGroupOffsetsSpec> groupSpecs = Map.of(groupId, new ListShareGroupOffsetsSpec());

                try {
                    Map<TopicPartition, OffsetAndMetadata> startOffsets = adminClient.listShareGroupOffsets(groupSpecs).all().get().get(groupId);
                    Set<SharePartitionOffsetInformation> partitionOffsets = mapOffsetsToSharePartitionInformation(groupId, startOffsets);

                    groupOffsets.put(groupId, new SimpleImmutableEntry<>(shareGroup, partitionOffsets));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            return groupOffsets;
        }

        private static Set<SharePartitionOffsetInformation> mapOffsetsToSharePartitionInformation(String groupId, Map<TopicPartition, OffsetAndMetadata> startOffsets) {
            Set<SharePartitionOffsetInformation> partitionOffsets = new HashSet<>();

            startOffsets.forEach((tp, offsetAndMetadata) -> {
                if (offsetAndMetadata != null) {
                    partitionOffsets.add(new SharePartitionOffsetInformation(
                        groupId,
                        tp.topic(),
                        tp.partition(),
                        Optional.of(offsetAndMetadata.offset()),
                        offsetAndMetadata.leaderEpoch()
                    ));
                } else {
                    partitionOffsets.add(new SharePartitionOffsetInformation(
                        groupId,
                        tp.topic(),
                        tp.partition(),
                        Optional.empty(),
                        Optional.empty()
                    ));
                }
            });
            return partitionOffsets;
        }

        private void printOffsets(TreeMap<String, Entry<ShareGroupDescription, Collection<SharePartitionOffsetInformation>>> offsets, boolean verbose) {
            offsets.forEach((groupId, tuple) -> {
                Collection<SharePartitionOffsetInformation> offsetsInfo = tuple.getValue().stream()
                    .sorted(Comparator
                        .comparing((SharePartitionOffsetInformation info) -> info.topic)
                        .thenComparingInt(info -> info.partition))
                    .toList();

                String fmt = printOffsetFormat(groupId, offsetsInfo, verbose);

                if (verbose) {
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "LEADER-EPOCH", "START-OFFSET");
                } else {
                    System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "START-OFFSET");
                }

                for (SharePartitionOffsetInformation info : offsetsInfo) {
                    if (verbose) {
                        System.out.printf(fmt,
                            groupId,
                            info.topic,
                            info.partition,
                            info.leaderEpoch.map(Object::toString).orElse(MISSING_COLUMN_VALUE),
                            info.offset.map(Object::toString).orElse(MISSING_COLUMN_VALUE)
                        );
                    } else {
                        System.out.printf(fmt,
                            groupId,
                            info.topic,
                            info.partition,
                            info.offset.map(Object::toString).orElse(MISSING_COLUMN_VALUE)
                        );
                    }
                }
                System.out.println();
            });
        }

        private static String printOffsetFormat(String groupId, Collection<SharePartitionOffsetInformation> offsetsInfo, boolean verbose) {
            int groupLen = Math.max(15, groupId.length());
            int maxTopicLen = 15;
            for (SharePartitionOffsetInformation info : offsetsInfo) {
                maxTopicLen = Math.max(maxTopicLen, info.topic.length());
            }
            if (verbose) {
                return "\n%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %-13s %s";
            } else {
                return "\n%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %s";
            }
        }

        private void printStates(Map<String, ShareGroupDescription> descriptions, boolean verbose) {
            descriptions.forEach((groupId, description) -> {
                maybePrintEmptyGroupState(groupId, description.groupState(), 1);

                int groupLen = Math.max(15, groupId.length());
                String coordinator = description.coordinator().host() + ":" + description.coordinator().port() + "  (" + description.coordinator().idString() + ")";
                int coordinatorLen = Math.max(25, coordinator.length());

                if (verbose) {
                    String fmt = "\n%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %-12s %-17s %s";
                    System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "GROUP-EPOCH", "ASSIGNMENT-EPOCH", "#MEMBERS");
                    System.out.printf(fmt, groupId, coordinator, description.groupState().toString(),
                        description.groupEpoch(), description.targetAssignmentEpoch(), description.members().size());
                } else {
                    String fmt = "\n%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %s";
                    System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "#MEMBERS");
                    System.out.printf(fmt, groupId, coordinator, description.groupState().toString(), description.members().size());
                }
                System.out.println();
            });
        }

        private void printMembers(TreeMap<String, ShareGroupDescription> descriptions, boolean verbose) {
            descriptions.forEach((groupId, description) -> {
                int groupLen = Math.max(15, groupId.length());
                int maxConsumerIdLen = 15, maxHostLen = 15, maxClientIdLen = 15;
                Collection<ShareMemberDescription> members = description.members()
                    .stream()
                    .sorted(Comparator.comparing(ShareMemberDescription::consumerId))
                    .toList();
                if (maybePrintEmptyGroupState(groupId, description.groupState(), description.members().size())) {
                    for (ShareMemberDescription member : members) {
                        maxConsumerIdLen = Math.max(maxConsumerIdLen, member.consumerId().length());
                        maxHostLen = Math.max(maxHostLen, member.host().length());
                        maxClientIdLen = Math.max(maxClientIdLen, member.clientId().length());
                    }

                    if (verbose) {
                        String fmt = "\n%" + -groupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-12s %-13s %s";
                        System.out.printf(fmt, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS", "MEMBER-EPOCH", "ASSIGNMENT");
                        for (ShareMemberDescription member : members) {
                            System.out.printf(fmt, groupId, member.consumerId(), member.host(), member.clientId(),
                                member.assignment().topicPartitions().size(), member.memberEpoch(), getAssignmentString(member.assignment()));
                        }
                    } else {
                        String fmt = "\n%" + -groupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %-12s %s";
                        System.out.printf(fmt, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS", "ASSIGNMENT");
                        for (ShareMemberDescription member : members) {
                            System.out.printf(fmt, groupId, member.consumerId(), member.host(), member.clientId(),
                                member.assignment().topicPartitions().size(), getAssignmentString(member.assignment()));
                        }
                    }
                    System.out.println();
                }
            });
        }

        private String getAssignmentString(ShareMemberAssignment assignment) {
            Map<String, List<TopicPartition>> grouped = new HashMap<>();
            assignment.topicPartitions().forEach(tp ->
                grouped
                   .computeIfAbsent(tp.topic(), key -> new ArrayList<>())
                    .add(tp)
            );
            return grouped.entrySet().stream().map(entry -> {
                String topicName = entry.getKey();
                List<TopicPartition> topicPartitions = entry.getValue();
                return topicPartitions
                    .stream()
                    .map(TopicPartition::partition)
                    .sorted()
                    .map(Object::toString)
                    .collect(Collectors.joining(",", topicName + ":", ""));
            }).sorted().collect(Collectors.joining(";"));
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

    record SharePartitionOffsetInformation(String group, String topic, int partition, Optional<Long> offset,
                                           Optional<Integer> leaderEpoch) {
    }
}
