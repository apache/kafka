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
import org.apache.kafka.clients.admin.DescribeShareGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListShareGroupsOptions;
import org.apache.kafka.clients.admin.ListShareGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.clients.admin.ShareGroupListing;
import org.apache.kafka.common.ShareGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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

public class ShareGroupCommand {

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
        try (ShareGroupService shareGroupService = new ShareGroupService(opts, Collections.emptyMap())) {
            if (opts.options.has(opts.listOpt)) {
                shareGroupService.listGroups();
            } else if (opts.options.has(opts.describeOpt)) {
                shareGroupService.describeGroups();
            } else if (opts.options.has(opts.deleteOpt)) {
                throw new UnsupportedOperationException("--delete option is not yet implemented");
            } else if (opts.options.has(opts.resetOffsetsOpt)) {
                throw new UnsupportedOperationException("--reset-offsets option is not yet implemented");
            } else if (opts.options.has(opts.deleteOffsetsOpt)) {
                throw new UnsupportedOperationException("--delete-offsets option is not yet implemented");
            }
        } catch (IllegalArgumentException e) {
            CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage());
        } catch (Throwable e) {
            printError("Executing share group command failed due to " + e.getMessage(), Optional.of(e));
        }
    }

    static Set<ShareGroupState> shareGroupStatesFromString(String input) {
        Set<ShareGroupState> parsedStates =
            Arrays.stream(input.split(",")).map(s -> ShareGroupState.parse(s.trim())).collect(Collectors.toSet());
        if (parsedStates.contains(ShareGroupState.UNKNOWN)) {
            Collection<ShareGroupState> validStates =
                Arrays.stream(ShareGroupState.values()).filter(s -> s != ShareGroupState.UNKNOWN).collect(Collectors.toList());
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " +
                validStates.stream().map(Object::toString).collect(Collectors.joining(", ")));
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

        public ShareGroupService(ShareGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public ShareGroupService(ShareGroupCommandOptions opts, Admin adminClient) {
            this.opts = opts;
            this.adminClient = adminClient;
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<ShareGroupState> states = (stateValue == null || stateValue.isEmpty())
                    ? Collections.emptySet()
                    : shareGroupStatesFromString(stateValue);
                List<ShareGroupListing> listings = listShareGroupsWithState(states);

                printGroupInfo(listings);
            } else
                listShareGroups().forEach(System.out::println);
        }

        List<String> listShareGroups() {
            try {
                ListShareGroupsResult result = adminClient.listShareGroups(withTimeoutMs(new ListShareGroupsOptions()));
                Collection<ShareGroupListing> listings = result.all().get();
                return listings.stream().map(ShareGroupListing::groupId).collect(Collectors.toList());
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        List<ShareGroupListing> listShareGroupsWithState(Set<ShareGroupState> states) throws ExecutionException, InterruptedException {
            ListShareGroupsOptions listShareGroupsOptions = withTimeoutMs(new ListShareGroupsOptions());
            listShareGroupsOptions.inStates(states);
            ListShareGroupsResult result = adminClient.listShareGroups(listShareGroupsOptions);
            return new ArrayList<>(result.all().get());
        }

        private void printGroupInfo(List<ShareGroupListing> groups) {
            // find proper columns width
            int maxGroupLen = 15;
            for (ShareGroupListing group : groups) {
                maxGroupLen = Math.max(maxGroupLen, group.groupId().length());
            }
            System.out.printf("%" + (-maxGroupLen) + "s %s\n", "GROUP", "STATE");
            for (ShareGroupListing group : groups) {
                String groupId = group.groupId();
                String state = group.state().orElse(ShareGroupState.UNKNOWN).toString();
                System.out.printf("%" + (-maxGroupLen) + "s %s\n", groupId, state);
            }
        }

        /**
         * Prints a summary of the state for situations where the group is empty or dead.
         *
         * @return Whether the group detail should be printed
         */
        public static boolean maybePrintEmptyGroupState(String group, ShareGroupState state, int numRows) {
            if (state == ShareGroupState.DEAD) {
                printError("Share group '" + group + "' does not exist.", Optional.empty());
            } else if (state == ShareGroupState.EMPTY) {
                System.err.println("\nShare group '" + group + "' has no active members.");
            }

            return !state.equals(ShareGroupState.DEAD) && numRows > 0;
        }

        public void describeGroups() throws ExecutionException, InterruptedException {
            String group = opts.options.valueOf(opts.groupOpt);
            ShareGroupDescription description = getDescribeGroup(group);
            if (description == null)
                return;
            if (opts.options.has(opts.membersOpt)) {
                printMembers(description);
            } else if (opts.options.has(opts.stateOpt)) {
                printStates(description);
            } else {
                printOffsets(description);
            }
        }

        ShareGroupDescription getDescribeGroup(String group) throws ExecutionException, InterruptedException {
            DescribeShareGroupsResult result = adminClient.describeShareGroups(Collections.singletonList(group));
            Map<String, ShareGroupDescription> descriptionMap = result.all().get();
            if (descriptionMap.containsKey(group)) {
                return descriptionMap.get(group);
            }
            return null;
        }

        Map<TopicPartition, Long> getOffsets(Collection<MemberDescription> members) throws ExecutionException, InterruptedException {
            Set<TopicPartition> allTp = new HashSet<>();
            for (MemberDescription memberDescription : members) {
                allTp.addAll(memberDescription.assignment().topicPartitions());
            }
            // fetch latest and earliest offsets
            Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
            Map<TopicPartition, OffsetSpec> latest = new HashMap<>();

            for (TopicPartition tp : allTp) {
                earliest.put(tp, OffsetSpec.earliest());
                latest.put(tp, OffsetSpec.latest());
            }
            // This call to obtain the earliest offsets will be replaced once adminClient.listShareGroupOffsets is implemented
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestResult = adminClient.listOffsets(earliest).all().get();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestResult = adminClient.listOffsets(latest).all().get();

            Map<TopicPartition, Long> lag = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> tp : earliestResult.entrySet()) {
                lag.put(tp.getKey(), latestResult.get(tp.getKey()).offset() - earliestResult.get(tp.getKey()).offset());
            }
            return lag;
        }

        private void printOffsets(ShareGroupDescription description) throws ExecutionException, InterruptedException {
            Map<TopicPartition, Long> offsets = getOffsets(description.members());
            if (maybePrintEmptyGroupState(description.groupId(), description.state(), offsets.size())) {
                String fmt = printOffsetFormat(description, offsets);
                System.out.printf(fmt, "GROUP", "TOPIC", "PARTITION", "OFFSET");

                for (Map.Entry<TopicPartition, Long> offset : offsets.entrySet()) {
                    System.out.printf(fmt, description.groupId(), offset.getKey().topic(), offset.getKey().partition(), offset.getValue());
                }
            }
        }

        private static String printOffsetFormat(ShareGroupDescription description, Map<TopicPartition, Long> offsets) {
            int groupLen = Math.max(15, description.groupId().length());
            int maxTopicLen = 15;
            for (TopicPartition topicPartition : offsets.keySet()) {
                maxTopicLen = Math.max(maxTopicLen, topicPartition.topic().length());
            }
            return "%" + (-groupLen) + "s %" + (-maxTopicLen) + "s %-10s %s\n";
        }

        private void printStates(ShareGroupDescription description) {
            maybePrintEmptyGroupState(description.groupId(), description.state(), 1);

            int groupLen = Math.max(15, description.groupId().length());
            String coordinator = description.coordinator().host() + ":" + description.coordinator().port() + "  (" + description.coordinator().idString() + ")";
            int coordinatorLen = Math.max(25, coordinator.length());

            String fmt = "%" + -groupLen + "s %" + -coordinatorLen + "s %-15s %s\n";
            System.out.printf(fmt, "GROUP", "COORDINATOR (ID)", "STATE", "#MEMBERS");
            System.out.printf(fmt, description.groupId(), coordinator, description.state().toString(), description.members().size());
        }

        private void printMembers(ShareGroupDescription description) {
            int groupLen = Math.max(15, description.groupId().length());
            int maxConsumerIdLen = 15, maxHostLen = 15, maxClientIdLen = 15;
            Collection<MemberDescription> members = description.members();
            if (maybePrintEmptyGroupState(description.groupId(), description.state(), description.members().size())) {
                for (MemberDescription member : members) {
                    maxConsumerIdLen = Math.max(maxConsumerIdLen, member.consumerId().length());
                    maxHostLen = Math.max(maxHostLen, member.host().length());
                    maxClientIdLen = Math.max(maxClientIdLen, member.clientId().length());
                }

                String fmt = "%" + -groupLen + "s %" + -maxConsumerIdLen + "s %" + -maxHostLen + "s %" + -maxClientIdLen + "s %s\n";
                System.out.printf(fmt, "GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "ASSIGNMENT");
                for (MemberDescription member : members) {
                    System.out.printf(fmt, description.groupId(), member.consumerId(), member.host(), member.clientId(),
                        member.assignment().topicPartitions().stream().map(part -> part.topic() + ":" + part.partition()).collect(Collectors.joining(",")));
                }
            }
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

        private <T extends AbstractOptions<T>> T withTimeoutMs(T options) {
            int t = opts.options.valueOf(opts.timeoutMsOpt).intValue();
            return options.timeoutMs(t);
        }
    }
}
