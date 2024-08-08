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

import joptsimple.OptionException;
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
        Set<ShareGroupState> parsedStates = Arrays.stream(input.split(",")).map(s -> ShareGroupState.parse(s.trim())).collect(Collectors.toSet());
        if (parsedStates.contains(ShareGroupState.UNKNOWN)) {
            Collection<ShareGroupState> validStates = Arrays.stream(ShareGroupState.values()).filter(s -> s != ShareGroupState.UNKNOWN).collect(Collectors.toList());
            throw new IllegalArgumentException("Invalid state list '" + input + "'. Valid states are: " + validStates.stream().map(Object::toString).collect(Collectors.joining(", ")));
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
        final Map<String, String> configOverrides;
        private final Admin adminClient;

        public ShareGroupService(ShareGroupCommandOptions opts, Map<String, String> configOverrides) {
            this.opts = opts;
            this.configOverrides = configOverrides;
            try {
                this.adminClient = createAdminClient(configOverrides);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void listGroups() throws ExecutionException, InterruptedException {
            if (opts.options.has(opts.stateOpt)) {
                String stateValue = opts.options.valueOf(opts.stateOpt);
                Set<ShareGroupState> states = (stateValue == null || stateValue.isEmpty())
                    ? Collections.emptySet()
                    : shareGroupStatesFromString(stateValue);
                List<ShareGroupListing> listings = listShareGroupsWithState(states);

                printGroupStates(listings);
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

        private void printGroupStates(List<ShareGroupListing> groups) {
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

        private void describeGroups() throws ExecutionException, InterruptedException {
            String group = opts.options.valueOf(opts.groupOpt);
            ShareGroupDescription description = getDescribeGroup(group);
            if (description == null)
                return;
            boolean shouldPrintState = opts.options.has(opts.stateOpt);
            boolean shouldPrintMemDetails = opts.options.has(opts.membersOpt);
            if (shouldPrintMemDetails) {
                printMemberDetails(description, description.members());
                return;
            }
            printGroupDescriptionTable(description, shouldPrintState);
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
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestResult = adminClient.listOffsets(earliest).all().get();
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestResult = adminClient.listOffsets(latest).all().get();

            Map<TopicPartition, Long> lag = new HashMap<>();
            for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> tp : earliestResult.entrySet()) {
                lag.put(tp.getKey(), latestResult.get(tp.getKey()).offset() - earliestResult.get(tp.getKey()).offset());
            }
            return lag;
        }

        private void printGroupDescriptionTable(ShareGroupDescription description, boolean shouldPrintState) throws ExecutionException, InterruptedException {
            Map<TopicPartition, Long> offsets = getOffsets(description.members());
            boolean notOffset = offsets == null || offsets.isEmpty();
            if (notOffset) {
                offsets = new HashMap<>();
                offsets.put(new TopicPartition("SENTINEL", -1), -1L);
            }

            boolean printedHeader = false;
            int maxItemLength = 20;
            boolean foundMax = false;
            for (Map.Entry<TopicPartition, Long> offset : offsets.entrySet()) {
                List<String> lineItem = new ArrayList<>();
                lineItem.add(description.groupId());
                lineItem.add(description.coordinator().idString());
                if (notOffset) {
                    lineItem.add("");
                } else {
                    lineItem.add(offset.getKey() + "=>" + offset.getValue());
                }
                if (shouldPrintState) {
                    lineItem.add(description.state().toString());
                }

                if (!foundMax) {
                    for (String item : lineItem) {
                        if (item != null) {
                            maxItemLength = Math.max(maxItemLength, item.length());
                        }
                    }
                    foundMax = true;
                }
                String formatAtom = "%" + (-maxItemLength) + "s";
                if (!printedHeader) {
                    String formatHeader = String.format(formatAtom + " " + formatAtom + " " + formatAtom, "GROUP", "COORDINATOR-NODE", "OFFSETS");
                    if (shouldPrintState) {
                        formatHeader = String.format(formatAtom + " " + formatAtom + " " + formatAtom + " " + formatAtom, "GROUP", "COORDINATOR-NODE", "OFFSETS", "STATE");
                    }
                    System.out.println(formatHeader);
                    printedHeader = true;
                }
                for (String item : lineItem) {
                    System.out.printf(formatAtom + " ", item);
                }
                System.out.println();
            }
        }

        private void printMemberDetails(ShareGroupDescription description, Collection<MemberDescription> members) {
            List<List<String>> lineItems = new ArrayList<>();
            int maxLen = 20;
            for (MemberDescription member : members) {
                List<String> lineItem = new ArrayList<>();
                lineItem.add(description.groupId());
                lineItem.add(member.consumerId());
                lineItem.add(member.host());
                lineItem.add(member.clientId());
                lineItem.add(member.assignment().topicPartitions().stream().map(part -> part.topic() + ":" + part.partition()).collect(Collectors.joining(",")));
                for (String item : lineItem) {
                    if (item != null) {
                        maxLen = Math.max(maxLen, item.length());
                    }
                }
                lineItems.add(lineItem);
            }

            String fmt = "%" + (-maxLen) + "s";
            String header = fmt + " " + fmt + " " + fmt + " " + fmt + " " + fmt;
            System.out.printf(header, "GROUP", "MEMBER-ID", "HOST", "CLIENT-ID", "ASSIGNMENT");
            System.out.println();
            for (List<String> item : lineItems) {
                for (String atom : item) {
                    System.out.printf(fmt + " ", atom);
                }
                System.out.println();
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
