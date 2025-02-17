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

import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

import static org.apache.kafka.tools.ToolsUtils.minus;

public class ShareGroupCommandOptions extends CommandDefaultOptions {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShareGroupCommandOptions.class);

    private static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server(s) to connect to.";
    private static final String GROUP_DOC = "The share group we wish to act on.";
    private static final String TOPIC_DOC = "The topic whose offset information should be deleted or included in the reset offset process. " +
        "When resetting offsets, partitions can be specified using this format: 'topic1:0,1,2', where 0,1,2 are the partitions to be included.";
    private static final String ALL_TOPICS_DOC = "Consider all topics assigned to a share group in the 'reset-offsets' process.";
    private static final String LIST_DOC = "List all share groups.";
    private static final String DESCRIBE_DOC = "Describe share group, members and offset information.";
    private static final String ALL_GROUPS_DOC = "Apply to all share groups.";
    private static final String NL = System.lineSeparator();
    private static final String DELETE_DOC = "Delete share group.";
    private static final String TIMEOUT_MS_DOC = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
        "to specify the maximum amount of time in milliseconds to wait before the group stabilizes.";
    private static final String COMMAND_CONFIG_DOC = "Property file containing configs to be passed to Admin Client.";
    private static final String RESET_OFFSETS_DOC = "Reset offsets of share group. Supports one share group at the time, and instances must be inactive." + NL +
        "Has 2 execution options: --dry-run (the default) to plan which offsets to reset, and --execute to reset the offsets. " + NL +
        "You must choose one of the following reset specifications: --to-datetime, --to-earliest, --to-latest." + NL +
        "To define the scope use --all-topics or --topic.";
    private static final String DRY_RUN_DOC = "Only show results without executing changes on share groups. Supported operations: reset-offsets.";
    private static final String EXECUTE_DOC = "Execute operation. Supported operations: reset-offsets.";
    private static final String RESET_TO_DATETIME_DOC = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDThh:mm:ss.sss'";
    private static final String RESET_TO_EARLIEST_DOC = "Reset offsets to earliest offset.";
    private static final String RESET_TO_LATEST_DOC = "Reset offsets to latest offset.";
    private static final String MEMBERS_DOC = "Describe members of the group. This option may be used with the '--describe' option only.";
    private static final String OFFSETS_DOC = "Describe the group and list all topic partitions in the group along with their offset information. " +
        "This is the default sub-action and may be used with the '--describe' option only.";
    private static final String STATE_DOC = "When specified with '--describe', includes the state of the group." + NL +
        "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states. " +
        "Valid values are Empty, Stable and Dead.";
    private static final String VERBOSE_DOC = "Provide additional information, if any, when describing the group. This option may be used " +
        "with the '--describe' option only.";
    private static final String DELETE_OFFSETS_DOC = "Delete offsets of share group. Supports one share group at the time, and multiple topics.";

    final OptionSpec<String> bootstrapServerOpt;
    final OptionSpec<String> groupOpt;
    final OptionSpec<String> topicOpt;
    final OptionSpec<Void> allTopicsOpt;
    final OptionSpec<Void> listOpt;
    final OptionSpec<Void> describeOpt;
    final OptionSpec<Void> allGroupsOpt;
    final OptionSpec<Void> deleteOpt;
    final OptionSpec<Long> timeoutMsOpt;
    final OptionSpec<String> commandConfigOpt;
    final OptionSpec<Void> resetOffsetsOpt;
    final OptionSpec<Void> deleteOffsetsOpt;
    final OptionSpec<Void> dryRunOpt;
    final OptionSpec<Void> executeOpt;
    final OptionSpec<String> resetToDatetimeOpt;
    final OptionSpec<Void> resetToEarliestOpt;
    final OptionSpec<Void> resetToLatestOpt;
    final OptionSpec<Void> membersOpt;
    final OptionSpec<Void> offsetsOpt;
    final OptionSpec<String> stateOpt;
    final OptionSpec<Void> verboseOpt;

    final Set<OptionSpec<?>> allGroupSelectionScopeOpts;
    final Set<OptionSpec<?>> allShareGroupLevelOpts;
    final Set<OptionSpec<?>> allResetOffsetScenarioOpts;
    final Set<OptionSpec<?>> allDeleteOffsetsOpts;

    public ShareGroupCommandOptions(String[] args) {
        super(args);

        bootstrapServerOpt = parser.accepts("bootstrap-server", BOOTSTRAP_SERVER_DOC)
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        groupOpt = parser.accepts("group", GROUP_DOC)
            .withRequiredArg()
            .describedAs("share group")
            .ofType(String.class);
        topicOpt = parser.accepts("topic", TOPIC_DOC)
            .withRequiredArg()
            .describedAs("topic")
            .ofType(String.class);
        allTopicsOpt = parser.accepts("all-topics", ALL_TOPICS_DOC);
        listOpt = parser.accepts("list", LIST_DOC);
        describeOpt = parser.accepts("describe", DESCRIBE_DOC);
        allGroupsOpt = parser.accepts("all-groups", ALL_GROUPS_DOC);
        deleteOpt = parser.accepts("delete", DELETE_DOC);
        timeoutMsOpt = parser.accepts("timeout", TIMEOUT_MS_DOC)
            .withRequiredArg()
            .describedAs("timeout (ms)")
            .ofType(Long.class)
            .defaultsTo(30000L);
        commandConfigOpt = parser.accepts("command-config", COMMAND_CONFIG_DOC)
            .withRequiredArg()
            .describedAs("command config property file")
            .ofType(String.class);
        resetOffsetsOpt = parser.accepts("reset-offsets", RESET_OFFSETS_DOC);
        deleteOffsetsOpt = parser.accepts("delete-offsets", DELETE_OFFSETS_DOC);
        dryRunOpt = parser.accepts("dry-run", DRY_RUN_DOC);
        executeOpt = parser.accepts("execute", EXECUTE_DOC);
        resetToDatetimeOpt = parser.accepts("to-datetime", RESET_TO_DATETIME_DOC)
            .withRequiredArg()
            .describedAs("datetime")
            .ofType(String.class);
        resetToEarliestOpt = parser.accepts("to-earliest", RESET_TO_EARLIEST_DOC);
        resetToLatestOpt = parser.accepts("to-latest", RESET_TO_LATEST_DOC);
        membersOpt = parser.accepts("members", MEMBERS_DOC)
            .availableIf(describeOpt);
        offsetsOpt = parser.accepts("offsets", OFFSETS_DOC)
            .availableIf(describeOpt);
        stateOpt = parser.accepts("state", STATE_DOC)
            .availableIf(describeOpt, listOpt)
            .withOptionalArg()
            .ofType(String.class);
        verboseOpt = parser.accepts("verbose", VERBOSE_DOC)
            .availableIf(describeOpt);

        allGroupSelectionScopeOpts = new HashSet<>(Arrays.asList(groupOpt, allGroupsOpt));
        allShareGroupLevelOpts = new HashSet<>(Arrays.asList(listOpt, describeOpt, deleteOpt, deleteOffsetsOpt, resetOffsetsOpt));
        allResetOffsetScenarioOpts = new HashSet<>(Arrays.asList(resetToDatetimeOpt, resetToEarliestOpt, resetToLatestOpt));
        allDeleteOffsetsOpts = new HashSet<>(Arrays.asList(groupOpt, topicOpt));

        options = parser.parse(args);
    }

    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    public void checkArgs() {
        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list, describe, reset and delete share groups.");

        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);

        if (options.has(describeOpt)) {
            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + describeOpt + " takes one of these options: " + allGroupSelectionScopeOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
            List<OptionSpec<?>> mutuallyExclusiveOpts = Arrays.asList(membersOpt, offsetsOpt, stateOpt);
            if (mutuallyExclusiveOpts.stream().mapToInt(o -> options.has(o) ? 1 : 0).sum() > 1) {
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + describeOpt + " takes at most one of these options: " + mutuallyExclusiveOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
            }
            if (options.has(stateOpt) && options.valueOf(stateOpt) != null)
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + describeOpt + " does not take a value for " + stateOpt);
        }

        if (options.has(deleteOpt)) {
            if (!options.has(groupOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + deleteOpt + " takes the option: " + groupOpt);
            if (options.has(topicOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + deleteOpt + " does not take the option: " + topicOpt);
        }

        if (options.has(deleteOffsetsOpt)) {
            if (!options.has(groupOpt) || !options.has(topicOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + deleteOffsetsOpt + " takes the following options: " + allDeleteOffsetsOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }

        if (options.has(resetOffsetsOpt)) {
            if (options.has(dryRunOpt) && options.has(executeOpt))
                CommandLineUtils.printUsageAndExit(parser, "Option " + resetOffsetsOpt + " only accepts one of " + executeOpt + " and " + dryRunOpt);

            if (!options.has(dryRunOpt) && !options.has(executeOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "Option " + resetOffsetsOpt + " takes the option: " + executeOpt + " or " + dryRunOpt);
            }

            if (!options.has(groupOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + resetOffsetsOpt + " takes the option: " + groupOpt);

            CommandLineUtils.checkInvalidArgs(parser, options, resetToDatetimeOpt, minus(allResetOffsetScenarioOpts, resetToDatetimeOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToEarliestOpt, minus(allResetOffsetScenarioOpts, resetToEarliestOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToLatestOpt, minus(allResetOffsetScenarioOpts, resetToLatestOpt));
        }

        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allGroupSelectionScopeOpts, groupOpt));
        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allShareGroupLevelOpts, describeOpt, deleteOpt, deleteOffsetsOpt, resetOffsetsOpt));
        CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, minus(allShareGroupLevelOpts, deleteOpt, resetOffsetsOpt));
    }
}
