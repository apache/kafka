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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import joptsimple.OptionSpec;

import static org.apache.kafka.tools.ToolsUtils.minus;

public class ShareGroupCommandOptions extends CommandDefaultOptions {
    private static final String NL = System.lineSeparator();

    private static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server(s) to connect to.";
    private static final String GROUP_DOC = "The share group id.";
    private static final String TOPIC_DOC = "The topic whose offset information should be deleted or reset. " +
        "When resetting offsets, partitions can be specified using this format: `topic:0,1,2`, where 0,1,2 are the partitions to be included.";
    private static final String ALL_TOPICS_DOC = "Apply to all topics. Supported operation: reset-offsets.";
    private static final String LIST_DOC = "List all share groups.";
    private static final String DESCRIBE_DOC = "Describe share group, members and offset information.";
    private static final String ALL_GROUPS_DOC = "Apply to all share groups.";
    private static final String DELETE_DOC = "Delete share group.";
    private static final String TIMEOUT_MS_DOC = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
        "to specify the maximum amount of time in milliseconds to wait before the group stabilizes.";
    private static final String COMMAND_CONFIG_DOC = "Property file containing configs to be passed to Admin Client.";
    private static final String RESET_OFFSETS_DOC = "Reset offsets of share group. Supports one share group at a time, and instances must be inactive." + NL +
        "Has 2 execution options: --dry-run to plan which offsets to reset, and --execute to reset the offsets. " + NL +
        "Additionally, the --export option can be used to generate the offsets in CSV format for export to a file." + NL +
        "You must choose one of the following reset specifications: --to-datetime, --to-earliest, --to-latest, --from-file, --to-current, --to-offset." + NL +
        "To define the scope, use --all-topics or --topic. The scope must be specified unless you use --from-file." + NL +
        "Fails if neither --dry-run nor --execute is specified.";
    private static final String DRY_RUN_DOC = "Output offset reset information without executing the operation. Supported operation: reset-offsets.";
    private static final String EXECUTE_DOC = "Execute the offset reset operation. Supported operation: reset-offsets.";
    private static final String EXPORT_DOC = "Generate offset reset information in CSV format for export to a file. Supported operation: reset-offsets.";
    private static final String RESET_TO_OFFSET_DOC = "Reset offsets to a specific offset.";
    private static final String RESET_FROM_FILE_DOC = "Reset offsets to values defined in CSV file.";
    private static final String RESET_TO_DATETIME_DOC = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDThh:mm:ss.sss'";
    private static final String RESET_TO_EARLIEST_DOC = "Reset offsets to earliest offset.";
    private static final String RESET_TO_LATEST_DOC = "Reset offsets to latest offset.";
    private static final String RESET_TO_CURRENT_DOC = "Reset offsets to current offset.";
    private static final String MEMBERS_DOC = "Describe members of the group. This option may be used with the --describe option only.";
    private static final String OFFSETS_DOC = "Describe the group and list all topic partitions in the group along with their offset information. " +
        "This is the default sub-action and may be used with the --describe option only.";
    private static final String STATE_DOC = "When specified with --describe, it displays the state of the group." + NL +
        "When specified with --list, it displays the state of all groups. It can also be used to list groups with specific states. " +
        "Valid values are Empty, Stable and Dead.";
    private static final String VERBOSE_DOC = "Provide additional information, if any, when describing the group. This option may be used " +
        "with the --describe option only.";
    private static final String DELETE_OFFSETS_DOC = "Delete offsets of share group. Supports one share group at a time, and multiple topics.";

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
    final OptionSpec<Void> exportOpt;
    final OptionSpec<Long> resetToOffsetOpt;
    final OptionSpec<String> resetFromFileOpt;
    final OptionSpec<Void> executeOpt;
    final OptionSpec<String> resetToDatetimeOpt;
    final OptionSpec<Void> resetToEarliestOpt;
    final OptionSpec<Void> resetToLatestOpt;
    final OptionSpec<Void> resetToCurrentOpt;
    final OptionSpec<Void> membersOpt;
    final OptionSpec<Void> offsetsOpt;
    final OptionSpec<String> stateOpt;
    final OptionSpec<Void> verboseOpt;

    final Set<OptionSpec<?>> allGroupSelectionScopeOpts;
    final Set<OptionSpec<?>> allTopicSelectionScopeOpts;
    final Set<OptionSpec<?>> allShareGroupLevelOpts;
    final Set<OptionSpec<?>> allResetOffsetsScopeOpts;
    final Set<OptionSpec<?>> allResetOffsetsScenarioOpts;
    final Set<OptionSpec<?>> allDeleteOffsetsOpts;

    public ShareGroupCommandOptions(String[] args) {
        super(args);

        bootstrapServerOpt = parser.accepts("bootstrap-server", BOOTSTRAP_SERVER_DOC)
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        groupOpt = parser.accepts("group", GROUP_DOC)
            .withRequiredArg()
            .describedAs("group id")
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
        exportOpt = parser.accepts("export", EXPORT_DOC);
        resetToOffsetOpt = parser.accepts("to-offset", RESET_TO_OFFSET_DOC)
            .withRequiredArg()
            .describedAs("offset")
            .ofType(Long.class);
        resetFromFileOpt = parser.accepts("from-file", RESET_FROM_FILE_DOC)
            .withRequiredArg()
            .describedAs("path to CSV file")
            .ofType(String.class);
        resetToDatetimeOpt = parser.accepts("to-datetime", RESET_TO_DATETIME_DOC)
            .withRequiredArg()
            .describedAs("datetime")
            .ofType(String.class);
        resetToEarliestOpt = parser.accepts("to-earliest", RESET_TO_EARLIEST_DOC);
        resetToLatestOpt = parser.accepts("to-latest", RESET_TO_LATEST_DOC);
        resetToCurrentOpt = parser.accepts("to-current", RESET_TO_CURRENT_DOC);
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

        allGroupSelectionScopeOpts = Set.of(groupOpt, allGroupsOpt);
        allTopicSelectionScopeOpts = Set.of(topicOpt, allTopicsOpt);
        allShareGroupLevelOpts = Set.of(listOpt, describeOpt, deleteOpt, resetOffsetsOpt);
        allResetOffsetsScopeOpts = Set.of(topicOpt, allTopicsOpt, resetFromFileOpt);
        allResetOffsetsScenarioOpts = Set.of(resetToOffsetOpt, resetToDatetimeOpt,
            resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt);
        allDeleteOffsetsOpts = Set.of(groupOpt, topicOpt);

        options = parser.parse(args);
    }

    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    public void checkArgs() {
        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list, describe, delete and manage the offsets of share groups.");

        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);

        if (options.has(describeOpt)) {
            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + describeOpt + " takes one of these options: " + allGroupSelectionScopeOpts.stream().map(Object::toString).sorted().collect(Collectors.joining(", ")));
            List<OptionSpec<?>> mutuallyExclusiveOpts = List.of(membersOpt, offsetsOpt, stateOpt);
            if (mutuallyExclusiveOpts.stream().mapToInt(o -> options.has(o) ? 1 : 0).sum() > 1) {
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + describeOpt + " takes at most one of these options: " + mutuallyExclusiveOpts.stream().map(Object::toString).sorted().collect(Collectors.joining(", ")));
            }
            if (options.has(stateOpt) && options.valueOf(stateOpt) != null)
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + describeOpt + " does not take a value for " + stateOpt);
        }

        if (options.has(deleteOpt)) {
            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    String.format("Option %s takes the options %s or %s.", deleteOpt, groupOpt, allGroupsOpt));
            if (options.has(allGroupsOpt) && options.has(groupOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    String.format("Option %s takes either %s or %s, not both.", deleteOpt, groupOpt, allGroupsOpt));
            if (options.has(allTopicsOpt) || options.has(topicOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    String.format("Option %s does not take the options %s or %s.", deleteOpt, topicOpt, allTopicsOpt));
        }

        if (options.has(deleteOffsetsOpt)) {
            if (!options.has(groupOpt) || !options.has(topicOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + deleteOffsetsOpt + " takes the following options: " + allDeleteOffsetsOpts.stream().map(Object::toString).sorted().collect(Collectors.joining(", ")));
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

            if (!options.has(topicOpt) && !options.has(allTopicsOpt) && !options.has(resetFromFileOpt)) {
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + resetOffsetsOpt + " takes one of these options: " + allResetOffsetsScopeOpts.stream().map(Object::toString).sorted().collect(Collectors.joining(", ")));
            }

            CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, minus(allResetOffsetsScopeOpts, topicOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, allTopicsOpt, minus(allResetOffsetsScopeOpts, allTopicsOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetFromFileOpt, minus(allResetOffsetsScopeOpts, resetFromFileOpt));

            if (!options.has(resetToOffsetOpt) && !options.has(resetToEarliestOpt) && !options.has(resetToLatestOpt) && !options.has(resetToDatetimeOpt) && !options.has(resetToCurrentOpt) && !options.has(resetFromFileOpt)) {
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + resetOffsetsOpt + " takes one of these options: " + allResetOffsetsScenarioOpts.stream().map(Object::toString).sorted().collect(Collectors.joining(", ")));
            }

            CommandLineUtils.checkInvalidArgs(parser, options, resetToOffsetOpt, minus(allResetOffsetsScenarioOpts, resetToOffsetOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToDatetimeOpt, minus(allResetOffsetsScenarioOpts, resetToDatetimeOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToEarliestOpt, minus(allResetOffsetsScenarioOpts, resetToEarliestOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToLatestOpt, minus(allResetOffsetsScenarioOpts, resetToLatestOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToCurrentOpt, minus(allResetOffsetsScenarioOpts, resetToCurrentOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetFromFileOpt, minus(allResetOffsetsScenarioOpts, resetFromFileOpt));
        }

        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allGroupSelectionScopeOpts, groupOpt));
        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allShareGroupLevelOpts, deleteOpt, deleteOffsetsOpt, describeOpt, resetOffsetsOpt));
        CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, minus(allShareGroupLevelOpts, deleteOffsetsOpt, resetOffsetsOpt));
    }
}
