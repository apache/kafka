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

public class ConsumerGroupCommandOptions extends CommandDefaultOptions {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupCommandOptions.class);

    private static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server(s) to connect to.";
    private static final String GROUP_DOC = "The consumer group we wish to act on.";
    private static final String TOPIC_DOC = "The topic whose consumer group information should be deleted or topic whose should be included in the reset offset process. " +
        "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
        "Reset-offsets also supports multiple topic inputs.";
    private static final String ALL_TOPICS_DOC = "Consider all topics assigned to a group in the `reset-offsets` process.";
    private static final String LIST_DOC = "List all consumer groups.";
    private static final String DESCRIBE_DOC = "Describe consumer group and list offset lag (number of messages not yet processed) related to given group.";
    private static final String ALL_GROUPS_DOC = "Apply to all consumer groups.";
    private static final String NL = System.lineSeparator();
    private static final String DELETE_DOC = "Pass in groups to delete topic partition offsets and ownership information " +
        "over the entire consumer group. For instance --group g1 --group g2";
    private static final String TIMEOUT_MS_DOC = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
        "to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, " +
        "or is going through some changes).";
    private static final String COMMAND_CONFIG_DOC = "Property file containing configs to be passed to Admin Client and Consumer.";
    private static final String RESET_OFFSETS_DOC = "Reset offsets of consumer group. Supports one consumer group at the time, and instances should be inactive" + NL +
        "Has 2 execution options: --dry-run (the default) to plan which offsets to reset, and --execute to update the offsets. " +
        "Additionally, the --export option is used to export the results to a CSV format." + NL +
        "You must choose one of the following reset specifications: --to-datetime, --by-duration, --to-earliest, " +
        "--to-latest, --shift-by, --from-file, --to-current, --to-offset." + NL +
        "To define the scope use --all-topics or --topic. One scope must be specified unless you use '--from-file'.";
    private static final String DRY_RUN_DOC = "Only show results without executing changes on Consumer Groups. Supported operations: reset-offsets.";
    private static final String EXECUTE_DOC = "Execute operation. Supported operations: reset-offsets.";
    private static final String EXPORT_DOC = "Export operation execution to a CSV file. Supported operations: reset-offsets.";
    private static final String RESET_TO_OFFSET_DOC = "Reset offsets to a specific offset.";
    private static final String RESET_FROM_FILE_DOC = "Reset offsets to values defined in CSV file.";
    private static final String RESET_TO_DATETIME_DOC = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'";
    private static final String RESET_BY_DURATION_DOC = "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'";
    private static final String RESET_TO_EARLIEST_DOC = "Reset offsets to earliest offset.";
    private static final String RESET_TO_LATEST_DOC = "Reset offsets to latest offset.";
    private static final String RESET_TO_CURRENT_DOC = "Reset offsets to current offset.";
    private static final String RESET_SHIFT_BY_DOC = "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative.";
    private static final String MEMBERS_DOC = "Describe members of the group. This option may be used with '--describe' and '--bootstrap-server' options only." + NL +
        "Example: --bootstrap-server localhost:9092 --describe --group group1 --members";
    private static final String VERBOSE_DOC = "Provide additional information, if any, when describing the group. This option may be used " +
        "with '--offsets'/'--members'/'--state' and '--bootstrap-server' options only." + NL + "Example: --bootstrap-server localhost:9092 --describe --group group1 --members --verbose";
    private static final String OFFSETS_DOC = "Describe the group and list all topic partitions in the group along with their offset lag. " +
        "This is the default sub-action of and may be used with '--describe' and '--bootstrap-server' options only." + NL +
        "Example: --bootstrap-server localhost:9092 --describe --group group1 --offsets";
    private static final String STATE_DOC = "When specified with '--describe', includes the state of the group." + NL +
        "Example: --bootstrap-server localhost:9092 --describe --group group1 --state" + NL +
        "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states." + NL +
        "Example: --bootstrap-server localhost:9092 --list --state stable,empty" + NL +
        "This option may be used with '--describe', '--list' and '--bootstrap-server' options only.";
    private static final String TYPE_DOC = "When specified with '--list', it displays the types of all the groups. It can also be used to list groups with specific types." + NL +
        "Example: --bootstrap-server localhost:9092 --list --type classic,consumer" + NL +
        "This option may be used with the '--list' option only.";
    private static final String DELETE_OFFSETS_DOC = "Delete offsets of consumer group. Supports one consumer group at the time, and multiple topics.";

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
    final OptionSpec<Void> exportOpt;
    final OptionSpec<Long> resetToOffsetOpt;
    final OptionSpec<String> resetFromFileOpt;
    final OptionSpec<String> resetToDatetimeOpt;
    final OptionSpec<String> resetByDurationOpt;
    final OptionSpec<Void> resetToEarliestOpt;
    final OptionSpec<Void> resetToLatestOpt;
    final OptionSpec<Void> resetToCurrentOpt;
    final OptionSpec<Long> resetShiftByOpt;
    final OptionSpec<Void> membersOpt;
    final OptionSpec<Void> verboseOpt;
    final OptionSpec<Void> offsetsOpt;
    final OptionSpec<String> stateOpt;
    final OptionSpec<String> typeOpt;

    final Set<OptionSpec<?>> allGroupSelectionScopeOpts;
    final Set<OptionSpec<?>> allConsumerGroupLevelOpts;
    final Set<OptionSpec<?>> allResetOffsetScenarioOpts;
    final Set<OptionSpec<?>> allDeleteOffsetsOpts;

    public static ConsumerGroupCommandOptions fromArgs(String[] args) {
        ConsumerGroupCommandOptions opts = new ConsumerGroupCommandOptions(args);
        opts.checkArgs();
        return opts;
    }

    private ConsumerGroupCommandOptions(String[] args) {
        super(args);

        bootstrapServerOpt = parser.accepts("bootstrap-server", BOOTSTRAP_SERVER_DOC)
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        groupOpt = parser.accepts("group", GROUP_DOC)
            .withRequiredArg()
            .describedAs("consumer group")
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
        resetByDurationOpt = parser.accepts("by-duration", RESET_BY_DURATION_DOC)
            .withRequiredArg()
            .describedAs("duration")
            .ofType(String.class);
        resetToEarliestOpt = parser.accepts("to-earliest", RESET_TO_EARLIEST_DOC);
        resetToLatestOpt = parser.accepts("to-latest", RESET_TO_LATEST_DOC);
        resetToCurrentOpt = parser.accepts("to-current", RESET_TO_CURRENT_DOC);
        resetShiftByOpt = parser.accepts("shift-by", RESET_SHIFT_BY_DOC)
            .withRequiredArg()
            .describedAs("number-of-offsets")
            .ofType(Long.class);
        membersOpt = parser.accepts("members", MEMBERS_DOC)
            .availableIf(describeOpt);
        verboseOpt = parser.accepts("verbose", VERBOSE_DOC)
            .availableIf(describeOpt);
        offsetsOpt = parser.accepts("offsets", OFFSETS_DOC)
            .availableIf(describeOpt);
        stateOpt = parser.accepts("state", STATE_DOC)
            .availableIf(describeOpt, listOpt)
            .withOptionalArg()
            .ofType(String.class);
        typeOpt = parser.accepts("type", TYPE_DOC)
            .availableIf(listOpt)
            .withOptionalArg()
            .ofType(String.class);

        allGroupSelectionScopeOpts = new HashSet<>(Arrays.asList(groupOpt, allGroupsOpt));
        allConsumerGroupLevelOpts = new HashSet<>(Arrays.asList(listOpt, describeOpt, deleteOpt, resetOffsetsOpt));
        allResetOffsetScenarioOpts = new HashSet<>(Arrays.asList(resetToOffsetOpt, resetShiftByOpt,
            resetToDatetimeOpt, resetByDurationOpt, resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt));
        allDeleteOffsetsOpts = new HashSet<>(Arrays.asList(groupOpt, topicOpt));

        options = parser.parse(args);
    }

    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    void checkArgs() {
        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list all consumer groups, describe a consumer group, delete consumer group info, or reset consumer group offsets.");

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
        } else {
            if (options.has(timeoutMsOpt))
                LOGGER.debug("Option " + timeoutMsOpt + " is applicable only when " + describeOpt + " is used.");
        }

        if (options.has(deleteOpt)) {
            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
            "Option " + deleteOpt + " takes one of these options: " + allGroupSelectionScopeOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
            if (options.has(topicOpt))
                CommandLineUtils.printUsageAndExit(parser, "The consumer does not support topic-specific offset " +
                    "deletion from a consumer group.");
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
                System.err.println("WARN: No action will be performed as the --execute option is missing." +
                    "In a future major release, the default behavior of this command will be to prompt the user before " +
                    "executing the reset rather than doing a dry run. You should add the --dry-run option explicitly " +
                    "if you are scripting this command and want to keep the current default behavior without prompting.");
            }

            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + resetOffsetsOpt + " takes one of these options: " + allGroupSelectionScopeOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToOffsetOpt, minus(allResetOffsetScenarioOpts, resetToOffsetOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToDatetimeOpt, minus(allResetOffsetScenarioOpts, resetToDatetimeOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetByDurationOpt, minus(allResetOffsetScenarioOpts, resetByDurationOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToEarliestOpt, minus(allResetOffsetScenarioOpts, resetToEarliestOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToLatestOpt, minus(allResetOffsetScenarioOpts, resetToLatestOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetToCurrentOpt, minus(allResetOffsetScenarioOpts, resetToCurrentOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetShiftByOpt, minus(allResetOffsetScenarioOpts, resetShiftByOpt));
            CommandLineUtils.checkInvalidArgs(parser, options, resetFromFileOpt, minus(allResetOffsetScenarioOpts, resetFromFileOpt));
        }

        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allGroupSelectionScopeOpts, groupOpt));
        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allConsumerGroupLevelOpts, describeOpt, deleteOpt, resetOffsetsOpt));
        CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, minus(allConsumerGroupLevelOpts, deleteOpt, resetOffsetsOpt));
    }
}
