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

public class StreamsGroupCommandOptions extends CommandDefaultOptions {
    private static final String NL = System.lineSeparator();
    public static final Logger LOGGER = LoggerFactory.getLogger(StreamsGroupCommandOptions.class);

    public static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server(s) to connect to.";
    public static final String GROUP_DOC = "The streams group we wish to act on.";
    private static final String ALL_GROUPS_DOC = "Apply to all streams groups.";
    private static final String INPUT_TOPIC_DOC = "The input topic whose streams group information should be deleted or topic that should be included in the reset offset process. " +
        "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
        "Reset-offsets also supports multiple topic inputs.";
    private static final String ALL_INPUT_TOPICS_DOC = "Consider all topics assigned to a group in the `reset-offsets` process.";
    public static final String LIST_DOC = "List all streams groups.";
    public static final String DESCRIBE_DOC = "Describe streams group and list offset lag related to given group.";
    private static final String DELETE_DOC = "Pass in groups to delete topic partition offsets and ownership information " +
        "over the entire streams group. For instance --group g1 --group g2";
    public static final String TIMEOUT_MS_DOC = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
        "to specify the maximum amount of time in milliseconds to wait before the group stabilizes.";
    public static final String COMMAND_CONFIG_DOC = "Property file containing configs to be passed to Admin Client.";
    public static final String STATE_DOC = "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states. " +
        "Valid values are Empty, NotReady, Stable, Assigning, Reconciling, and Dead.";
    public static final String MEMBERS_DOC = "Describe members of the group. This option may be used with the '--describe' option only.";
    public static final String OFFSETS_DOC = "Describe the group and list all topic partitions in the group along with their offset information." +
        "This is the default sub-action and may be used with the '--describe' option only.";
    private static final String RESET_OFFSETS_DOC = "Reset offsets of streams group. The instances should be inactive" + NL +
        "Has 2 execution options: --dry-run (the default) to plan which offsets to reset, and --execute to update the offsets." + NL +
        "You must choose one of the following reset specifications: --to-datetime, --by-duration, --to-earliest, " +
        "--to-latest, --shift-by, --from-file, --to-current, --to-offset." + NL +
        "To define the scope use --all-topics or --topic. One scope must be specified unless you use '--from-file'.";
    private static final String DRY_RUN_DOC = "Only show results without executing changes on streams group. Supported operations: reset-offsets.";
    private static final String EXECUTE_DOC = "Execute operation. Supported operations: reset-offsets.";
    private static final String EXPORT_DOC = "Export operation execution to a CSV file. Supported operations: reset-offsets.";
    private static final String RESET_TO_OFFSET_DOC = "Reset offsets to a specific offset.";
    private static final String RESET_FROM_FILE_DOC = "Reset offsets to values defined in CSV file.";
    private static final String RESET_TO_DATETIME_DOC = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDThh:mm:ss.sss'";
    private static final String RESET_BY_DURATION_DOC = "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'";
    private static final String RESET_TO_EARLIEST_DOC = "Reset offsets to earliest offset.";
    private static final String RESET_TO_LATEST_DOC = "Reset offsets to latest offset.";
    private static final String RESET_TO_CURRENT_DOC = "Reset offsets to current offset.";
    private static final String RESET_SHIFT_BY_DOC = "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative.";
    public static final String VERBOSE_DOC = """
        Use with --describe --state  to show group epoch and target assignment epoch.
        Use with --describe --members to show for each member the member epoch, target assignment epoch, current assignment, target assignment, and whether member is still using the classic rebalance protocol.
        Use with --describe --offsets  and --describe  to show leader epochs for each partition.""";

    public final OptionSpec<String> bootstrapServerOpt;
    public final OptionSpec<String> groupOpt;
    public final OptionSpec<String> inputTopicOpt;
    public final OptionSpec<Void> allInputTopicsOpt;
    public final OptionSpec<Void> listOpt;
    public final OptionSpec<Void> describeOpt;
    public final OptionSpec<Void> deleteOpt;
    public final OptionSpec<Void> allGroupsOpt;
    public final OptionSpec<Long> timeoutMsOpt;
    public final OptionSpec<String> commandConfigOpt;
    public final OptionSpec<String> stateOpt;
    public final OptionSpec<Void> membersOpt;
    public final OptionSpec<Void> offsetsOpt;
    public final OptionSpec<Void> resetOffsetsOpt;
    public final OptionSpec<Long> resetToOffsetOpt;
    public final OptionSpec<String> resetFromFileOpt;
    public final OptionSpec<String> resetToDatetimeOpt;
    public final OptionSpec<String> resetByDurationOpt;
    public final OptionSpec<Void> resetToEarliestOpt;
    public final OptionSpec<Void> resetToLatestOpt;
    public final OptionSpec<Void> resetToCurrentOpt;
    public final OptionSpec<Long> resetShiftByOpt;
    public final OptionSpec<Void> dryRunOpt;
    public final OptionSpec<Void> executeOpt;
    public final OptionSpec<Void> exportOpt;
    public final OptionSpec<Void> verboseOpt;

    final Set<OptionSpec<?>> allResetOffsetScenarioOpts;
    final Set<OptionSpec<?>> allGroupSelectionScopeOpts;
    final Set<OptionSpec<?>> allStreamsGroupLevelOpts;

    public static StreamsGroupCommandOptions fromArgs(String[] args) {
        StreamsGroupCommandOptions opts = new StreamsGroupCommandOptions(args);
        opts.checkArgs();
        return opts;
    }

    public StreamsGroupCommandOptions(String[] args) {
        super(args);

        bootstrapServerOpt = parser.accepts("bootstrap-server", BOOTSTRAP_SERVER_DOC)
            .withRequiredArg()
            .describedAs("server to connect to")
            .ofType(String.class);
        groupOpt = parser.accepts("group", GROUP_DOC)
            .withRequiredArg()
            .describedAs("streams group")
            .ofType(String.class);
        inputTopicOpt = parser.accepts("input-topic", INPUT_TOPIC_DOC)
            .withRequiredArg()
            .describedAs("topic")
            .ofType(String.class);
        allInputTopicsOpt = parser.accepts("all-input-topics", ALL_INPUT_TOPICS_DOC);
        listOpt = parser.accepts("list", LIST_DOC);
        describeOpt = parser.accepts("describe", DESCRIBE_DOC);
        allGroupsOpt = parser.accepts("all-groups", ALL_GROUPS_DOC);
        deleteOpt = parser.accepts("delete", DELETE_DOC);
        timeoutMsOpt = parser.accepts("timeout", TIMEOUT_MS_DOC)
            .availableIf(describeOpt)
            .withRequiredArg()
            .describedAs("timeout (ms)")
            .ofType(Long.class)
            .defaultsTo(5000L);
        commandConfigOpt = parser.accepts("command-config", COMMAND_CONFIG_DOC)
            .withRequiredArg()
            .describedAs("command config property file")
            .ofType(String.class);

        stateOpt = parser.accepts("state", STATE_DOC)
            .availableIf(listOpt, describeOpt)
            .withOptionalArg()
            .ofType(String.class);
        membersOpt = parser.accepts("members", MEMBERS_DOC)
            .availableIf(describeOpt);
        offsetsOpt = parser.accepts("offsets", OFFSETS_DOC)
            .availableIf(describeOpt);
        resetOffsetsOpt = parser.accepts("reset-offsets", RESET_OFFSETS_DOC);
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

        verboseOpt = parser.accepts("verbose", VERBOSE_DOC)
            .availableIf(describeOpt);
        dryRunOpt = parser.accepts("dry-run", DRY_RUN_DOC);
        executeOpt = parser.accepts("execute", EXECUTE_DOC);
        exportOpt = parser.accepts("export", EXPORT_DOC);
        options = parser.parse(args);

        allResetOffsetScenarioOpts = new HashSet<>(Arrays.asList(resetToOffsetOpt, resetShiftByOpt,
            resetToDatetimeOpt, resetByDurationOpt, resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt));
        allGroupSelectionScopeOpts = new HashSet<>(Arrays.asList(groupOpt, allGroupsOpt));
        allStreamsGroupLevelOpts = new HashSet<>(Arrays.asList(listOpt, describeOpt, deleteOpt));
    }

    public void checkArgs() {
        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list, or describe streams groups.");

        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);

        if (options.has(describeOpt)) {
            checkDescribeArgs();
        } else {
            if (options.has(timeoutMsOpt))
                LOGGER.debug("Option " + timeoutMsOpt + " is applicable only when " + describeOpt + " is used.");
        }

        if (options.has(deleteOpt)) {
            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + deleteOpt + " takes one of these options: " + allGroupSelectionScopeOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }

        if (options.has(resetOffsetsOpt)) {
            checkOffsetResetArgs();
        }

        if ((options.has(dryRunOpt) || options.has(executeOpt)) && !options.has(resetOffsetsOpt))
            CommandLineUtils.printUsageAndExit(parser, "Only Option " + resetOffsetsOpt + "accepts " + executeOpt + " or " + dryRunOpt);

        CommandLineUtils.checkInvalidArgs(parser, options, listOpt, membersOpt, offsetsOpt);
        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allStreamsGroupLevelOpts, describeOpt, deleteOpt));
    }

    private void checkDescribeArgs() {
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
    private void checkOffsetResetArgs() {
        if (options.has(dryRunOpt) && options.has(executeOpt))
            CommandLineUtils.printUsageAndExit(parser, "Option " + resetOffsetsOpt + " only accepts one of " + executeOpt + " and " + dryRunOpt);

        if (!options.has(dryRunOpt) && !options.has(executeOpt)) {
            System.err.println("WARN: No action will be performed as the --execute option is missing. " +
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
}