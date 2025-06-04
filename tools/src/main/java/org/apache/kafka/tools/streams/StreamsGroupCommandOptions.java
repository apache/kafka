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
    public static final Logger LOGGER = LoggerFactory.getLogger(StreamsGroupCommandOptions.class);

    public static final String BOOTSTRAP_SERVER_DOC = "REQUIRED: The server(s) to connect to.";
    public static final String GROUP_DOC = "The streams group we wish to act on.";
    public static final String LIST_DOC = "List all streams groups.";
    public static final String DESCRIBE_DOC = "Describe streams group and list offset lag related to given group.";
    private static final String NL = System.lineSeparator();
    private static final String DELETE_DOC = "Pass in groups to delete topic partition offsets and ownership information " +
        "over the entire streams group. For instance --group g1 --group g2. " + NL +
        "Moreover, it can be used to delete all internal topics by passing in a comma-separated list of internal topics which " +
        "must be a subset of the internal topics marked for deletion by the default behaviour. Do a dry-run without this option to view these topics.";
    private static final String INTERNAL_TOPICS_DOC = " Comma-separated list of internal topics to delete. " +
        "This option is only applicable when the --delete option is used.";
    private static final String DRY_RUN_DOC = "Only show results without executing changes on streams groups. Supported operations: delete internal-topics.";
    private static final String EXECUTE_DOC = "Execute operation. Supported operations: delete internal-topics.";
    public static final String TIMEOUT_MS_DOC = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
        "to specify the maximum amount of time in milliseconds to wait before the group stabilizes.";
    public static final String COMMAND_CONFIG_DOC = "Property file containing configs to be passed to Admin Client.";
    public static final String STATE_DOC = "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states. " +
        "Valid values are Empty, NotReady, Stable, Assigning, Reconciling, and Dead.";
    public static final String MEMBERS_DOC = "Describe members of the group. This option may be used with the '--describe' option only.";
    public static final String OFFSETS_DOC = "Describe the group and list all topic partitions in the group along with their offset information." +
        "This is the default sub-action and may be used with the '--describe' option only.";
    public static final String VERBOSE_DOC = """
        Use with --describe --state  to show group epoch and target assignment epoch.
        Use with --describe --members to show for each member the member epoch, target assignment epoch, current assignment, target assignment, and whether member is still using the classic rebalance protocol.
        Use with --describe --offsets  and --describe  to show leader epochs for each partition.""";

    public final OptionSpec<String> bootstrapServerOpt;
    public final OptionSpec<String> groupOpt;
    public final OptionSpec<Void> listOpt;
    public final OptionSpec<Void> describeOpt;
    public final OptionSpec<Void> deleteOpt;
    public final OptionSpec<String> internalTopicsOpt;
    public final OptionSpec<Void> dryRunOpt;
    public final OptionSpec<Void> executeOpt;
    public final OptionSpec<Long> timeoutMsOpt;
    public final OptionSpec<String> commandConfigOpt;
    public final OptionSpec<String> stateOpt;
    public final OptionSpec<Void> membersOpt;
    public final OptionSpec<Void> offsetsOpt;
    public final OptionSpec<Void> verboseOpt;

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
        listOpt = parser.accepts("list", LIST_DOC);
        describeOpt = parser.accepts("describe", DESCRIBE_DOC);
        deleteOpt = parser.accepts("delete", DELETE_DOC);
        dryRunOpt = parser.accepts("dry-run", DRY_RUN_DOC);
        executeOpt = parser.accepts("execute", EXECUTE_DOC);
        internalTopicsOpt = parser.accepts("internal-topics", INTERNAL_TOPICS_DOC)
            .availableIf(deleteOpt)
            .withOptionalArg()
            .describedAs("comma-separated list of internal topics to delete")
            .ofType(String.class);
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
        verboseOpt = parser.accepts("verbose", VERBOSE_DOC)
            .availableIf(describeOpt);

        options = parser.parse(args);
        allStreamsGroupLevelOpts = new HashSet<>(Arrays.asList(listOpt, describeOpt, deleteOpt));
    }

    public void checkArgs() {
        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to list, or describe streams groups.");

        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);

        if (options.has(describeOpt)) {
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

        if (options.has(deleteOpt) && !options.has(internalTopicsOpt)) {
            CommandLineUtils.printUsageAndExit(parser,
                "Option " + deleteOpt + " takes " + internalTopicsOpt + " as an argument to delete internal topics.");
        }

        if (options.has(deleteOpt) && options.has(internalTopicsOpt)) {
            if (options.has(dryRunOpt) && options.has(executeOpt))
                CommandLineUtils.printUsageAndExit(parser, "Option " + internalTopicsOpt + " only accepts one of " + executeOpt + " and " + dryRunOpt);

            if (!options.has(dryRunOpt) && !options.has(executeOpt)) {
                System.err.println("WARN: No action will be performed as the --execute option is missing. " +
                    "In a future major release, the default behavior of this command will be to prompt the user before " +
                    "executing the delete rather than doing a dry run. You should add the --dry-run option explicitly " +
                    "if you are scripting this command and want to keep the current default behavior without prompting.");
            }
        }

        CommandLineUtils.checkInvalidArgs(parser, options, listOpt, membersOpt, offsetsOpt);
        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allStreamsGroupLevelOpts, describeOpt, deleteOpt));
    }
}
