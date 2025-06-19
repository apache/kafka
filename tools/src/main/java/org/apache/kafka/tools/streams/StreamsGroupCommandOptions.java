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
    private static final String INPUT_TOPIC_DOC = "The topic whose streams group information should be deleted or topic whose should be included in the reset offset process. " +
        "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
        "Reset-offsets also supports multiple topic inputs. All types of topics are supported.";
    private static final String ALL_INPUT_TOPICS_DOC = "Consider all input topics assigned to a group in the `reset-offsets` and `delete-offsets` process." +
        " Only input topics are supported.";
    public static final String LIST_DOC = "List all streams groups.";
    public static final String DESCRIBE_DOC = "Describe streams group and list offset lag related to given group.";
    private static final String ALL_GROUPS_DOC = "Apply to all streams groups.";
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
    public static final String VERBOSE_DOC = """
        Use with --describe --state  to show group epoch and target assignment epoch.
        Use with --describe --members to show for each member the member epoch, target assignment epoch, current assignment, target assignment, and whether member is still using the classic rebalance protocol.
        Use with --describe --offsets  and --describe  to show leader epochs for each partition.""";
    private static final String DELETE_OFFSETS_DOC = "Delete offsets of streams group. Supports one streams group at the time, and multiple topics.";

    public final OptionSpec<String> bootstrapServerOpt;
    public final OptionSpec<String> groupOpt;
    public final OptionSpec<String> inputTopicOpt;
    public final OptionSpec<Void> allInputTopicsOpt;
    public final OptionSpec<Void> listOpt;
    public final OptionSpec<Void> describeOpt;
    public final OptionSpec<Void> allGroupsOpt;
    public final OptionSpec<Void> deleteOpt;
    public final OptionSpec<Void> deleteOffsetsOpt;
    public final OptionSpec<Long> timeoutMsOpt;
    public final OptionSpec<String> commandConfigOpt;
    public final OptionSpec<String> stateOpt;
    public final OptionSpec<Void> membersOpt;
    public final OptionSpec<Void> offsetsOpt;
    public final OptionSpec<Void> verboseOpt;


    final Set<OptionSpec<?>> allDeleteOffsetsOpts;
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
        deleteOffsetsOpt = parser.accepts("delete-offsets", DELETE_OFFSETS_DOC);
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
        allDeleteOffsetsOpts = new HashSet<>(Arrays.asList(inputTopicOpt, allInputTopicsOpt));
        allStreamsGroupLevelOpts = new HashSet<>(Arrays.asList(listOpt, describeOpt, deleteOpt));
        allGroupSelectionScopeOpts = new HashSet<>(Arrays.asList(groupOpt, allGroupsOpt));
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

        if (options.has(deleteOffsetsOpt)) {
            checkDeleteOffsetsArgs();
        }

        if (options.has(deleteOpt)) {
            if (!options.has(groupOpt) && !options.has(allGroupsOpt))
                CommandLineUtils.printUsageAndExit(parser,
                    "Option " + deleteOpt + " takes one of these options: " + allGroupSelectionScopeOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }

        CommandLineUtils.checkInvalidArgs(parser, options, listOpt, membersOpt, offsetsOpt);
        CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, minus(allStreamsGroupLevelOpts, describeOpt, deleteOpt));
    }

    private void checkDescribeArgs() {
        List<OptionSpec<?>> mutuallyExclusiveOpts = Arrays.asList(membersOpt, offsetsOpt, stateOpt);
        if (mutuallyExclusiveOpts.stream().mapToInt(o -> options.has(o) ? 1 : 0).sum() > 1) {
            CommandLineUtils.printUsageAndExit(parser,
                "Option " + describeOpt + " takes at most one of these options: " + mutuallyExclusiveOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
        }
        if (options.has(stateOpt) && options.valueOf(stateOpt) != null)
            CommandLineUtils.printUsageAndExit(parser,
                "Option " + describeOpt + " does not take a value for " + stateOpt);
    }

    private void checkDeleteOffsetsArgs() {
        if ((!options.has(inputTopicOpt) && !options.has(allInputTopicsOpt)) || !options.has(groupOpt))
            CommandLineUtils.printUsageAndExit(parser,
                "Option " + deleteOffsetsOpt + " takes the " + groupOpt + " and one of these options: " + allDeleteOffsetsOpts.stream().map(Object::toString).collect(Collectors.joining(", ")));
        if (options.valuesOf(groupOpt).size() > 1)
            CommandLineUtils.printUsageAndExit(parser,
                "Option " + deleteOffsetsOpt + " supports only one " + groupOpt + " at a time, but found: " + options.valuesOf(groupOpt));
    }
}
