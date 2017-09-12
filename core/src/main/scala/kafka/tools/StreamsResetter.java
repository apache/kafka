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
package kafka.tools;


import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
import kafka.admin.ConsumerGroupCommand;
import kafka.utils.CommandLineUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.utils.Exit;
import scala.collection.mutable.HashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * {@link StreamsResetter} resets the processing state of a Kafka Streams application so that, for example, you can reprocess its input from scratch.
 * <p>
 * <strong>This class is not part of public API. For backward compatibility, use the provided script in "bin/" instead of calling this class directly from your code.</strong>
 * <p>
 * Resetting the processing state of an application includes the following actions:
 * <ol>
 * <li>setting the application's consumer offsets for input and internal topics to zero</li>
 * <li>skip over all intermediate user topics (i.e., "seekToEnd" for consumers of intermediate topics)</li>
 * <li>deleting any topics created internally by Kafka Streams for this application</li>
 * </ol>
 * <p>
 * Do only use this tool if <strong>no</strong> application instance is running. Otherwise, the application will get into an invalid state and crash or produce wrong results.
 * <p>
 * If you run multiple application instances, running this tool once is sufficient.
 * However, you need to call {@code KafkaStreams#cleanUp()} before re-starting any instance (to clean local state store directory).
 * Otherwise, your application is in an invalid state.
 * <p>
 * User output topics will not be deleted or modified by this tool.
 * If downstream applications consume intermediate or output topics, it is the user's responsibility to adjust those applications manually if required.
 */
@InterfaceStability.Unstable
public class StreamsResetter {
    private static final int EXIT_CODE_SUCCESS = 0;
    private static final int EXIT_CODE_ERROR = 1;

    private static OptionSpec<String> bootstrapServerOption;
    private static OptionSpec<String> applicationIdOption;
    private static OptionSpec<String> inputTopicsOption;
    private static OptionSpec<String> intermediateTopicsOption;
    private static OptionSpec<Long> toOffsetOption;
    private static OptionSpec<String> toDatetimeOption;
    private static OptionSpec<String> byDurationOption;
    private static OptionSpecBuilder toEarliestOption;
    private static OptionSpecBuilder toLatestOption;
    private static OptionSpecBuilder toCurrentOption;
    private static OptionSpec<Long> shiftByOption;
    private static OptionSpecBuilder dryRunOption;

    private OptionSet options = null;
    private final Properties consumerConfig = new Properties();
    private final List<String> allTopics = new LinkedList<>();
    private boolean dryRun = false;
    private String scenario = "to-earliest";

    public int run(final String[] args) {
        return run(args, new Properties());
    }

    public int run(final String[] args, final Properties config) {
        consumerConfig.clear();
        consumerConfig.putAll(config);

        parseArguments(args);

        consumerConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));

        int exitCode = EXIT_CODE_SUCCESS;

        try (AdminClient adminClient = KafkaAdminClient.create(consumerConfig)) {
            dryRun = options.has(dryRunOption);

            allTopics.clear();
            allTopics.addAll(adminClient.listTopics().names().get());

            if (dryRun) {
                System.out.println("----Dry run displays the actions which will be performed when running Streams Reset Tool----");
            }

            maybeResetInputAndSeekToEndIntermediateTopicOffsets();
            maybeDeleteInternalTopics(adminClient);

        } catch (final Throwable e) {
            exitCode = EXIT_CODE_ERROR;
            System.err.println("ERROR: " + e.getMessage());
        }

        return exitCode;
    }

    private void parseArguments(final String[] args) {

        final OptionParser optionParser = new OptionParser(false);
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id).")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("id")
            .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-server", "Comma-separated list of broker urls with format: HOST1:PORT1,HOST2:PORT2")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:9092")
            .describedAs("urls");
        inputTopicsOption = optionParser.accepts("input-topics", "Comma-separated list of user input topics. For these topics, the tool will reset the offset to the earliest available offset.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
        intermediateTopicsOption = optionParser.accepts("intermediate-topics", "Comma-separated list of intermediate user topics (topics used in the through() method). For these topics, the tool will skip to the end.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
        toOffsetOption = optionParser.accepts("to-offset", "Reset offsets to a specific offset.")
                .withRequiredArg()
                .ofType(Long.class);
        toDatetimeOption = optionParser.accepts("to-datetime", "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'")
                .withRequiredArg()
                .ofType(String.class);
        byDurationOption = optionParser.accepts("by-duration", "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'")
                .withRequiredArg()
                .ofType(String.class);
        toEarliestOption = optionParser.accepts("to-earliest", "Reset offsets to earliest offset.");
        toLatestOption = optionParser.accepts("to-latest", "Reset offsets to latest offset.");
        toCurrentOption = optionParser.accepts("to-current", "Reset offsets to current offset.");
        shiftByOption = optionParser.accepts("shift-by", "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative")
                .withRequiredArg()
                .describedAs("number-of-offsets")
                .ofType(Long.class);
        dryRunOption = optionParser.accepts("dry-run", "Display the actions that would be performed without executing the reset commands.");

        try {
            options = optionParser.parse(args);
        } catch (final OptionException e) {
            printHelp(optionParser);
            throw e;
        }

        HashSet<OptionSpec<?>> allScenarioOptions = new HashSet<>();
        allScenarioOptions.add(toOffsetOption);
        allScenarioOptions.add(toDatetimeOption);
        allScenarioOptions.add(byDurationOption);
        allScenarioOptions.add(toEarliestOption);
        allScenarioOptions.add(toLatestOption);
        allScenarioOptions.add(toCurrentOption);
        allScenarioOptions.add(shiftByOption);

        CommandLineUtils.checkInvalidArgs(optionParser, options, toOffsetOption, allScenarioOptions.$minus$eq(toOffsetOption));
        CommandLineUtils.checkInvalidArgs(optionParser, options, toDatetimeOption, allScenarioOptions.$minus$eq(toDatetimeOption));
        CommandLineUtils.checkInvalidArgs(optionParser, options, byDurationOption, allScenarioOptions.$minus$eq(byDurationOption));
        CommandLineUtils.checkInvalidArgs(optionParser, options, toEarliestOption, allScenarioOptions.$minus$eq(toEarliestOption));
        CommandLineUtils.checkInvalidArgs(optionParser, options, toLatestOption, allScenarioOptions.$minus$eq(toLatestOption));
        CommandLineUtils.checkInvalidArgs(optionParser, options, toCurrentOption, allScenarioOptions.$minus$eq(toCurrentOption));
        CommandLineUtils.checkInvalidArgs(optionParser, options, shiftByOption, allScenarioOptions.$minus$eq(shiftByOption));
    }

    private void maybeResetInputAndSeekToEndIntermediateTopicOffsets() {
        final List<String> inputTopics = options.valuesOf(inputTopicsOption);
        final List<String> intermediateTopics = options.valuesOf(intermediateTopicsOption);

        if (inputTopics.size() == 0 && intermediateTopics.size() == 0) {
            System.out.println("No input or intermediate topics specified. Skipping seek.");
            return;
        }

        if (inputTopics.size() != 0) {
            if (options.has(toOffsetOption)) {
                scenario = "to-offset " + options.valueOf(toOffsetOption).toString();
            } else if (options.has(toDatetimeOption)) {
                scenario = "to-datetime " + options.valueOf(toDatetimeOption);
            } else if (options.has(byDurationOption)) {
                scenario = "by-duration " + options.valueOf(byDurationOption);
            } else if (options.has(toLatestOption)) {
                scenario = "to-latest ";
            } else if (options.has(toCurrentOption)) {
                scenario = "to-current ";
            } else if (options.has(toEarliestOption)) {
                scenario = "to-earliest ";
            } else if (options.has(shiftByOption)) {
                scenario = "shift-by " + options.valueOf(shiftByOption);
            }

            System.out.println("Seek-" + scenario + " for input topics " + inputTopics);
        }
        if (intermediateTopics.size() != 0) {
            System.out.println("Seek-to-end for intermediate topics " + intermediateTopics);
        }

        try {

            maybeResetInputTopicPartitions(inputTopics);

            maybeResetIntermediateTopicPartitions(intermediateTopics);

        } catch (final RuntimeException e) {
            System.err.println("ERROR: Resetting offsets failed.");
            throw e;
        }
        System.out.println("Done.");
    }

    private void maybeResetIntermediateTopicPartitions(final List<String> intermediateTopics) {

        final String groupId = options.valueOf(applicationIdOption);
        final String bootstrapServer = consumerConfig.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (intermediateTopics.size() > 0) {
            System.out.println("Following intermediate topics offsets will be reset to end (for consumer group " + groupId + ")");
            StringBuilder intermediateTopicValue = new StringBuilder();
            for (final String topic : intermediateTopics) {
                if (allTopics.contains(topic)) {
                    intermediateTopicValue.append(topic).append(",");
                    System.out.println("Topic: " + topic);
                }
            }
            String intermediateTopicList = intermediateTopicValue.substring(0, intermediateTopicValue.length() - 1);
            if (!dryRun) {
                ConsumerGroupCommand.main(new String[]{"--reset-offsets",
                    "--topic", intermediateTopicList,
                    "--to-latest",
                    "--group", groupId,
                    "--bootstrap-server", bootstrapServer});
            } else {
                ConsumerGroupCommand.main(new String[]{"--reset-offsets",
                    "--topic", intermediateTopicList,
                    "--to-latest",
                    "--group", groupId,
                    "--bootstrap-server", bootstrapServer,
                    "--dry-run"});
            }
        }
    }

    private void maybeResetInputTopicPartitions(final List<String> inputTopics) {

        final String groupId = options.valueOf(applicationIdOption);
        final String bootstrapServer = consumerConfig.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

        if (inputTopics.size() > 0) {
            System.out.println("Following input topics offsets will be reset to beginning (for consumer group " + groupId + ")");
            StringBuilder inputTopicsValue = new StringBuilder();
            for (final String topic : inputTopics) {
                String topicName = topic.split(":")[0];
                if (allTopics.contains(topicName)) {
                    inputTopicsValue.append(topic).append(",");
                    System.out.println("Topic: " + topic);
                }
            }
            String inputTopicList = inputTopicsValue.substring(0, inputTopicsValue.length() - 1);
            if (!dryRun) {
                ConsumerGroupCommand.main(new String[]{"--reset-offsets",
                    "--topic", inputTopicList,
                    "--" + scenario,
                    "--group", groupId,
                    "--bootstrap-server", bootstrapServer});
            } else {
                ConsumerGroupCommand.main(new String[]{"--reset-offsets",
                    "--topic", inputTopicList,
                    "--" + scenario,
                    "--group", groupId,
                    "--bootstrap-server", bootstrapServer,
                    "--dry-run"});
            }
        }
    }

    private void maybeDeleteInternalTopics(AdminClient adminClient) {

        System.out.println("Deleting all internal/auto-created topics for application " + options.valueOf(applicationIdOption));

        List<String> internalTopics = new ArrayList<>();

        for (final String topic : allTopics) {
            if (isInternalTopic(topic)) {
                internalTopics.add(topic);
            }
        }

        if (!dryRun) {
            adminClient.deleteTopics(internalTopics);
        } else {
            for (final String internalTopic : internalTopics) {
                System.out.println("Topic: " + internalTopic);
            }
        }

        System.out.println("Done.");
    }

    private boolean isInternalTopic(final String topicName) {
        return topicName.startsWith(options.valueOf(applicationIdOption) + "-")
            && (topicName.endsWith("-changelog") || topicName.endsWith("-repartition"));
    }

    private void printHelp(OptionParser parser) {
        System.err.println("The Streams Reset Tool allows you to quickly reset an application in order to reprocess "
                + "its data from scratch.\n"
                + "* This tool resets offsets of input topics to the earliest available offset and it skips to the end of "
                + "intermediate topics (topics used in the through() method).\n"
                + "* This tool deletes the internal topics that were created by Kafka Streams (topics starting with "
                + "\"<application.id>-\").\n"
                + "You do not need to specify internal topics because the tool finds them automatically.\n"
                + "* This tool will not delete output topics (if you want to delete them, you need to do it yourself "
                + "with the bin/kafka-topics.sh command).\n"
                + "* This tool will not clean up the local state on the stream application instances (the persisted "
                + "stores used to cache aggregation results).\n"
                + "You need to call KafkaStreams#cleanUp() in your application or manually delete them from the "
                + "directory specified by \"state.dir\" configuration (/tmp/kafka-streams/<application.id> by default).\n\n"
                + "*** Important! You will get wrong output if you don't clean up the local stores after running the "
                + "reset tool!\n\n"
        );
        try {
            parser.printHelpOn(System.err);
        } catch (IOException e) {
            System.err.println("ERROR: " + e.getMessage());
        }
    }

    public static void main(final String[] args) {
        Exit.exit(new StreamsResetter().run(args));
    }

}
