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
import kafka.utils.CommandLineUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.requests.ListOffsetsResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@link StreamsResetter} resets the processing state of a Kafka Streams application so that, for example,
 * you can reprocess its input from scratch.
 * <p>
 * <strong>This class is not part of public API. For backward compatibility,
 * use the provided script in "bin/" instead of calling this class directly from your code.</strong>
 * <p>
 * Resetting the processing state of an application includes the following actions:
 * <ol>
 * <li>setting the application's consumer offsets for input and internal topics to zero</li>
 * <li>skip over all intermediate user topics (i.e., "seekToEnd" for consumers of intermediate topics)</li>
 * <li>deleting any topics created internally by Kafka Streams for this application</li>
 * </ol>
 * <p>
 * Do only use this tool if <strong>no</strong> application instance is running.
 * Otherwise, the application will get into an invalid state and crash or produce wrong results.
 * <p>
 * If you run multiple application instances, running this tool once is sufficient.
 * However, you need to call {@code KafkaStreams#cleanUp()} before re-starting any instance
 * (to clean local state store directory).
 * Otherwise, your application is in an invalid state.
 * <p>
 * User output topics will not be deleted or modified by this tool.
 * If downstream applications consume intermediate or output topics,
 * it is the user's responsibility to adjust those applications manually if required.
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
    private static OptionSpec<String> fromFileOption;
    private static OptionSpec<Long> shiftByOption;
    private static OptionSpecBuilder dryRunOption;
    private static OptionSpec<Void> helpOption;
    private static OptionSpec<Void> versionOption;
    private static OptionSpec<String> commandConfigOption;
    private static OptionSpecBuilder forceOption;

    private final static String USAGE = "This tool helps to quickly reset an application in order to reprocess "
            + "its data from scratch.\n"
            + "* This tool resets offsets of input topics to the earliest available offset and it skips to the end of "
            + "intermediate topics (topics that are input and output topics, e.g., used by deprecated through() method).\n"
            + "* This tool deletes the internal topics that were created by Kafka Streams (topics starting with "
            + "\"<application.id>-\").\n"
            + "You do not need to specify internal topics because the tool finds them automatically.\n"
            + "* This tool will not delete output topics (if you want to delete them, you need to do it yourself "
            + "with the bin/kafka-topics.sh command).\n"
            + "* This tool will not clean up the local state on the stream application instances (the persisted "
            + "stores used to cache aggregation results).\n"
            + "You need to call KafkaStreams#cleanUp() in your application or manually delete them from the "
            + "directory specified by \"state.dir\" configuration (${java.io.tmpdir}/kafka-streams/<application.id> by default).\n"
            + "* When long session timeout has been configured, active members could take longer to get expired on the "
            + "broker thus blocking the reset job to complete. Use the \"--force\" option could remove those left-over "
            + "members immediately. Make sure to stop all stream applications when this option is specified "
            + "to avoid unexpected disruptions.\n\n"
            + "*** Important! You will get wrong output if you don't clean up the local stores after running the "
            + "reset tool!\n\n";

    private OptionSet options = null;
    private final List<String> allTopics = new LinkedList<>();


    public int run(final String[] args) {
        return run(args, new Properties());
    }

    public int run(final String[] args,
                   final Properties config) {
        int exitCode;

        Admin adminClient = null;
        try {
            parseArguments(args);

            final boolean dryRun = options.has(dryRunOption);

            final String groupId = options.valueOf(applicationIdOption);
            final Properties properties = new Properties();
            if (options.has(commandConfigOption)) {
                properties.putAll(Utils.loadProps(options.valueOf(commandConfigOption)));
            }
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));

            adminClient = Admin.create(properties);
            maybeDeleteActiveConsumers(groupId, adminClient);

            allTopics.clear();
            allTopics.addAll(adminClient.listTopics().names().get(60, TimeUnit.SECONDS));

            if (dryRun) {
                System.out.println("----Dry run displays the actions which will be performed when running Streams Reset Tool----");
            }

            final HashMap<Object, Object> consumerConfig = new HashMap<>(config);
            consumerConfig.putAll(properties);
            exitCode = maybeResetInputAndSeekToEndIntermediateTopicOffsets(consumerConfig, dryRun);
            maybeDeleteInternalTopics(adminClient, dryRun);
        } catch (final Throwable e) {
            exitCode = EXIT_CODE_ERROR;
            System.err.println("ERROR: " + e);
            e.printStackTrace(System.err);
        } finally {
            if (adminClient != null) {
                adminClient.close(Duration.ofSeconds(60));
            }
        }

        return exitCode;
    }

    private void maybeDeleteActiveConsumers(final String groupId,
                                            final Admin adminClient)
        throws ExecutionException, InterruptedException {

        final DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(10 * 1000));
        final List<MemberDescription> members =
            new ArrayList<>(describeResult.describedGroups().get(groupId).get().members());
        if (!members.isEmpty()) {
            if (options.has(forceOption)) {
                System.out.println("Force deleting all active members in the group: " + groupId);
                adminClient.removeMembersFromConsumerGroup(groupId, new RemoveMembersFromConsumerGroupOptions()).all().get();
            } else {
                throw new IllegalStateException("Consumer group '" + groupId + "' is still active "
                        + "and has following members: " + members + ". "
                        + "Make sure to stop all running application instances before running the reset tool."
                        + " You can use option '--force' to remove active members from the group.");
            }
        }
    }

    private void parseArguments(final String[] args) {
        final OptionParser optionParser = new OptionParser(false);
        applicationIdOption = optionParser.accepts("application-id", "The Kafka Streams application ID (application.id).")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("id")
            .required();
        bootstrapServerOption = optionParser.accepts("bootstrap-servers", "Comma-separated list of broker urls with format: HOST1:PORT1,HOST2:PORT2")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("localhost:9092")
            .describedAs("urls");
        inputTopicsOption = optionParser.accepts("input-topics", "Comma-separated list of user input topics. For these topics, the tool will reset the offset to the earliest available offset.")
            .withRequiredArg()
            .ofType(String.class)
            .withValuesSeparatedBy(',')
            .describedAs("list");
        intermediateTopicsOption = optionParser.accepts("intermediate-topics", "Comma-separated list of intermediate user topics (topics that are input and output topics, e.g., used in the deprecated through() method). For these topics, the tool will skip to the end.")
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
        fromFileOption = optionParser.accepts("from-file", "Reset offsets to values defined in CSV file.")
            .withRequiredArg()
            .ofType(String.class);
        shiftByOption = optionParser.accepts("shift-by", "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative")
            .withRequiredArg()
            .describedAs("number-of-offsets")
            .ofType(Long.class);
        commandConfigOption = optionParser.accepts("config-file", "Property file containing configs to be passed to admin clients and embedded consumer.")
            .withRequiredArg()
            .ofType(String.class)
            .describedAs("file name");
        forceOption = optionParser.accepts("force", "Force the removal of members of the consumer group (intended to remove stopped members if a long session timeout was used). " +
                "Make sure to shut down all stream applications when this option is specified to avoid unexpected rebalances.");

        dryRunOption = optionParser.accepts("dry-run", "Display the actions that would be performed without executing the reset commands.");
        helpOption = optionParser.accepts("help", "Print usage information.").forHelp();
        versionOption = optionParser.accepts("version", "Print version information and exit.").forHelp();

        try {
            options = optionParser.parse(args);
            if (args.length == 0 || options.has(helpOption)) {
                CommandLineUtils.printUsageAndDie(optionParser, USAGE);
            }
            if (options.has(versionOption)) {
                CommandLineUtils.printVersionAndDie();
            }
        } catch (final OptionException e) {
            CommandLineUtils.printUsageAndDie(optionParser, e.getMessage());
        }

        final Set<OptionSpec<?>> allScenarioOptions = new HashSet<>();
        allScenarioOptions.add(toOffsetOption);
        allScenarioOptions.add(toDatetimeOption);
        allScenarioOptions.add(byDurationOption);
        allScenarioOptions.add(toEarliestOption);
        allScenarioOptions.add(toLatestOption);
        allScenarioOptions.add(fromFileOption);
        allScenarioOptions.add(shiftByOption);

        checkInvalidArgs(optionParser, options, allScenarioOptions, toOffsetOption);
        checkInvalidArgs(optionParser, options, allScenarioOptions, toDatetimeOption);
        checkInvalidArgs(optionParser, options, allScenarioOptions, byDurationOption);
        checkInvalidArgs(optionParser, options, allScenarioOptions, toEarliestOption);
        checkInvalidArgs(optionParser, options, allScenarioOptions, toLatestOption);
        checkInvalidArgs(optionParser, options, allScenarioOptions, fromFileOption);
        checkInvalidArgs(optionParser, options, allScenarioOptions, shiftByOption);
    }

    private <T> void checkInvalidArgs(final OptionParser optionParser,
                                      final OptionSet options,
                                      final Set<OptionSpec<?>> allOptions,
                                      final OptionSpec<T> option) {
        final Set<OptionSpec<?>> invalidOptions = new HashSet<>(allOptions);
        invalidOptions.remove(option);
        CommandLineUtils.checkInvalidArgs(
            optionParser,
            options,
            option,
            JavaConverters.asScalaSetConverter(invalidOptions).asScala());
    }

    private int maybeResetInputAndSeekToEndIntermediateTopicOffsets(final Map<Object, Object> consumerConfig,
                                                                    final boolean dryRun)
        throws IOException, ParseException {

        final List<String> inputTopics = options.valuesOf(inputTopicsOption);
        final List<String> intermediateTopics = options.valuesOf(intermediateTopicsOption);
        int topicNotFound = EXIT_CODE_SUCCESS;

        final List<String> notFoundInputTopics = new ArrayList<>();
        final List<String> notFoundIntermediateTopics = new ArrayList<>();

        final String groupId = options.valueOf(applicationIdOption);

        if (inputTopics.size() == 0 && intermediateTopics.size() == 0) {
            System.out.println("No input or intermediate topics specified. Skipping seek.");
            return EXIT_CODE_SUCCESS;
        }

        if (inputTopics.size() != 0) {
            System.out.println("Reset-offsets for input topics " + inputTopics);
        }
        if (intermediateTopics.size() != 0) {
            System.out.println("Seek-to-end for intermediate topics " + intermediateTopics);
        }

        final Set<String> topicsToSubscribe = new HashSet<>(inputTopics.size() + intermediateTopics.size());

        for (final String topic : inputTopics) {
            if (!allTopics.contains(topic)) {
                notFoundInputTopics.add(topic);
            } else {
                topicsToSubscribe.add(topic);
            }
        }
        for (final String topic : intermediateTopics) {
            if (!allTopics.contains(topic)) {
                notFoundIntermediateTopics.add(topic);
            } else {
                topicsToSubscribe.add(topic);
            }
        }

        if (!notFoundInputTopics.isEmpty()) {
            System.out.println("Following input topics are not found, skipping them");
            for (final String topic : notFoundInputTopics) {
                System.out.println("Topic: " + topic);
            }
            topicNotFound = EXIT_CODE_ERROR;
        }

        if (!notFoundIntermediateTopics.isEmpty()) {
            System.out.println("Following intermediate topics are not found, skipping them");
            for (final String topic : notFoundIntermediateTopics) {
                System.out.println("Topic:" + topic);
            }
            topicNotFound = EXIT_CODE_ERROR;
        }

        // Return early if there are no topics to reset (the consumer will raise an error if we
        // try to poll with an empty subscription)
        if (topicsToSubscribe.isEmpty()) {
            return topicNotFound;
        }

        final Properties config = new Properties();
        config.putAll(consumerConfig);
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (final KafkaConsumer<byte[], byte[]> client =
                 new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {

            final Collection<TopicPartition> partitions = topicsToSubscribe.stream().map(client::partitionsFor)
                    .flatMap(Collection::stream)
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .collect(Collectors.toList());
            client.assign(partitions);

            final Set<TopicPartition> inputTopicPartitions = new HashSet<>();
            final Set<TopicPartition> intermediateTopicPartitions = new HashSet<>();

            for (final TopicPartition p : partitions) {
                final String topic = p.topic();
                if (isInputTopic(topic)) {
                    inputTopicPartitions.add(p);
                } else if (isIntermediateTopic(topic)) {
                    intermediateTopicPartitions.add(p);
                } else {
                    System.err.println("Skipping invalid partition: " + p);
                }
            }

            maybeReset(groupId, client, inputTopicPartitions);

            maybeSeekToEnd(groupId, client, intermediateTopicPartitions);

            if (!dryRun) {
                for (final TopicPartition p : partitions) {
                    client.position(p);
                }
                client.commitSync();
            }
        } catch (final IOException | ParseException e) {
            System.err.println("ERROR: Resetting offsets failed.");
            throw e;
        }
        System.out.println("Done.");
        return topicNotFound;
    }

    // visible for testing
    public void maybeSeekToEnd(final String groupId,
                               final Consumer<byte[], byte[]> client,
                               final Set<TopicPartition> intermediateTopicPartitions) {
        if (intermediateTopicPartitions.size() > 0) {
            System.out.println("Following intermediate topics offsets will be reset to end (for consumer group " + groupId + ")");
            for (final TopicPartition topicPartition : intermediateTopicPartitions) {
                if (allTopics.contains(topicPartition.topic())) {
                    System.out.println("Topic: " + topicPartition.topic());
                }
            }
            client.seekToEnd(intermediateTopicPartitions);
        }
    }

    private void maybeReset(final String groupId,
                            final Consumer<byte[], byte[]> client,
                            final Set<TopicPartition> inputTopicPartitions)
        throws IOException, ParseException {

        if (inputTopicPartitions.size() > 0) {
            System.out.println("Following input topics offsets will be reset to (for consumer group " + groupId + ")");
            if (options.has(toOffsetOption)) {
                resetOffsetsTo(client, inputTopicPartitions, options.valueOf(toOffsetOption));
            } else if (options.has(toEarliestOption)) {
                client.seekToBeginning(inputTopicPartitions);
            } else if (options.has(toLatestOption)) {
                client.seekToEnd(inputTopicPartitions);
            } else if (options.has(shiftByOption)) {
                shiftOffsetsBy(client, inputTopicPartitions, options.valueOf(shiftByOption));
            } else if (options.has(toDatetimeOption)) {
                final String ts = options.valueOf(toDatetimeOption);
                final long timestamp = Utils.getDateTime(ts);
                resetToDatetime(client, inputTopicPartitions, timestamp);
            } else if (options.has(byDurationOption)) {
                final String duration = options.valueOf(byDurationOption);
                resetByDuration(client, inputTopicPartitions, Duration.parse(duration));
            } else if (options.has(fromFileOption)) {
                final String resetPlanPath = options.valueOf(fromFileOption);
                final Map<TopicPartition, Long> topicPartitionsAndOffset =
                    getTopicPartitionOffsetFromResetPlan(resetPlanPath);
                resetOffsetsFromResetPlan(client, inputTopicPartitions, topicPartitionsAndOffset);
            } else {
                client.seekToBeginning(inputTopicPartitions);
            }

            for (final TopicPartition p : inputTopicPartitions) {
                System.out.println("Topic: " + p.topic() + " Partition: " + p.partition() + " Offset: " + client.position(p));
            }
        }
    }

    // visible for testing
    public void resetOffsetsFromResetPlan(final Consumer<byte[], byte[]> client,
                                          final Set<TopicPartition> inputTopicPartitions,
                                          final Map<TopicPartition, Long> topicPartitionsAndOffset) {
        final Map<TopicPartition, Long> endOffsets = client.endOffsets(inputTopicPartitions);
        final Map<TopicPartition, Long> beginningOffsets = client.beginningOffsets(inputTopicPartitions);

        final Map<TopicPartition, Long> validatedTopicPartitionsAndOffset =
            checkOffsetRange(topicPartitionsAndOffset, beginningOffsets, endOffsets);

        for (final TopicPartition topicPartition : inputTopicPartitions) {
            client.seek(topicPartition, validatedTopicPartitionsAndOffset.get(topicPartition));
        }
    }

    private Map<TopicPartition, Long> getTopicPartitionOffsetFromResetPlan(final String resetPlanPath)
        throws IOException, ParseException {

        final String resetPlanCsv = Utils.readFileAsString(resetPlanPath);
        return parseResetPlan(resetPlanCsv);
    }

    private void resetByDuration(final Consumer<byte[], byte[]> client,
                                 final Set<TopicPartition> inputTopicPartitions,
                                 final Duration duration) {
        resetToDatetime(client, inputTopicPartitions, Instant.now().minus(duration).toEpochMilli());
    }

    // visible for testing
    public void resetToDatetime(final Consumer<byte[], byte[]> client,
                                final Set<TopicPartition> inputTopicPartitions,
                                final Long timestamp) {
        final Map<TopicPartition, Long> topicPartitionsAndTimes = new HashMap<>(inputTopicPartitions.size());
        for (final TopicPartition topicPartition : inputTopicPartitions) {
            topicPartitionsAndTimes.put(topicPartition, timestamp);
        }

        final Map<TopicPartition, OffsetAndTimestamp> topicPartitionsAndOffset = client.offsetsForTimes(topicPartitionsAndTimes);

        for (final TopicPartition topicPartition : inputTopicPartitions) {
            final Optional<Long> partitionOffset = Optional.ofNullable(topicPartitionsAndOffset.get(topicPartition))
                    .map(OffsetAndTimestamp::offset)
                    .filter(offset -> offset != ListOffsetsResponse.UNKNOWN_OFFSET);
            if (partitionOffset.isPresent()) {
                client.seek(topicPartition, partitionOffset.get());
            } else {
                client.seekToEnd(Collections.singletonList(topicPartition));
                System.out.println("Partition " + topicPartition.partition() + " from topic " + topicPartition.topic() +
                        " is empty, without a committed record. Falling back to latest known offset.");
            }
        }
    }

    // visible for testing
    public void shiftOffsetsBy(final Consumer<byte[], byte[]> client,
                               final Set<TopicPartition> inputTopicPartitions,
                               final long shiftBy) {
        final Map<TopicPartition, Long> endOffsets = client.endOffsets(inputTopicPartitions);
        final Map<TopicPartition, Long> beginningOffsets = client.beginningOffsets(inputTopicPartitions);

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>(inputTopicPartitions.size());
        for (final TopicPartition topicPartition : inputTopicPartitions) {
            final long position = client.position(topicPartition);
            final long offset = position + shiftBy;
            topicPartitionsAndOffset.put(topicPartition, offset);
        }

        final Map<TopicPartition, Long> validatedTopicPartitionsAndOffset =
            checkOffsetRange(topicPartitionsAndOffset, beginningOffsets, endOffsets);

        for (final TopicPartition topicPartition : inputTopicPartitions) {
            client.seek(topicPartition, validatedTopicPartitionsAndOffset.get(topicPartition));
        }
    }

    // visible for testing
    public void resetOffsetsTo(final Consumer<byte[], byte[]> client,
                               final Set<TopicPartition> inputTopicPartitions,
                               final Long offset) {
        final Map<TopicPartition, Long> endOffsets = client.endOffsets(inputTopicPartitions);
        final Map<TopicPartition, Long> beginningOffsets = client.beginningOffsets(inputTopicPartitions);

        final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>(inputTopicPartitions.size());
        for (final TopicPartition topicPartition : inputTopicPartitions) {
            topicPartitionsAndOffset.put(topicPartition, offset);
        }

        final Map<TopicPartition, Long> validatedTopicPartitionsAndOffset =
            checkOffsetRange(topicPartitionsAndOffset, beginningOffsets, endOffsets);

        for (final TopicPartition topicPartition : inputTopicPartitions) {
            client.seek(topicPartition, validatedTopicPartitionsAndOffset.get(topicPartition));
        }
    }


    private Map<TopicPartition, Long> parseResetPlan(final String resetPlanCsv) throws ParseException {
        final Map<TopicPartition, Long> topicPartitionAndOffset = new HashMap<>();
        if (resetPlanCsv == null || resetPlanCsv.isEmpty()) {
            throw new ParseException("Error parsing reset plan CSV file. It is empty,", 0);
        }

        final String[] resetPlanCsvParts = resetPlanCsv.split("\n");

        for (final String line : resetPlanCsvParts) {
            final String[] lineParts = line.split(",");
            if (lineParts.length != 3) {
                throw new ParseException("Reset plan CSV file is not following the format `TOPIC,PARTITION,OFFSET`.", 0);
            }
            final String topic = lineParts[0];
            final int partition = Integer.parseInt(lineParts[1]);
            final long offset = Long.parseLong(lineParts[2]);
            final TopicPartition topicPartition = new TopicPartition(topic, partition);
            topicPartitionAndOffset.put(topicPartition, offset);
        }

        return topicPartitionAndOffset;
    }

    private Map<TopicPartition, Long> checkOffsetRange(final Map<TopicPartition, Long> inputTopicPartitionsAndOffset,
                                                       final Map<TopicPartition, Long> beginningOffsets,
                                                       final Map<TopicPartition, Long> endOffsets) {
        final Map<TopicPartition, Long> validatedTopicPartitionsOffsets = new HashMap<>();
        for (final Map.Entry<TopicPartition, Long> topicPartitionAndOffset : inputTopicPartitionsAndOffset.entrySet()) {
            final long endOffset = endOffsets.get(topicPartitionAndOffset.getKey());
            final long offset = topicPartitionAndOffset.getValue();
            if (offset < endOffset) {
                final long beginningOffset = beginningOffsets.get(topicPartitionAndOffset.getKey());
                if (offset > beginningOffset) {
                    validatedTopicPartitionsOffsets.put(topicPartitionAndOffset.getKey(), offset);
                } else {
                    System.out.println("New offset (" + offset + ") is lower than earliest offset. Value will be set to " + beginningOffset);
                    validatedTopicPartitionsOffsets.put(topicPartitionAndOffset.getKey(), beginningOffset);
                }
            } else {
                System.out.println("New offset (" + offset + ") is higher than latest offset. Value will be set to " + endOffset);
                validatedTopicPartitionsOffsets.put(topicPartitionAndOffset.getKey(), endOffset);
            }
        }
        return validatedTopicPartitionsOffsets;
    }

    private boolean isInputTopic(final String topic) {
        return options.valuesOf(inputTopicsOption).contains(topic);
    }

    private boolean isIntermediateTopic(final String topic) {
        return options.valuesOf(intermediateTopicsOption).contains(topic);
    }

    private void maybeDeleteInternalTopics(final Admin adminClient, final boolean dryRun) {
        System.out.println("Deleting all internal/auto-created topics for application " + options.valueOf(applicationIdOption));
        final List<String> topicsToDelete = new ArrayList<>();
        for (final String listing : allTopics) {
            if (isInternalTopic(listing)) {
                if (!dryRun) {
                    topicsToDelete.add(listing);
                } else {
                    System.out.println("Topic: " + listing);
                }
            }
        }
        if (!dryRun) {
            doDelete(topicsToDelete, adminClient);
        }
        System.out.println("Done.");
    }

    // visible for testing
    public void doDelete(final List<String> topicsToDelete,
                         final Admin adminClient) {
        boolean hasDeleteErrors = false;
        final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
        final Map<String, KafkaFuture<Void>> results = deleteTopicsResult.values();

        for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
            try {
                entry.getValue().get(30, TimeUnit.SECONDS);
            } catch (final Exception e) {
                System.err.println("ERROR: deleting topic " + entry.getKey());
                e.printStackTrace(System.err);
                hasDeleteErrors = true;
            }
        }
        if (hasDeleteErrors) {
            throw new RuntimeException("Encountered an error deleting one or more topics");
        }
    }

    private boolean isInternalTopic(final String topicName) {
        // Specified input/intermediate topics might be named like internal topics (by chance).
        // Even is this is not expected in general, we need to exclude those topics here
        // and don't consider them as internal topics even if they follow the same naming schema.
        // Cf. https://issues.apache.org/jira/browse/KAFKA-7930
        return !isInputTopic(topicName) && !isIntermediateTopic(topicName) && topicName.startsWith(options.valueOf(applicationIdOption) + "-")
               && matchesInternalTopicFormat(topicName);
    }

    // visible for testing
    public boolean matchesInternalTopicFormat(final String topicName) {
        return topicName.endsWith("-changelog") || topicName.endsWith("-repartition")
               || topicName.endsWith("-subscription-registration-topic")
               || topicName.endsWith("-subscription-response-topic")
               || topicName.matches(".+-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-\\d+-topic")
               || topicName.matches(".+-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-\\d+-topic");
    }

    public static void main(final String[] args) {
        Exit.exit(new StreamsResetter().run(args));
    }

}
