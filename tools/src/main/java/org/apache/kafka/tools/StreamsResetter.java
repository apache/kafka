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
package org.apache.kafka.tools;

import joptsimple.OptionException;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;
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
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

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

    private final static String USAGE = "This tool helps to quickly reset an application in order to reprocess "
            + "its data from scratch.\n"
            + "* This tool resets offsets of input topics to the earliest available offset (by default), or to a specific defined position"
            + " and it skips to the end of intermediate topics (topics that are input and output topics, e.g., used by deprecated through() method).\n"
            + "* This tool deletes the internal topics that were created by Kafka Streams (topics starting with "
            + "\"<application.id>-\").\n"
            + "The tool finds these internal topics automatically. If the topics flagged automatically for deletion by "
            + "the dry-run are unsuitable, you can specify a subset with the \"--internal-topics\" option.\n"
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
            + "reset tool!\n\n"
            + "*** Warning! This tool makes irreversible changes to your application. It is strongly recommended that "
            + "you run this once with \"--dry-run\" to preview your changes before making them.\n\n";

    private final List<String> allTopics = new LinkedList<>();

    public static void main(final String[] args) {
        Exit.exit(new StreamsResetter().execute(args));
    }

    public int execute(final String[] args) {
        return execute(args, new Properties());
    }

    public int execute(final String[] args, final Properties config) {
        try {
            StreamsResetterOptions options = new StreamsResetterOptions(args);

            String groupId = options.applicationId();
            Properties properties = new Properties();
            if (options.hasCommandConfig()) {
                properties.putAll(Utils.loadProps(options.commandConfig()));
            }

            String bootstrapServerValue = "localhost:9092";
            if (options.hasBootstrapServer()) {
                bootstrapServerValue = options.bootstrapServer();
            } else if (options.hasBootstrapServers()) {
                bootstrapServerValue = options.bootstrapServers();
            }

            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerValue);

            try (Admin adminClient = Admin.create(properties)) {
                maybeDeleteActiveConsumers(groupId, adminClient, options);

                allTopics.clear();
                allTopics.addAll(adminClient.listTopics().names().get(60, TimeUnit.SECONDS));

                if (options.hasDryRun()) {
                    System.out.println("----Dry run displays the actions which will be performed when running Streams Reset Tool----");
                }

                final HashMap<Object, Object> consumerConfig = new HashMap<>(config);
                consumerConfig.putAll(properties);
                int exitCode = maybeResetInputAndSeekToEndIntermediateTopicOffsets(consumerConfig, options);
                exitCode |= maybeDeleteInternalTopics(adminClient, options);
                return exitCode;
            }
        } catch (Throwable e) {
            System.err.println("ERROR: " + e);
            e.printStackTrace(System.err);
            return EXIT_CODE_ERROR;
        }
    }

    private void maybeDeleteActiveConsumers(final String groupId,
                                            final Admin adminClient,
                                            final StreamsResetterOptions options)
        throws ExecutionException, InterruptedException {
        final DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
            Collections.singleton(groupId),
            new DescribeConsumerGroupsOptions().timeoutMs(10 * 1000));
        final List<MemberDescription> members =
            new ArrayList<>(describeResult.describedGroups().get(groupId).get().members());
        if (!members.isEmpty()) {
            if (options.hasForce()) {
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

    private int maybeResetInputAndSeekToEndIntermediateTopicOffsets(final Map<Object, Object> consumerConfig,
                                                                    final StreamsResetterOptions options)
        throws IOException, ParseException {
        final List<String> inputTopics = options.inputTopicsOption();
        final List<String> intermediateTopics = options.intermediateTopicsOption();
        int topicNotFound = EXIT_CODE_SUCCESS;

        final List<String> notFoundInputTopics = new ArrayList<>();
        final List<String> notFoundIntermediateTopics = new ArrayList<>();

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
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, options.applicationId());
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
                if (options.isInputTopic(topic)) {
                    inputTopicPartitions.add(p);
                } else if (options.isIntermediateTopic(topic)) {
                    intermediateTopicPartitions.add(p);
                } else {
                    System.err.println("Skipping invalid partition: " + p);
                }
            }

            maybeReset(client, inputTopicPartitions, options);

            maybeSeekToEnd(options.applicationId(), client, intermediateTopicPartitions);

            if (!options.hasDryRun()) {
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

    private void maybeReset(final Consumer<byte[], byte[]> client,
                            final Set<TopicPartition> inputTopicPartitions,
                            final StreamsResetterOptions options)
        throws IOException, ParseException {
        if (inputTopicPartitions.size() > 0) {
            System.out.println("Following input topics offsets will be reset to (for consumer group " + options.applicationId() + ")");
            if (options.hasToOffset()) {
                resetOffsetsTo(client, inputTopicPartitions, options.toOffset());
            } else if (options.hasToEarliest()) {
                client.seekToBeginning(inputTopicPartitions);
            } else if (options.hasToLatest()) {
                client.seekToEnd(inputTopicPartitions);
            } else if (options.hasShiftBy()) {
                shiftOffsetsBy(client, inputTopicPartitions, options.shiftBy());
            } else if (options.hasToDatetime()) {
                final String ts = options.toDatetime();
                final long timestamp = Utils.getDateTime(ts);
                resetToDatetime(client, inputTopicPartitions, timestamp);
            } else if (options.hasByDuration()) {
                final String duration = options.byDuration();
                resetByDuration(client, inputTopicPartitions, Duration.parse(duration));
            } else if (options.hasFromFile()) {
                final String resetPlanPath = options.fromFile();
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

    private int maybeDeleteInternalTopics(final Admin adminClient, final StreamsResetterOptions options) {
        final List<String> inferredInternalTopics = allTopics.stream()
                .filter(options::isInferredInternalTopic)
                .collect(Collectors.toList());
        final List<String> specifiedInternalTopics = options.internalTopics();
        final List<String> topicsToDelete;

        if (!specifiedInternalTopics.isEmpty()) {
            if (!inferredInternalTopics.containsAll(specifiedInternalTopics)) {
                throw new IllegalArgumentException("Invalid topic specified in the "
                        + "--internal-topics option. "
                        + "Ensure that the topics specified are all internal topics. "
                        + "Do a dry run without the --internal-topics option to see the "
                        + "list of all internal topics that can be deleted.");
            }

            topicsToDelete = specifiedInternalTopics;
            System.out.println("Deleting specified internal topics " + topicsToDelete);
        } else {
            topicsToDelete = inferredInternalTopics;
            System.out.println("Deleting inferred internal topics " + topicsToDelete);
        }

        if (!options.hasDryRun()) {
            doDelete(topicsToDelete, adminClient);
        }

        System.out.println("Done.");
        return EXIT_CODE_SUCCESS;
    }

    // visible for testing
    public void doDelete(final List<String> topicsToDelete, final Admin adminClient) {
        boolean hasDeleteErrors = false;
        final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
        final Map<String, KafkaFuture<Void>> results = deleteTopicsResult.topicNameValues();

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

    // visible for testing
    public static boolean matchesInternalTopicFormat(final String topicName) {
        return topicName.endsWith("-changelog") || topicName.endsWith("-repartition")
               || topicName.endsWith("-subscription-registration-topic")
               || topicName.endsWith("-subscription-response-topic")
               || topicName.matches(".+-KTABLE-FK-JOIN-SUBSCRIPTION-REGISTRATION-\\d+-topic")
               || topicName.matches(".+-KTABLE-FK-JOIN-SUBSCRIPTION-RESPONSE-\\d+-topic");
    }

    private static class StreamsResetterOptions extends CommandDefaultOptions {
        private final OptionSpec<String> bootstrapServersOption;
        private final OptionSpec<String> bootstrapServerOption;
        private final OptionSpec<String> applicationIdOption;
        private final OptionSpec<String> inputTopicsOption;
        private final OptionSpec<String> intermediateTopicsOption;
        private final OptionSpec<String> internalTopicsOption;
        private final OptionSpec<Long> toOffsetOption;
        private final OptionSpec<String> toDatetimeOption;
        private final OptionSpec<String> byDurationOption;
        private final OptionSpecBuilder toEarliestOption;
        private final OptionSpecBuilder toLatestOption;
        private final OptionSpec<String> fromFileOption;
        private final OptionSpec<Long> shiftByOption;
        private final OptionSpecBuilder dryRunOption;
        private final OptionSpec<String> commandConfigOption;
        private final OptionSpecBuilder forceOption;

        public StreamsResetterOptions(String[] args) {
            super(args);
            applicationIdOption = parser.accepts("application-id", "The Kafka Streams application ID (application.id).")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("id")
                .required();
            bootstrapServersOption = parser.accepts("bootstrap-servers", "DEPRECATED: Comma-separated list of broker urls with format: HOST1:PORT1,HOST2:PORT2")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("urls");
            bootstrapServerOption = parser.accepts("bootstrap-server", "REQUIRED unless --bootstrap-servers(deprecated) is specified. The server(s) to connect to. The broker list string in the form HOST1:PORT1,HOST2:PORT2. (default: localhost:9092)")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("server to connect to");
            inputTopicsOption = parser.accepts("input-topics", "Comma-separated list of user input topics. For these topics, the tool by default will reset the offset to the earliest available offset. "
                    + "Reset to other offset position by appending other reset offset option, ex: --input-topics foo --shift-by 5")
                .withRequiredArg()
                .ofType(String.class)
                .withValuesSeparatedBy(',')
                .describedAs("list");
            intermediateTopicsOption = parser.accepts("intermediate-topics", "Comma-separated list of intermediate user topics (topics that are input and output topics, "
                    + "e.g., used in the deprecated through() method). For these topics, the tool will skip to the end.")
                .withRequiredArg()
                .ofType(String.class)
                .withValuesSeparatedBy(',')
                .describedAs("list");
            internalTopicsOption = parser.accepts("internal-topics", "Comma-separated list of "
                    + "internal topics to delete. Must be a subset of the internal topics marked for deletion by the "
                    + "default behaviour (do a dry-run without this option to view these topics).")
                .withRequiredArg()
                .ofType(String.class)
                .withValuesSeparatedBy(',')
                .describedAs("list");
            toOffsetOption = parser.accepts("to-offset", "Reset offsets to a specific offset.")
                .withRequiredArg()
                .ofType(Long.class);
            toDatetimeOption = parser.accepts("to-datetime", "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'")
                .withRequiredArg()
                .ofType(String.class);
            byDurationOption = parser.accepts("by-duration", "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'")
                .withRequiredArg()
                .ofType(String.class);
            toEarliestOption = parser.accepts("to-earliest", "Reset offsets to earliest offset.");
            toLatestOption = parser.accepts("to-latest", "Reset offsets to latest offset.");
            fromFileOption = parser.accepts("from-file", "Reset offsets to values defined in CSV file.")
                .withRequiredArg()
                .ofType(String.class);
            shiftByOption = parser.accepts("shift-by", "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative")
                .withRequiredArg()
                .describedAs("number-of-offsets")
                .ofType(Long.class);
            commandConfigOption = parser.accepts("config-file", "Property file containing configs to be passed to admin clients and embedded consumer.")
                .withRequiredArg()
                .ofType(String.class)
                .describedAs("file name");
            forceOption = parser.accepts("force", "Force the removal of members of the consumer group (intended to remove stopped members if a long session timeout was used). " +
                "Make sure to shut down all stream applications when this option is specified to avoid unexpected rebalances.");

            dryRunOption = parser.accepts("dry-run", "Display the actions that would be performed without executing the reset commands.");

            try {
                options = parser.parse(args);
                if (CommandLineUtils.isPrintHelpNeeded(this)) {
                    CommandLineUtils.printUsageAndExit(parser, USAGE);
                }
                if (CommandLineUtils.isPrintVersionNeeded(this)) {
                    CommandLineUtils.printVersionAndExit();
                }
                CommandLineUtils.checkInvalidArgs(parser, options, toOffsetOption, toDatetimeOption, byDurationOption, toEarliestOption, toLatestOption, fromFileOption, shiftByOption);
                CommandLineUtils.checkInvalidArgs(parser, options, toDatetimeOption, toOffsetOption, byDurationOption, toEarliestOption, toLatestOption, fromFileOption, shiftByOption);
                CommandLineUtils.checkInvalidArgs(parser, options, byDurationOption, toOffsetOption, toDatetimeOption, toEarliestOption, toLatestOption, fromFileOption, shiftByOption);
                CommandLineUtils.checkInvalidArgs(parser, options, toEarliestOption, toOffsetOption, toDatetimeOption, byDurationOption, toLatestOption, fromFileOption, shiftByOption);
                CommandLineUtils.checkInvalidArgs(parser, options, toLatestOption, toOffsetOption, toDatetimeOption, byDurationOption, toEarliestOption, fromFileOption, shiftByOption);
                CommandLineUtils.checkInvalidArgs(parser, options, fromFileOption, toOffsetOption, toDatetimeOption, byDurationOption, toEarliestOption, toLatestOption, shiftByOption);
                CommandLineUtils.checkInvalidArgs(parser, options, shiftByOption, toOffsetOption, toDatetimeOption, byDurationOption, toEarliestOption, toLatestOption, fromFileOption);
            } catch (final OptionException e) {
                CommandLineUtils.printUsageAndExit(parser, e.getMessage());
            }
        }

        public boolean hasDryRun() {
            return options.has(dryRunOption);
        }

        public String applicationId() {
            return options.valueOf(applicationIdOption);
        }

        public boolean hasCommandConfig() {
            return options.has(commandConfigOption);
        }

        public String commandConfig() {
            return options.valueOf(commandConfigOption);
        }

        public boolean hasBootstrapServer() {
            return options.has(bootstrapServerOption);
        }

        public String bootstrapServer() {
            return options.valueOf(bootstrapServerOption);
        }

        public boolean hasBootstrapServers() {
            return options.has(bootstrapServersOption);
        }

        public String bootstrapServers() {
            return options.valueOf(bootstrapServersOption);
        }

        public boolean hasForce() {
            return options.has(forceOption);
        }

        public List<String> inputTopicsOption() {
            return options.valuesOf(inputTopicsOption);
        }

        public List<String> intermediateTopicsOption() {
            return options.valuesOf(intermediateTopicsOption);
        }

        public boolean hasToOffset() {
            return options.has(toOffsetOption);
        }

        public long toOffset() {
            return options.valueOf(toOffsetOption);
        }

        public boolean hasToEarliest() {
            return options.has(toEarliestOption);
        }

        public boolean hasToLatest() {
            return options.has(toLatestOption);
        }

        public boolean hasShiftBy() {
            return options.has(shiftByOption);
        }

        public long shiftBy() {
            return options.valueOf(shiftByOption);
        }

        public boolean hasToDatetime() {
            return options.has(toDatetimeOption);
        }

        public String toDatetime() {
            return options.valueOf(toDatetimeOption);
        }

        public boolean hasByDuration() {
            return options.has(byDurationOption);
        }

        public String byDuration() {
            return options.valueOf(byDurationOption);
        }

        public boolean hasFromFile() {
            return options.has(fromFileOption);
        }

        public String fromFile() {
            return options.valueOf(fromFileOption);
        }

        public boolean isInputTopic(String topic) {
            return options.valuesOf(inputTopicsOption).contains(topic);
        }

        public boolean isIntermediateTopic(String topic) {
            return options.valuesOf(intermediateTopicsOption).contains(topic);
        }

        private boolean isInferredInternalTopic(final String topicName) {
            // Specified input/intermediate topics might be named like internal topics (by chance).
            // Even is this is not expected in general, we need to exclude those topics here
            // and don't consider them as internal topics even if they follow the same naming schema.
            // Cf. https://issues.apache.org/jira/browse/KAFKA-7930
            return !isInputTopic(topicName) && !isIntermediateTopic(topicName) && topicName.startsWith(options.valueOf(applicationIdOption) + "-")
                && matchesInternalTopicFormat(topicName);
        }

        public List<String> internalTopics() {
            return options.valuesOf(internalTopicsOption);
        }
    }
}
