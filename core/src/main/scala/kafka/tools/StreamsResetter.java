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
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
    private static OptionSpec<String> fromFileOption;
    private static OptionSpec<Long> shiftByOption;
    private static OptionSpecBuilder dryRunOption;
    private static OptionSpec<String> commandConfigOption;

    private OptionSet options = null;
    private final List<String> allTopics = new LinkedList<>();
    private boolean dryRun = false;


    public int run(final String[] args) {
        return run(args, new Properties());
    }

    public int run(final String[] args,
                   final Properties config) {
        int exitCode = EXIT_CODE_SUCCESS;

        KafkaAdminClient kafkaAdminClient = null;

        try {
            parseArguments(args);
            dryRun = options.has(dryRunOption);

            final String groupId = options.valueOf(applicationIdOption);
            final Properties properties = new Properties();
            if (options.has(commandConfigOption)) {
                properties.putAll(Utils.loadProps(options.valueOf(commandConfigOption)));
            }
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(bootstrapServerOption));

            validateNoActiveConsumers(groupId, properties);
            kafkaAdminClient = (KafkaAdminClient) AdminClient.create(properties);

            allTopics.clear();
            allTopics.addAll(kafkaAdminClient.listTopics().names().get(60, TimeUnit.SECONDS));

            if (dryRun) {
                System.out.println("----Dry run displays the actions which will be performed when running Streams Reset Tool----");
            }

            final HashMap<Object, Object> consumerConfig = new HashMap<>(config);
            consumerConfig.putAll(properties);
            maybeResetInputAndSeekToEndIntermediateTopicOffsets(consumerConfig);
            maybeDeleteInternalTopics(kafkaAdminClient);

        } catch (final Throwable e) {
            exitCode = EXIT_CODE_ERROR;
            System.err.println("ERROR: " + e);
            e.printStackTrace(System.err);
        } finally {
            if (kafkaAdminClient != null) {
                kafkaAdminClient.close(60, TimeUnit.SECONDS);
            }
        }

        return exitCode;
    }

    private void validateNoActiveConsumers(final String groupId,
                                           final Properties properties) {
        kafka.admin.AdminClient olderAdminClient = null;
        try {
            olderAdminClient = kafka.admin.AdminClient.create(properties);
            if (!olderAdminClient.describeConsumerGroup(groupId, 0).consumers().get().isEmpty()) {
                throw new IllegalStateException("Consumer group '" + groupId + "' is still active. "
                                                + "Make sure to stop all running application instances before running the reset tool.");
            }
        } finally {
            if (olderAdminClient != null) {
                olderAdminClient.close();
            }
        }
    }

    private void parseArguments(final String[] args) throws IOException {

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
        dryRunOption = optionParser.accepts("dry-run", "Display the actions that would be performed without executing the reset commands.");

        // TODO: deprecated in 1.0; can be removed eventually
        optionParser.accepts("zookeeper", "Zookeeper option is deprecated by bootstrap.servers, as the reset tool would no longer access Zookeeper directly.");

        try {
            options = optionParser.parse(args);
        } catch (final OptionException e) {
            printHelp(optionParser);
            throw e;
        }

        /*HashSet<OptionSpec<?>> allScenarioOptions = new HashSet<>();
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
        CommandLineUtils.checkInvalidArgs(optionParser, options, shiftByOption, allScenarioOptions.$minus$eq(shiftByOption));*/
    }

    private void maybeResetInputAndSeekToEndIntermediateTopicOffsets(final Map consumerConfig) throws Exception {
        final List<String> inputTopics = options.valuesOf(inputTopicsOption);
        final List<String> intermediateTopics = options.valuesOf(intermediateTopicsOption);


        final List<String> notFoundInputTopics = new ArrayList<>();
        final List<String> notFoundIntermediateTopics = new ArrayList<>();

        String groupId = options.valueOf(applicationIdOption);

        if (inputTopics.size() == 0 && intermediateTopics.size() == 0) {
            System.out.println("No input or intermediate topics specified. Skipping seek.");
            return;
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

        final Properties config = new Properties();
        config.putAll(consumerConfig);
        config.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (final KafkaConsumer<byte[], byte[]> client = new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            client.subscribe(topicsToSubscribe);
            client.poll(1);

            final Set<TopicPartition> partitions = client.assignment();
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

            maybeReset(client, inputTopicPartitions);

            maybeSeekToEnd(client, intermediateTopicPartitions);

            if (!dryRun) {
                for (final TopicPartition p : partitions) {
                    client.position(p);
                }
                client.commitSync();
            }

            if (notFoundInputTopics.size() > 0) {
                System.out.println("Following input topics are not found, skipping them");
                for (final String topic : notFoundInputTopics) {
                    System.out.println("Topic: " + topic);
                }
            }

            if (notFoundIntermediateTopics.size() > 0) {
                System.out.println("Following intermediate topics are not found, skipping them");
                for (final String topic : notFoundIntermediateTopics) {
                    System.out.println("Topic:" + topic);
                }
            }

        } catch (final Exception e) {
            System.err.println("ERROR: Resetting offsets failed.");
            throw e;
        }
        System.out.println("Done.");
    }

    private void maybeSeekToEnd(final KafkaConsumer<byte[], byte[]> client,
                                final Set<TopicPartition> intermediateTopicPartitions) {

        final String groupId = options.valueOf(applicationIdOption);
        final List<String> intermediateTopics = options.valuesOf(intermediateTopicsOption);

        if (intermediateTopicPartitions.size() > 0) {
            System.out.println("Following intermediate topics offsets will be reset to end (for consumer group " + groupId + ")");
            for (final String topic : intermediateTopics) {
                if (allTopics.contains(topic)) {
                    System.out.println("Topic: " + topic);
                }
            }
            if (!dryRun) {
                client.seekToEnd(intermediateTopicPartitions);
            }
        }
    }

    private void maybeReset(final KafkaConsumer<byte[], byte[]> client,
                            final Set<TopicPartition> inputTopicPartitions)
        throws Exception {

        final String groupId = options.valueOf(applicationIdOption);

        if (inputTopicPartitions.size() > 0) {
            System.out.println("Following input topics offsets will be reset to (for consumer group " + groupId + ")");
            if (options.has(toOffsetOption)) {
                final Long offset = options.valueOf(toOffsetOption);
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
            } else if (options.has(toEarliestOption)) {
                client.seekToBeginning(inputTopicPartitions);
            } else if (options.has(toLatestOption)) {
                client.seekToEnd(inputTopicPartitions);
            } else if (options.has(shiftByOption)) {
                final Long shiftBy = options.valueOf(shiftByOption);
                final Map<TopicPartition, Long> endOffsets = client.endOffsets(inputTopicPartitions);
                final Map<TopicPartition, Long> beginningOffsets = client.beginningOffsets(inputTopicPartitions);

                final Map<TopicPartition, Long> topicPartitionsAndOffset = new HashMap<>(inputTopicPartitions.size());
                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    final Long position = client.position(topicPartition);
                    final Long offset = position + shiftBy;
                    topicPartitionsAndOffset.put(topicPartition, offset);
                }

                final Map<TopicPartition, Long> validatedTopicPartitionsAndOffset =
                    checkOffsetRange(topicPartitionsAndOffset, beginningOffsets, endOffsets);

                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    client.seek(topicPartition, validatedTopicPartitionsAndOffset.get(topicPartition));
                }
            } else if (options.has(toDatetimeOption)) {
                String ts = options.valueOf(toDatetimeOption);
                if (!(ts.split("T")[1].contains("+") || ts.split("T")[1].contains("-") || ts.split("T")[1].contains("Z"))) {
                    ts = ts + "Z";
                }

                Date date;
                try {
                    date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").parse(ts);
                } catch (ParseException e) {
                    date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(ts);
                }

                final Long timestamp = date.getTime();

                final Map<TopicPartition, Long> topicPartitionsAndTimes = new HashMap<>(inputTopicPartitions.size());
                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    topicPartitionsAndTimes.put(topicPartition, timestamp);
                }

                final Map<TopicPartition, OffsetAndTimestamp> topicPartitionsAndOffset = client.offsetsForTimes(topicPartitionsAndTimes);

                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    final Long offset = topicPartitionsAndOffset.get(topicPartition).offset();
                    client.seek(topicPartition, offset);
                }
            } else if (options.has(byDurationOption)) {
                final String duration = options.valueOf(byDurationOption);
                final Date now = new Date();
                final Duration durationParsed = DatatypeFactory.newInstance().newDuration(duration);
                durationParsed.negate().addTo(now);
                final Long timestamp = now.getTime();

                final Map<TopicPartition, Long> topicPartitionsAndTimes = new HashMap<>(inputTopicPartitions.size());
                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    topicPartitionsAndTimes.put(topicPartition, timestamp);
                }

                final Map<TopicPartition, OffsetAndTimestamp> topicPartitionsAndOffset = client.offsetsForTimes(topicPartitionsAndTimes);

                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    final Long offset = topicPartitionsAndOffset.get(topicPartition).offset();
                    client.seek(topicPartition, offset);
                }
            } else if (options.has(fromFileOption)) {
                final Map<TopicPartition, Long> endOffsets = client.endOffsets(inputTopicPartitions);
                final Map<TopicPartition, Long> beginningOffsets = client.beginningOffsets(inputTopicPartitions);

                final String resetPlanPath = options.valueOf(fromFileOption);
                final String resetPlanCsv = Utils.readFileAsString(resetPlanPath);
                final Map<TopicPartition, Long> topicPartitionsAndOffset = parseResetPlan(resetPlanCsv);

                final Map<TopicPartition, Long> validatedTopicPartitionsAndOffset =
                    checkOffsetRange(topicPartitionsAndOffset, beginningOffsets, endOffsets);

                for (final TopicPartition topicPartition : inputTopicPartitions) {
                    final Long offset = validatedTopicPartitionsAndOffset.get(topicPartition);
                    client.seek(topicPartition, offset);
                }
            } else {
                client.seekToBeginning(inputTopicPartitions);
            }

            for (final TopicPartition p : inputTopicPartitions) {
                final Long position = client.position(p);
                System.out.println("Topic: " + p.topic() + " Partition: " + p.partition() + " Offset: " + position);
            }
        }
    }

    private Map<TopicPartition, Long> parseResetPlan(String resetPlanCsv) {
        final Map<TopicPartition, Long> topicPartitionAndOffset = new HashMap<>();
        for (final String line : resetPlanCsv.split("\n")) {
            final String[] parts = line.split(",");
            final String topic = parts[0];
            final int partition = Integer.parseInt(parts[1]);
            final long offset = Long.parseLong(parts[2]);
            final TopicPartition topicPartition = new TopicPartition(topic, partition);
            topicPartitionAndOffset.put(topicPartition, offset);
        }
        return topicPartitionAndOffset;
    }

    private Map<TopicPartition, Long> checkOffsetRange(Map<TopicPartition, Long> inputTopicPartitionsAndOffset,
                                                       Map<TopicPartition, Long> beginningOffsets,
                                                       Map<TopicPartition, Long> endOffsets) {
        Map<TopicPartition, Long> validatedTopicPartitionsOffsets = new HashMap<>();
        for (final TopicPartition topicPartition : inputTopicPartitionsAndOffset.keySet()) {
            final Long endOffset = endOffsets.get(topicPartition);
            final Long offset = inputTopicPartitionsAndOffset.get(topicPartition);
            if (offset < endOffset) {
                final Long beginningOffset = beginningOffsets.get(topicPartition);
                if (offset > beginningOffset) {
                    System.out.println("New offset (" + offset + ") is lower than earliest offset. Value will be set to " + beginningOffset);
                    //client.seek(topicPartition, beginningOffset);
                    validatedTopicPartitionsOffsets.put(topicPartition, beginningOffset);
                } else {
                    //client.seek(topicPartition, offset);
                    validatedTopicPartitionsOffsets.put(topicPartition, offset);
                }
            } else {
                System.out.println("New offset (" + offset + ") is higher than latest offset. Value will be set to " + endOffset);
                //client.seek(topicPartition, endOffset);
                validatedTopicPartitionsOffsets.put(topicPartition, endOffset);
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

    private void maybeDeleteInternalTopics(final KafkaAdminClient adminClient) {

        System.out.println("Deleting all internal/auto-created topics for application " + options.valueOf(applicationIdOption));
        List<String> topicsToDelete = new ArrayList<>();
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

    private void doDelete(final List<String> topicsToDelete,
                          final KafkaAdminClient adminClient) {
        boolean hasDeleteErrors = false;
        final DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
        final Map<String, KafkaFuture<Void>> results = deleteTopicsResult.values();

        for (final Map.Entry<String, KafkaFuture<Void>> entry : results.entrySet()) {
            try {
                entry.getValue().get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
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
        return topicName.startsWith(options.valueOf(applicationIdOption) + "-")
            && (topicName.endsWith("-changelog") || topicName.endsWith("-repartition"));
    }

    private void printHelp(OptionParser parser) throws IOException {
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
        parser.printHelpOn(System.err);
    }

    public static void main(final String[] args) {
        Exit.exit(new StreamsResetter().run(args));
    }

}
