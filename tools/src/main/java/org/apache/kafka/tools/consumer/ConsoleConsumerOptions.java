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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

public final class ConsoleConsumerOptions extends CommandDefaultOptions {

    private static final Random RANDOM = new Random();

    private final OptionSpec<String> topicOpt;
    private final OptionSpec<String> includeOpt;
    private final OptionSpec<Integer> partitionIdOpt;
    private final OptionSpec<String> offsetOpt;
    private final OptionSpec<String> messageFormatterOpt;
    private final OptionSpec<String> messageFormatterArgOpt;
    private final OptionSpec<String> messageFormatterConfigOpt;
    private final OptionSpec<?> resetBeginningOpt;
    private final OptionSpec<Integer> maxMessagesOpt;
    private final OptionSpec<Long> timeoutMsOpt;
    private final OptionSpec<?> skipMessageOnErrorOpt;
    private final OptionSpec<String> bootstrapServerOpt;
    private final OptionSpec<String> keyDeserializerOpt;
    private final OptionSpec<String> valueDeserializerOpt;
    private final OptionSpec<?> enableSystestEventsLoggingOpt;
    private final OptionSpec<String> isolationLevelOpt;
    private final OptionSpec<String> groupIdOpt;

    private final Properties consumerProps;
    private final long offset;
    private final long timeoutMs;
    private final MessageFormatter formatter;

    public ConsoleConsumerOptions(String[] args) throws IOException {
        super(args);
        topicOpt = parser.accepts("topic", "The topic to consume on.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
        includeOpt = parser.accepts("include",
                        "Regular expression specifying list of topics to include for consumption.")
                .withRequiredArg()
                .describedAs("Java regex (String)")
                .ofType(String.class);
        partitionIdOpt = parser.accepts("partition",
                        "The partition to consume from. Consumption starts from the end of the partition unless '--offset' is specified.")
                .withRequiredArg()
                .describedAs("partition")
                .ofType(Integer.class);
        offsetOpt = parser.accepts("offset", "The offset to consume from (a non-negative number), or 'earliest' which means from beginning, or 'latest' which means from end")
                .withRequiredArg()
                .describedAs("consume offset")
                .ofType(String.class)
                .defaultsTo("latest");
        OptionSpec<String> consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
                .withRequiredArg()
                .describedAs("consumer_prop")
                .ofType(String.class);
        OptionSpec<String> consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file. Note that " + consumerPropertyOpt + " takes precedence over this config.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
        messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting kafka messages for display.")
                .withRequiredArg()
                .describedAs("class")
                .ofType(String.class)
                .defaultsTo(DefaultMessageFormatter.class.getName());
        messageFormatterArgOpt = parser.accepts("property",
                        "The properties to initialize the message formatter. Default properties include: \n" +
                            " print.timestamp=true|false\n" +
                            " print.key=true|false\n" +
                            " print.offset=true|false\n" +
                            " print.epoch=true|false\n" +
                            " print.partition=true|false\n" +
                            " print.headers=true|false\n" +
                            " print.value=true|false\n" +
                            " key.separator=<key.separator>\n" +
                            " line.separator=<line.separator>\n" +
                            " headers.separator=<line.separator>\n" +
                            " null.literal=<null.literal>\n" +
                            " key.deserializer=<key.deserializer>\n" +
                            " value.deserializer=<value.deserializer>\n" +
                            " header.deserializer=<header.deserializer>\n" +
                            "\nUsers can also pass in customized properties for their formatter; more specifically, users can pass in properties keyed with 'key.deserializer.', 'value.deserializer.' and 'headers.deserializer.' prefixes to configure their deserializers.")
                .withRequiredArg()
                .describedAs("prop")
                .ofType(String.class);
        messageFormatterConfigOpt = parser.accepts("formatter-config", "Config properties file to initialize the message formatter. Note that " + messageFormatterArgOpt + " takes precedence over this config.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
        resetBeginningOpt = parser.accepts("from-beginning", "If the consumer does not already have an established offset to consume from, " +
                "start with the earliest message present in the log rather than the latest message.");
        maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
                .withRequiredArg()
                .describedAs("num_messages")
                .ofType(Integer.class);
        timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
                .withRequiredArg()
                .describedAs("timeout_ms")
                .ofType(Long.class);
        skipMessageOnErrorOpt = parser.accepts("skip-message-on-error", "If there is an error when processing a message, " +
                "skip it instead of halt.");
        bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
        keyDeserializerOpt = parser.accepts("key-deserializer")
                .withRequiredArg()
                .describedAs("deserializer for key")
                .ofType(String.class);
        valueDeserializerOpt = parser.accepts("value-deserializer")
                .withRequiredArg()
                .describedAs("deserializer for values")
                .ofType(String.class);
        enableSystestEventsLoggingOpt = parser.accepts("enable-systest-events",
                "Log lifecycle events of the consumer in addition to logging consumed messages. (This is specific for system tests.)");
        isolationLevelOpt = parser.accepts("isolation-level",
                        "Set to read_committed in order to filter out transactional messages which are not committed. Set to read_uncommitted " +
                                "to read all messages.")
                .withRequiredArg()
                .ofType(String.class)
                .defaultsTo("read_uncommitted");
        groupIdOpt = parser.accepts("group", "The consumer group id of the consumer.")
                .withRequiredArg()
                .describedAs("consumer group id")
                .ofType(String.class);

        try {
            options = parser.parse(args);
        } catch (OptionException oe) {
            CommandLineUtils.printUsageAndExit(parser, oe.getMessage());
        }

        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from Kafka topics and outputs it to standard output.");

        checkRequiredArgs();

        Properties consumerPropsFromFile = options.has(consumerConfigOpt)
                ? Utils.loadProps(options.valueOf(consumerConfigOpt))
                : new Properties();
        Properties extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt));
        Set<String> groupIdsProvided = checkConsumerGroup(consumerPropsFromFile, extraConsumerProps);
        consumerProps = buildConsumerProps(consumerPropsFromFile, extraConsumerProps, groupIdsProvided);
        offset = parseOffset();
        timeoutMs = parseTimeoutMs();
        formatter = buildFormatter();
    }

    private void checkRequiredArgs() {
        List<Optional<String>> topicOrFilterArgs = new ArrayList<>(Arrays.asList(topicArg(), includedTopicsArg()));
        topicOrFilterArgs.removeIf(arg -> !arg.isPresent());
        // user need to specify value for either --topic or --include options)
        if (topicOrFilterArgs.size() != 1) {
            CommandLineUtils.printUsageAndExit(parser, "Exactly one of --include/--topic is required. ");
        }

        if (partitionArg().isPresent()) {
            if (!options.has(topicOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "The topic is required when partition is specified.");
            }
            if (fromBeginning() && options.has(offsetOpt)) {
                CommandLineUtils.printUsageAndExit(parser, "Options from-beginning and offset cannot be specified together.");
            }
        } else if (options.has(offsetOpt)) {
            CommandLineUtils.printUsageAndExit(parser, "The partition is required when offset is specified.");
        }

        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
    }

    private Set<String> checkConsumerGroup(Properties consumerPropsFromFile, Properties extraConsumerProps) {
        // if the group id is provided in more than place (through different means) all values must be the same
        Set<String> groupIdsProvided = new HashSet<>();
        if (options.has(groupIdOpt)) {
            groupIdsProvided.add(options.valueOf(groupIdOpt));
        }

        if (consumerPropsFromFile.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            groupIdsProvided.add((String) consumerPropsFromFile.get(ConsumerConfig.GROUP_ID_CONFIG));
        }

        if (extraConsumerProps.containsKey(ConsumerConfig.GROUP_ID_CONFIG)) {
            groupIdsProvided.add(extraConsumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));
        }
        if (groupIdsProvided.size() > 1) {
            CommandLineUtils.printUsageAndExit(parser, "The group ids provided in different places (directly using '--group', "
                    + "via '--consumer-property', or via '--consumer.config') do not match. "
                    + "Detected group ids: "
                    + groupIdsProvided.stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")));
        }
        if (!groupIdsProvided.isEmpty() && partitionArg().isPresent()) {
            CommandLineUtils.printUsageAndExit(parser, "Options group and partition cannot be specified together.");
        }
        return groupIdsProvided;
    }

    private Properties buildConsumerProps(Properties consumerPropsFromFile, Properties extraConsumerProps, Set<String> groupIdsProvided) {
        Properties consumerProps = new Properties();
        consumerProps.putAll(consumerPropsFromFile);
        consumerProps.putAll(extraConsumerProps);
        setAutoOffsetResetValue(consumerProps);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer());
        if (consumerProps.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null) {
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "console-consumer");
        }
        CommandLineUtils.maybeMergeOptions(consumerProps, ConsumerConfig.ISOLATION_LEVEL_CONFIG, options, isolationLevelOpt);

        if (groupIdsProvided.isEmpty()) {
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-" + RANDOM.nextInt(100000));
            // By default, avoid unnecessary expansion of the coordinator cache since
            // the auto-generated group and its offsets is not intended to be used again
            if (!consumerProps.containsKey(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
                consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            }
        } else {
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdsProvided.iterator().next());
        }
        return consumerProps;
    }

    /**
     * Used to retrieve the correct value for the consumer parameter 'auto.offset.reset'.
     * Order of priority is:
     *   1. Explicitly set parameter via --consumer.property command line parameter
     *   2. Explicit --from-beginning given -> 'earliest'
     *   3. Default value of 'latest'
     * In case both --from-beginning and an explicit value are specified an error is thrown if these
     * are conflicting.
     */
    private void setAutoOffsetResetValue(Properties props) {
        String earliestConfigValue = "earliest";
        String latestConfigValue = "latest";

        if (props.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            // auto.offset.reset parameter was specified on the command line
            String autoResetOption = props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG);
            if (fromBeginning() && !earliestConfigValue.equals(autoResetOption)) {
                // conflicting options - latest und earliest, throw an error
                System.err.println("Can't simultaneously specify --from-beginning and 'auto.offset.reset=" + autoResetOption + "', " +
                        "please remove one option");
                Exit.exit(1);
            }
            // nothing to do, checking for valid parameter values happens later and the specified
            // value was already copied during .putall operation
        } else {
            // no explicit value for auto.offset.reset was specified
            // if --from-beginning was specified use earliest, otherwise default to latest
            String autoResetOption = fromBeginning() ? earliestConfigValue : latestConfigValue;
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoResetOption);
        }
    }

    private long parseOffset() {
        if (options.has(offsetOpt)) {
            switch (options.valueOf(offsetOpt).toLowerCase(Locale.ROOT)) {
                case "earliest":
                    return ListOffsetsRequest.EARLIEST_TIMESTAMP;
                case "latest":
                    return ListOffsetsRequest.LATEST_TIMESTAMP;
                default:
                    String offsetString = options.valueOf(offsetOpt);
                    try {
                        long offset = Long.parseLong(offsetString);
                        if (offset < 0) {
                            invalidOffset(offsetString);
                        }
                        return offset;
                    } catch (NumberFormatException nfe) {
                        invalidOffset(offsetString);
                    }
            }
        } else if (fromBeginning()) {
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        }
        return ListOffsetsRequest.LATEST_TIMESTAMP;
    }

    private void invalidOffset(String offset) {
        CommandLineUtils.printUsageAndExit(parser, "The provided offset value '" + offset + "' is incorrect. Valid values are " +
                "'earliest', 'latest', or a non-negative long.");
    }

    private long parseTimeoutMs() {
        long timeout = options.has(timeoutMsOpt) ? options.valueOf(timeoutMsOpt) : -1;
        return timeout >= 0 ? timeout : Long.MAX_VALUE;
    }

    private MessageFormatter buildFormatter() {
        MessageFormatter formatter = null;
        try {
            Class<?> messageFormatterClass = Class.forName(options.valueOf(messageFormatterOpt));
            formatter = (MessageFormatter) messageFormatterClass.getDeclaredConstructor().newInstance();

            Properties formatterArgs = formatterArgs();
            Map<String, String> formatterConfigs = new HashMap<>();
            for (final String name : formatterArgs.stringPropertyNames()) {
                formatterConfigs.put(name, formatterArgs.getProperty(name));
            }

            formatter.configure(formatterConfigs);

        } catch (Exception e) {
            CommandLineUtils.printUsageAndExit(parser, e.getMessage());
        }
        return formatter;
    }

    Properties consumerProps() {
        return consumerProps;
    }

    boolean fromBeginning() {
        return options.has(resetBeginningOpt);
    }

    long offsetArg() {
        return offset;
    }

    boolean skipMessageOnError() {
        return options.has(skipMessageOnErrorOpt);
    }

    OptionalInt partitionArg() {
        if (options.has(partitionIdOpt)) {
            return OptionalInt.of(options.valueOf(partitionIdOpt));
        }
        return OptionalInt.empty();
    }

    Optional<String> topicArg() {
        return options.has(topicOpt) ? Optional.of(options.valueOf(topicOpt)) : Optional.empty();
    }

    int maxMessages() {
        return options.has(maxMessagesOpt) ? options.valueOf(maxMessagesOpt) : -1;
    }

    long timeoutMs() {
        return timeoutMs;
    }

    boolean enableSystestEventsLogging() {
        return options.has(enableSystestEventsLoggingOpt);
    }

    String bootstrapServer() {
        return options.valueOf(bootstrapServerOpt);
    }

    Optional<String> includedTopicsArg() {
        return options.has(includeOpt)
                ? Optional.of(options.valueOf(includeOpt))
                : Optional.empty();
    }

    Properties formatterArgs() throws IOException {
        Properties formatterArgs = options.has(messageFormatterConfigOpt)
                ? Utils.loadProps(options.valueOf(messageFormatterConfigOpt))
                : new Properties();
        String keyDeserializer = options.valueOf(keyDeserializerOpt);
        if (keyDeserializer != null && !keyDeserializer.isEmpty()) {
            formatterArgs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        }
        String valueDeserializer = options.valueOf(valueDeserializerOpt);
        if (valueDeserializer != null && !valueDeserializer.isEmpty()) {
            formatterArgs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        }
        formatterArgs.putAll(CommandLineUtils.parseKeyValueArgs(options.valuesOf(messageFormatterArgOpt)));

        return formatterArgs;
    }

    MessageFormatter formatter() {
        return formatter;
    }
}
