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

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import joptsimple.OptionException;
import joptsimple.OptionSpec;

public final class ConsoleShareConsumerOptions extends CommandDefaultOptions {
    private final OptionSpec<String> messageFormatterOpt;
    private final OptionSpec<String> messageFormatterConfigOpt;
    private final OptionSpec<String> messageFormatterArgOpt;
    private final OptionSpec<String> keyDeserializerOpt;
    private final OptionSpec<String> valueDeserializerOpt;
    private final OptionSpec<Integer> maxMessagesOpt;
    private final OptionSpec<?> rejectMessageOnErrorOpt;
    private final OptionSpec<?> rejectOpt;
    private final OptionSpec<?> releaseOpt;
    private final OptionSpec<String> topicOpt;
    private final OptionSpec<Integer> timeoutMsOpt;
    private final OptionSpec<String> bootstrapServerOpt;
    private final OptionSpec<String> groupIdOpt;
    private final Properties consumerProps;
    private final MessageFormatter formatter;

    public ConsoleShareConsumerOptions(String[] args) throws IOException {
        super(args);
        topicOpt = parser.accepts("topic", "The topic to consume from.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
        OptionSpec<String> consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
                .withRequiredArg()
                .describedAs("consumer_prop")
                .ofType(String.class);
        OptionSpec<String> consumerConfigOpt = parser.accepts("consumer-config", "Consumer config properties file. Note that " + consumerPropertyOpt + " takes precedence over this config.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
        messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting Kafka messages for display.")
                .withRequiredArg()
                .describedAs("class")
                .ofType(String.class)
                .defaultsTo(DefaultMessageFormatter.class.getName());
        messageFormatterArgOpt = parser.accepts("property",
                        "The properties to initialize the message formatter. Default properties include: \n" +
                                " print.timestamp=true|false\n" +
                                " print.key=true|false\n" +
                                " print.offset=true|false\n" +
                                " print.delivery=true|false\n" +
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
        maxMessagesOpt = parser.accepts("max-messages", "The maximum number of messages to consume before exiting. If not set, consumption is continual.")
                .withRequiredArg()
                .describedAs("num_messages")
                .ofType(Integer.class);
        timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
                .withRequiredArg()
                .describedAs("timeout_ms")
                .ofType(Integer.class);
        rejectOpt = parser.accepts("reject", "If specified, messages are rejected as they are consumed.");
        releaseOpt = parser.accepts("release", "If specified, messages are released as they are consumed.");
        rejectMessageOnErrorOpt = parser.accepts("reject-message-on-error", "If there is an error when processing a message, " +
                "reject it instead of halting.");
        bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The server(s) to connect to.")
                .withRequiredArg()
                .describedAs("server to connect to")
                .ofType(String.class);
        keyDeserializerOpt = parser.accepts("key-deserializer", "The name of the class to use for deserializing keys.")
                .withRequiredArg()
                .describedAs("deserializer for key")
                .ofType(String.class);
        valueDeserializerOpt = parser.accepts("value-deserializer", "The name of the class to use for deserializing values.")
                .withRequiredArg()
                .describedAs("deserializer for values")
                .ofType(String.class);
        groupIdOpt = parser.accepts("group", "The share group id of the consumer.")
                .withRequiredArg()
                .describedAs("share group id")
                .ofType(String.class);

        try {
            options = parser.parse(args);
        } catch (OptionException oe) {
            CommandLineUtils.printUsageAndExit(parser, oe.getMessage());
        }

        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from Kafka topics using share groups and outputs it to standard output.");

        checkRequiredArgs();

        if (options.has(rejectOpt) && options.has(releaseOpt)) {
            CommandLineUtils.printUsageAndExit(parser, "At most one of --reject and --release may be specified.");
        }

        Properties consumerPropsFromFile = options.has(consumerConfigOpt)
                ? Utils.loadProps(options.valueOf(consumerConfigOpt))
                : new Properties();
        Properties extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt));

        Set<String> groupIdsProvided = checkShareGroup(consumerPropsFromFile, extraConsumerProps);
        consumerProps = buildConsumerProps(consumerPropsFromFile, extraConsumerProps, groupIdsProvided);
        formatter = buildFormatter();
    }

    private void checkRequiredArgs() {
        if (!options.has(topicOpt)) {
            CommandLineUtils.printUsageAndExit(parser, "--topic is a required argument");
        }
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
    }

    private Set<String> checkShareGroup(Properties consumerPropsFromFile, Properties extraConsumerProps) {
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
        // The default value for group.id is "console-share-consumer"
        if (groupIdsProvided.isEmpty()) {
            groupIdsProvided.add("console-share-consumer");
        } else if (groupIdsProvided.size() > 1) {
            CommandLineUtils.printUsageAndExit(parser, "The group ids provided in different places (directly using '--group', "
                    + "via '--consumer-property', or via '--consumer-config') do not match. "
                    + "Detected group ids: "
                    + groupIdsProvided.stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")));
        }
        return groupIdsProvided;
    }

    private Properties buildConsumerProps(Properties consumerPropsFromFile, Properties extraConsumerProps, Set<String> groupIdsProvided) {
        Properties consumerProps = new Properties();
        consumerProps.putAll(consumerPropsFromFile);
        consumerProps.putAll(extraConsumerProps);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer());
        if (consumerProps.getProperty(ConsumerConfig.CLIENT_ID_CONFIG) == null) {
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "console-share-consumer");
        }

        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdsProvided.iterator().next());
        return consumerProps;
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

    boolean rejectMessageOnError() {
        return options.has(rejectMessageOnErrorOpt);
    }

    AcknowledgeType acknowledgeType() {
        if (options.has(rejectOpt)) {
            return AcknowledgeType.REJECT;
        } else if (options.has(releaseOpt)) {
            return AcknowledgeType.RELEASE;
        } else {
            return AcknowledgeType.ACCEPT;
        }
    }

    String topicArg() {
        return options.valueOf(topicOpt);
    }

    int maxMessages() {
        return options.has(maxMessagesOpt) ? options.valueOf(maxMessagesOpt) : -1;
    }

    int timeoutMs() {
        return options.has(timeoutMsOpt) ? options.valueOf(timeoutMsOpt) : -1;
    }

    String bootstrapServer() {
        return options.valueOf(bootstrapServerOpt);
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
