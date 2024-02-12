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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public final class ConsoleShareConsumerOptions extends CommandDefaultOptions {
    private final OptionSpec<String> messageFormatterOpt;
    private final OptionSpec<String> messageFormatterConfigOpt;
    private final OptionSpec<String> messageFormatterArgOpt;
    private final OptionSpec<String> keyDeserializerOpt;
    private final OptionSpec<String> valueDeserializerOpt;
    private final OptionSpec<Integer> maxMessagesOpt;
    private final OptionSpec<?> rejectMessageOnErrorOpt;
    private final OptionSpec<String> topicOpt;
    private final OptionSpec<Integer> timeoutMsOpt;
    private final OptionSpec<String> bootstrapServerOpt;
    private final OptionSpec<String> groupIdOpt;
    private final Properties consumerProps;
    private final MessageFormatter formatter;

    public ConsoleShareConsumerOptions(String[] args) throws IOException {
        super(args);
        topicOpt = parser.accepts("topic", "The topic to consume on.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
        OptionSpec<String> consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
                .withRequiredArg()
                .describedAs("consumer_prop")
                .ofType(String.class);
        OptionSpec<String> consumerConfigOpt = parser.accepts("consumer.config", "Consumer config properties file. Note that " + consumerPropertyOpt + " takes precedence over this config.")
                .withRequiredArg()
                .describedAs("config file")
                .ofType(String.class);
        messageFormatterOpt = parser.accepts("formatter", "The name of a class to use for formatting Kafka messages for display.")
                .withRequiredArg()
                .describedAs("class")
                .ofType(String.class)
                .defaultsTo(DefaultMessageFormatterTemporary.class.getName());
        messageFormatterArgOpt = parser.accepts("property",
                        "The properties to initialize the message formatter. Default properties include: \n" +
                                " print.timestamp=true|false\n" +
                                " print.key=true|false\n" +
                                " print.offset=true|false\n" +
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
        rejectMessageOnErrorOpt = parser.accepts("reject-message-on-error", "If there is an error when processing a message, " +
                "reject it instead of halt.");
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
        groupIdOpt = parser.accepts("group", "The share group id of the consumer.")
                .withRequiredArg()
                .describedAs("consumer group id")
                .ofType(String.class);

        try {
            options = parser.parse(args);
        } catch (OptionException oe) {
            CommandLineUtils.printUsageAndExit(parser, oe.getMessage());
        }

        CommandLineUtils.maybePrintHelpOrVersion(this, "This tool helps to read data from Kafka topics using share groups and outputs it to standard output.");

        checkRequiredArgs();

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
        // The default value for group.id is "share"
        if (groupIdsProvided.isEmpty()) {
            groupIdsProvided.add("share");
        } else if (groupIdsProvided.size() > 1) {
            CommandLineUtils.printUsageAndExit(parser, "The group ids provided in different places (directly using '--group', "
                    + "via '--consumer-property', or via '--consumer.config') do not match. "
                    + "Detected group ids: "
                    + groupIdsProvided.stream().map(group -> "'" + group + "'").collect(Collectors.joining(", ")));
        }
        return groupIdsProvided;
    }

    private Properties buildConsumerProps(Properties consumerPropsFromFile, Properties extraConsumerProps, Set<String> groupIdsProvided) {
        Properties consumerProps = new Properties(consumerPropsFromFile);
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

    /**
     * TEMPORARY - The classes below are defined as separate classes
     * in the open PR - <a href="https://github.com/apache/kafka/pull/15274/files#">...</a>
     * These inner classes are temporary and will be removed once the above PR gets merged.
     */
    public static class DefaultMessageFormatterTemporary implements MessageFormatter {
        private static final Logger LOG = LoggerFactory.getLogger(DefaultMessageFormatterTemporary.class);

        private boolean printTimestamp = false;
        private boolean printKey = false;
        private boolean printValue = true;
        private boolean printPartition = false;
        private boolean printOffset = false;
        private boolean printHeaders = false;
        private byte[] keySeparator = utfBytes("\t");
        private byte[] lineSeparator = utfBytes("\n");
        private byte[] headersSeparator = utfBytes(",");
        private byte[] nullLiteral = utfBytes("null");

        private Optional<Deserializer<?>> keyDeserializer = Optional.empty();
        private Optional<Deserializer<?>> valueDeserializer = Optional.empty();
        private Optional<Deserializer<?>> headersDeserializer = Optional.empty();

        @Override
        public void configure(Map<String, ?> configs) {
            if (configs.containsKey("print.timestamp")) {
                printTimestamp = getBoolProperty(configs, "print.timestamp");
            }
            if (configs.containsKey("print.key")) {
                printKey = getBoolProperty(configs, "print.key");
            }
            if (configs.containsKey("print.offset")) {
                printOffset = getBoolProperty(configs, "print.offset");
            }
            if (configs.containsKey("print.partition")) {
                printPartition = getBoolProperty(configs, "print.partition");
            }
            if (configs.containsKey("print.headers")) {
                printHeaders = getBoolProperty(configs, "print.headers");
            }
            if (configs.containsKey("print.value")) {
                printValue = getBoolProperty(configs, "print.value");
            }
            if (configs.containsKey("key.separator")) {
                keySeparator = getByteProperty(configs, "key.separator");
            }
            if (configs.containsKey("line.separator")) {
                lineSeparator = getByteProperty(configs, "line.separator");
            }
            if (configs.containsKey("headers.separator")) {
                headersSeparator = getByteProperty(configs, "headers.separator");
            }
            if (configs.containsKey("null.literal")) {
                nullLiteral = getByteProperty(configs, "null.literal");
            }

            keyDeserializer = getDeserializerProperty(configs, true, "key.deserializer");
            valueDeserializer = getDeserializerProperty(configs, false, "value.deserializer");
            headersDeserializer = getDeserializerProperty(configs, false, "headers.deserializer");
        }

        // for testing
        public boolean printValue() {
            return printValue;
        }

        // for testing
        public Optional<Deserializer<?>> keyDeserializer() {
            return keyDeserializer;
        }

        private void writeSeparator(PrintStream output, boolean columnSeparator) {
            try {
                if (columnSeparator) {
                    output.write(keySeparator);
                } else {
                    output.write(lineSeparator);
                }
            } catch (IOException ioe) {
                LOG.error("Unable to write the separator to the output", ioe);
            }
        }

        private byte[] deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Optional<Deserializer<?>> deserializer, byte[] sourceBytes, String topic) {
            byte[] nonNullBytes = sourceBytes != null ? sourceBytes : nullLiteral;
            return deserializer.map(value -> utfBytes(value.deserialize(topic, consumerRecord.headers(), nonNullBytes).toString())).orElse(nonNullBytes);
        }

        @Override
        public void writeTo(ConsumerRecord<byte[], byte[]> consumerRecord, PrintStream output) {
            try {
                if (printTimestamp) {
                    if (consumerRecord.timestampType() != TimestampType.NO_TIMESTAMP_TYPE) {
                        output.print(consumerRecord.timestampType() + ":" + consumerRecord.timestamp());
                    } else {
                        output.print("NO_TIMESTAMP");
                    }
                    writeSeparator(output, printOffset || printPartition || printHeaders || printKey || printValue);
                }

                if (printPartition) {
                    output.print("Partition:");
                    output.print(consumerRecord.partition());
                    writeSeparator(output, printOffset || printHeaders || printKey || printValue);
                }

                if (printOffset) {
                    output.print("Offset:");
                    output.print(consumerRecord.offset());
                    writeSeparator(output, printHeaders || printKey || printValue);
                }

                if (printHeaders) {
                    Iterator<Header> headersIt = consumerRecord.headers().iterator();
                    if (!headersIt.hasNext()) {
                        output.print("NO_HEADERS");
                    } else {
                        while (headersIt.hasNext()) {
                            Header header = headersIt.next();
                            output.print(header.key() + ":");
                            output.write(deserialize(consumerRecord, headersDeserializer, header.value(), consumerRecord.topic()));
                            if (headersIt.hasNext()) {
                                output.write(headersSeparator);
                            }
                        }
                    }
                    writeSeparator(output, printKey || printValue);
                }

                if (printKey) {
                    output.write(deserialize(consumerRecord, keyDeserializer, consumerRecord.key(), consumerRecord.topic()));
                    writeSeparator(output, printValue);
                }

                if (printValue) {
                    output.write(deserialize(consumerRecord, valueDeserializer, consumerRecord.value(), consumerRecord.topic()));
                    output.write(lineSeparator);
                }
            } catch (IOException ioe) {
                LOG.error("Unable to write the consumer record to the output", ioe);
            }
        }

        private Map<String, ?> propertiesWithKeyPrefixStripped(String prefix, Map<String, ?> configs) {
            final Map<String, Object> newConfigs = new HashMap<>();
            for (Map.Entry<String, ?> entry : configs.entrySet()) {
                if (entry.getKey().startsWith(prefix) && entry.getKey().length() > prefix.length()) {
                    newConfigs.put(entry.getKey().substring(prefix.length()), entry.getValue());
                }
            }
            return newConfigs;
        }

        private byte[] utfBytes(String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }

        private byte[] getByteProperty(Map<String, ?> configs, String key) {
            return utfBytes((String) configs.get(key));
        }

        private boolean getBoolProperty(Map<String, ?> configs, String key) {
            return ((String) configs.get(key)).trim().equalsIgnoreCase("true");
        }

        private Optional<Deserializer<?>> getDeserializerProperty(Map<String, ?> configs, boolean isKey, String key) {
            if (configs.containsKey(key)) {
                String name = (String) configs.get(key);
                try {
                    Deserializer<?> deserializer = (Deserializer<?>) Class.forName(name).getDeclaredConstructor().newInstance();
                    Map<String, ?> deserializerConfig = propertiesWithKeyPrefixStripped(key + ".", configs);
                    deserializer.configure(deserializerConfig, isKey);
                    return Optional.of(deserializer);
                } catch (Exception e) {
                    LOG.error("Unable to instantiate a deserializer from " + name, e);
                }
            }
            return Optional.empty();
        }
    }

}