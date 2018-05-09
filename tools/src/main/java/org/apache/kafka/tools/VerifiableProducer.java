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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Exit;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Primarily intended for use with system testing, this producer prints metadata
 * in the form of JSON to stdout on each "send" request. For example, this helps
 * with end-to-end correctness tests by making externally visible which messages have been
 * acked and which have not.
 *
 * When used as a command-line tool, it produces increasing integers. It will produce a
 * fixed number of messages unless the default max-messages -1 is used, in which case
 * it produces indefinitely.
 *
 * If logging is left enabled, log output on stdout can be easily ignored by checking
 * whether a given line is valid JSON.
 */
public class VerifiableProducer {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String topic;
    private final Producer<String, String> producer;
    // If maxMessages < 0, produce until the process is killed externally
    private long maxMessages = -1;

    // Number of messages for which acks were received
    private long numAcked = 0;

    // Number of send attempts
    private long numSent = 0;

    // Throttle message throughput if this is set >= 0
    private final long throughput;

    // Hook to trigger producing thread to stop sending messages
    private boolean stopProducing = false;

    // Prefix (plus a dot separator) added to every value produced by verifiable producer
    // if null, then values are produced without a prefix
    private final Integer valuePrefix;

    // Send messages with a key of 0 incrementing by 1 for
    // each message produced when number specified is reached
    // key is reset to 0
    private final Integer repeatingKeys;

    private int keyCounter;

    // The create time to set in messages, in milliseconds since epoch
    private Long createTime;

    private final Long startTime;

    public VerifiableProducer(KafkaProducer<String, String> producer, String topic, int throughput, int maxMessages,
                              Integer valuePrefix, Long createTime, Integer repeatingKeys) {

        this.topic = topic;
        this.throughput = throughput;
        this.maxMessages = maxMessages;
        this.producer = producer;
        this.valuePrefix = valuePrefix;
        this.createTime = createTime;
        this.startTime = System.currentTimeMillis();
        this.repeatingKeys = repeatingKeys;

    }

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("verifiable-producer")
                .defaultHelp(true)
                .description("This tool produces increasing integers to the specified topic and prints JSON metadata to stdout on each \"send\" request, making externally visible which messages have been acked and which have not.");

        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("Produce messages to this topic.");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--max-messages")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("MAX-MESSAGES")
                .dest("maxMessages")
                .help("Produce this many messages. If -1, produce messages until the process is killed externally.");

        parser.addArgument("--throughput")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("If set >= 0, throttle maximum message throughput to *approximately* THROUGHPUT messages/sec.");

        parser.addArgument("--acks")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .choices(0, 1, -1)
                .metavar("ACKS")
                .help("Acks required on each produced message. See Kafka docs on acks for details.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Producer config properties file.");

        parser.addArgument("--message-create-time")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("CREATETIME")
                .dest("createTime")
                .help("Send messages with creation time starting at the arguments value, in milliseconds since epoch");

        parser.addArgument("--value-prefix")
            .action(store())
            .required(false)
            .type(Integer.class)
            .metavar("VALUE-PREFIX")
            .dest("valuePrefix")
            .help("If specified, each produced value will have this prefix with a dot separator");

        parser.addArgument("--repeating-keys")
            .action(store())
            .required(false)
            .type(Integer.class)
            .metavar("REPEATING-KEYS")
            .dest("repeatingKeys")
            .help("If specified, each produced record will have a key starting at 0 increment by 1 up to the number specified (exclusive), then the key is set to 0 again");

        return parser;
    }
    
    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     *
     * Note: this duplication of org.apache.kafka.common.utils.Utils.loadProps is unfortunate
     * but *intentional*. In order to use VerifiableProducer in compatibility and upgrade tests,
     * we use VerifiableProducer from the development tools package, and run it against 0.8.X.X kafka jars.
     * Since this method is not in Utils in the 0.8.X.X jars, we have to cheat a bit and duplicate.
     */
    public static Properties loadProps(String filename) throws IOException, FileNotFoundException {
        Properties props = new Properties();
        try (InputStream propStream = new FileInputStream(filename)) {
            props.load(propStream);
        }
        return props;
    }

    /** Construct a VerifiableProducer object from command-line arguments. */
    public static VerifiableProducer createFromArgs(ArgumentParser parser, String[] args) throws ArgumentParserException {
        Namespace res = parser.parseArgs(args);

        int maxMessages = res.getInt("maxMessages");
        String topic = res.getString("topic");
        int throughput = res.getInt("throughput");
        String configFile = res.getString("producer.config");
        Integer valuePrefix = res.getInt("valuePrefix");
        Long createTime = (long) res.getInt("createTime");
        Integer repeatingKeys = res.getInt("repeatingKeys");

        if (createTime == -1L)
            createTime = null;

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, Integer.toString(res.getInt("acks")));
        // No producer retries
        producerProps.put(ProducerConfig.RETRIES_CONFIG, "0");
        if (configFile != null) {
            try {
                producerProps.putAll(loadProps(configFile));
            } catch (IOException e) {
                throw new ArgumentParserException(e.getMessage(), parser);
            }
        }

        StringSerializer serializer = new StringSerializer();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps, serializer, serializer);

        return new VerifiableProducer(producer, topic, throughput, maxMessages, valuePrefix, createTime, repeatingKeys);
    }

    /** Produce a message with given key and value. */
    public void send(String key, String value) {
        ProducerRecord<String, String> record;

        // Older versions of ProducerRecord don't include the message create time in the constructor. So including
        // even a 'null' argument results in a NoSuchMethodException. Thus we only include the create time if it is
        // explicitly specified to remain fully backward compatible with older clients.
        if (createTime != null) {
            record = new ProducerRecord<>(topic, null, createTime, key, value);
            createTime += System.currentTimeMillis() - startTime;
        } else {
            record = new ProducerRecord<>(topic, key, value);
        }

        numSent++;
        try {
            producer.send(record, new PrintInfoCallback(key, value));
        } catch (Exception e) {

            synchronized (System.out) {
                printJson(new FailedSend(key, value, topic, e));
            }
        }
    }

    /** Returns a string to publish: ether 'valuePrefix'.'val' or 'val' **/
    public String getValue(long val) {
        if (this.valuePrefix != null) {
            return String.format("%d.%d", this.valuePrefix, val);
        }
        return String.format("%d", val);
    }

    public String getKey() {
        String key = null;
        if (repeatingKeys != null) {
            key = Integer.toString(keyCounter++);
            if (keyCounter == repeatingKeys) {
                keyCounter = 0;
            }
        }
        return key;
    }

    /** Close the producer to flush any remaining messages. */
    public void close() {
        producer.close();
        printJson(new ShutdownComplete());
    }

    @JsonPropertyOrder({ "timestamp", "name" })
    private static abstract class ProducerEvent {
        private final long timestamp = System.currentTimeMillis();

        @JsonProperty
        public abstract String name();

        @JsonProperty
        public long timestamp() {
            return timestamp;
        }
    }

    private static class StartupComplete extends ProducerEvent {

        @Override
        public String name() {
            return "startup_complete";
        }
    }

    private static class ShutdownComplete extends ProducerEvent {

        @Override
        public String name() {
            return "shutdown_complete";
        }
    }

    private static class SuccessfulSend extends ProducerEvent {

        private String key;
        private String value;
        private RecordMetadata recordMetadata;

        public SuccessfulSend(String key, String value, RecordMetadata recordMetadata) {
            assert recordMetadata != null : "Expected non-null recordMetadata object.";
            this.key = key;
            this.value = value;
            this.recordMetadata = recordMetadata;
        }

        @Override
        public String name() {
            return "producer_send_success";
        }

        @JsonProperty
        public String key() {
            return key;
        }

        @JsonProperty
        public String value() {
            return value;
        }

        @JsonProperty
        public String topic() {
            return recordMetadata.topic();
        }

        @JsonProperty
        public int partition() {
            return recordMetadata.partition();
        }

        @JsonProperty
        public long offset() {
            return recordMetadata.offset();
        }
    }

    private static class FailedSend extends ProducerEvent {

        private String topic;
        private String key;
        private String value;
        private Exception exception;

        public FailedSend(String key, String value, String topic, Exception exception) {
            assert exception != null : "Expected non-null exception.";
            this.key = key;
            this.value = value;
            this.topic = topic;
            this.exception = exception;
        }

        @Override
        public String name() {
            return "producer_send_error";
        }

        @JsonProperty
        public String key() {
            return key;
        }

        @JsonProperty
        public String value() {
            return value;
        }

        @JsonProperty
        public String topic() {
            return topic;
        }

        @JsonProperty
        public String exception() {
            return exception.getClass().toString();
        }

        @JsonProperty
        public String message() {
            return exception.getMessage();
        }
    }

    private static class ToolData extends ProducerEvent {

        private long sent;
        private long acked;
        private long targetThroughput;
        private double avgThroughput;

        public ToolData(long sent, long acked, long targetThroughput, double avgThroughput) {
            this.sent = sent;
            this.acked = acked;
            this.targetThroughput = targetThroughput;
            this.avgThroughput = avgThroughput;
        }

        @Override
        public String name() {
            return "tool_data";
        }

        @JsonProperty
        public long sent() {
            return this.sent;
        }

        @JsonProperty
        public long acked() {
            return this.acked;
        }

        @JsonProperty("target_throughput")
        public long targetThroughput() {
            return this.targetThroughput;
        }

        @JsonProperty("avg_throughput")
        public double avgThroughput() {
            return this.avgThroughput;
        }
    }

    private void printJson(Object data) {
        try {
            System.out.println(mapper.writeValueAsString(data));
        } catch (JsonProcessingException e) {
            System.out.println("Bad data can't be written as json: " + e.getMessage());
        }
    }

    /** Callback which prints errors to stdout when the producer fails to send. */
    private class PrintInfoCallback implements Callback {

        private String key;
        private String value;

        PrintInfoCallback(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            synchronized (System.out) {
                if (e == null) {
                    VerifiableProducer.this.numAcked++;
                    printJson(new SuccessfulSend(this.key, this.value, recordMetadata));
                } else {
                    printJson(new FailedSend(this.key, this.value, topic, e));
                }
            }
        }
    }

    public void run(ThroughputThrottler throttler) {

        printJson(new StartupComplete());
        // negative maxMessages (-1) means "infinite"
        long maxMessages = (this.maxMessages < 0) ? Long.MAX_VALUE : this.maxMessages;

        for (long i = 0; i < maxMessages; i++) {
            if (this.stopProducing) {
                break;
            }
            long sendStartMs = System.currentTimeMillis();

            this.send(this.getKey(), this.getValue(i));

            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }
    }

    public static void main(String[] args) {
        ArgumentParser parser = argParser();
        if (args.length == 0) {
            parser.printHelp();
            Exit.exit(0);
        }

        try {
            final VerifiableProducer producer = createFromArgs(parser, args);

            final long startMs = System.currentTimeMillis();
            ThroughputThrottler throttler = new ThroughputThrottler(producer.throughput, startMs);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Trigger main thread to stop producing messages
                    producer.stopProducing = true;

                    // Flush any remaining messages
                    producer.close();

                    // Print a summary
                    long stopMs = System.currentTimeMillis();
                    double avgThroughput = 1000 * ((producer.numAcked) / (double) (stopMs - startMs));

                    producer.printJson(new ToolData(producer.numSent, producer.numAcked, producer.throughput, avgThroughput));
                }
            });

            producer.run(throttler);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            Exit.exit(1);
        }
    }

}
