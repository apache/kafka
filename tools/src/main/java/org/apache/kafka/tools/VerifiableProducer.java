/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

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

    String topic;
    private Producer<String, String> producer;
    // If maxMessages < 0, produce until the process is killed externally
    private long maxMessages = -1;

    // Number of messages for which acks were received
    private long numAcked = 0;

    // Number of send attempts
    private long numSent = 0;

    // Throttle message throughput if this is set >= 0
    private long throughput;

    // Hook to trigger producing thread to stop sending messages
    private boolean stopProducing = false;

    // Prefix (plus a dot separator) added to every value produced by verifiable producer
    // if null, then values are produced without a prefix
    private Integer valuePrefix;

    public VerifiableProducer(
            Properties producerProps, String topic, int throughput, int maxMessages, Integer valuePrefix) {

        this.topic = topic;
        this.throughput = throughput;
        this.maxMessages = maxMessages;
        this.producer = new KafkaProducer<String, String>(producerProps);
        this.valuePrefix = valuePrefix;
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
                .help("Acks required on each produced message. See Kafka docs on request.required.acks for details.");

        parser.addArgument("--producer.config")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("CONFIG_FILE")
                .help("Producer config properties file.");

        parser.addArgument("--value-prefix")
            .action(store())
            .required(false)
            .type(Integer.class)
            .metavar("VALUE-PREFIX")
            .dest("valuePrefix")
            .help("If specified, each produced value will have this prefix with a dot separator");

        return parser;
    }
    
    /**
     * Read a properties file from the given path
     * @param filename The path of the file to read
     *                 
     * Note: this duplication of org.apache.kafka.common.utils.Utils.loadProps is unfortunate 
     * but *intentional*. In order to use VerifiableProducer in compatibility and upgrade tests, 
     * we use VerifiableProducer from trunk tools package, and run it against 0.8.X.X kafka jars.
     * Since this method is not in Utils in the 0.8.X.X jars, we have to cheat a bit and duplicate.
     */
    public static Properties loadProps(String filename) throws IOException, FileNotFoundException {
        Properties props = new Properties();
        InputStream propStream = null;
        try {
            propStream = new FileInputStream(filename);
            props.load(propStream);
        } finally {
            if (propStream != null)
                propStream.close();
        }
        return props;
    }
    
    /** Construct a VerifiableProducer object from command-line arguments. */
    public static VerifiableProducer createFromArgs(String[] args) {
        ArgumentParser parser = argParser();
        VerifiableProducer producer = null;

        try {
            Namespace res;
            res = parser.parseArgs(args);

            int maxMessages = res.getInt("maxMessages");
            String topic = res.getString("topic");
            int throughput = res.getInt("throughput");
            String configFile = res.getString("producer.config");
            Integer valuePrefix = res.getInt("valuePrefix");

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, res.getString("brokerList"));
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                              "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                              "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.ACKS_CONFIG, Integer.toString(res.getInt("acks")));
            // No producer retries
            producerProps.put("retries", "0");
            if (configFile != null) {
                try {
                    producerProps.putAll(loadProps(configFile));
                } catch (IOException e) {
                    throw new ArgumentParserException(e.getMessage(), parser);
                }
            }

            producer = new VerifiableProducer(producerProps, topic, throughput, maxMessages, valuePrefix);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }

        return producer;
    }

    /** Produce a message with given key and value. */
    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        numSent++;
        try {
            producer.send(record, new PrintInfoCallback(key, value));
        } catch (Exception e) {

            synchronized (System.out) {
                System.out.println(errorString(e, key, value, System.currentTimeMillis()));
            }
        }
    }

    /** Returns a string to publish: ether 'valuePrefix'.'val' or 'val' **/
    public String getValue(long val) {
        if (this.valuePrefix != null) {
            return String.format("%d.%d", this.valuePrefix.intValue(), val);
        }
        return String.format("%d", val);
    }

    /** Close the producer to flush any remaining messages. */
    public void close() {
        producer.close();
        System.out.println(shutdownString());
    }

    String shutdownString() {
        Map<String, Object> data = new HashMap<>();
        data.put("class", this.getClass().toString());
        data.put("name", "shutdown_complete");
        return toJsonString(data);
    }

    /**
     * Return JSON string encapsulating basic information about the exception, as well
     * as the key and value which triggered the exception.
     */
    String errorString(Exception e, String key, String value, Long nowMs) {
        assert e != null : "Expected non-null exception.";

        Map<String, Object> errorData = new HashMap<>();
        errorData.put("class", this.getClass().toString());
        errorData.put("name", "producer_send_error");

        errorData.put("time_ms", nowMs);
        errorData.put("exception", e.getClass().toString());
        errorData.put("message", e.getMessage());
        errorData.put("topic", this.topic);
        errorData.put("key", key);
        errorData.put("value", value);

        return toJsonString(errorData);
    }

    String successString(RecordMetadata recordMetadata, String key, String value, Long nowMs) {
        assert recordMetadata != null : "Expected non-null recordMetadata object.";

        Map<String, Object> successData = new HashMap<>();
        successData.put("class", this.getClass().toString());
        successData.put("name", "producer_send_success");

        successData.put("time_ms", nowMs);
        successData.put("topic", this.topic);
        successData.put("partition", recordMetadata.partition());
        successData.put("offset", recordMetadata.offset());
        successData.put("key", key);
        successData.put("value", value);

        return toJsonString(successData);
    }

    private String toJsonString(Map<String, Object> data) {
        String json;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            json = "Bad data can't be written as json: " + e.getMessage();
        }
        return json;
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
                    System.out.println(successString(recordMetadata, this.key, this.value, System.currentTimeMillis()));
                } else {
                    System.out.println(errorString(e, this.key, this.value, System.currentTimeMillis()));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {

        final VerifiableProducer producer = createFromArgs(args);
        final long startMs = System.currentTimeMillis();
        boolean infinite = producer.maxMessages < 0;

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

                Map<String, Object> data = new HashMap<>();
                data.put("class", producer.getClass().toString());
                data.put("name", "tool_data");
                data.put("sent", producer.numSent);
                data.put("acked", producer.numAcked);
                data.put("target_throughput", producer.throughput);
                data.put("avg_throughput", avgThroughput);

                System.out.println(producer.toJsonString(data));
            }
        });

        ThroughputThrottler throttler = new ThroughputThrottler(producer.throughput, startMs);
        long maxMessages = infinite ? Long.MAX_VALUE : producer.maxMessages;
        for (long i = 0; i < maxMessages; i++) {
            if (producer.stopProducing) {
                break;
            }
            long sendStartMs = System.currentTimeMillis();

            producer.send(null, producer.getValue(i));

            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }
    }

}
