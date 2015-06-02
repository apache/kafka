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

package org.apache.kafka.clients.tools;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.json.simple.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 * Primarily intended for use with system testing, this producer prints metadata
 * in the form of JSON to stdout on each "send" request. For example, this helps
 * with end-to-end correctness tests by making externally visible which messages have been
 * acked and which have not.
 *
 * When used as a command-line tool, it produces a fixed number of increasing integers.
 * If logging is left enabled, log output on stdout can be easily ignored by checking
 * whether a given line is valid JSON.
 */
public class VerifiableProducer {

    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;
    
    OptionParser commandLineParser;
    Map<String, OptionSpec<?>> commandLineOptions = new HashMap<String, OptionSpec<?>>();
  
    String topic;
    private Properties producerProps = new Properties();
    private Producer<String, String> producer;
    private int numMessages;
    private long throughput;
  
    /** Construct with command-line arguments */
    public VerifiableProducer(String[] args) throws IOException {
        this.configureParser();
        this.parseCommandLineArgs(args);
        this.producer = new KafkaProducer<String, String>(producerProps);
    }
  
    /** Set up the command-line options. */
    private void configureParser() {
        this.commandLineParser = new OptionParser();
        ArgumentAcceptingOptionSpec<String> topicOpt = commandLineParser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
                .withRequiredArg()
                .describedAs("topic")
                .ofType(String.class);
        commandLineOptions.put("topic", topicOpt);
    
        ArgumentAcceptingOptionSpec<String>  brokerListOpt = commandLineParser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                .withRequiredArg()
                .describedAs("broker-list")
                .ofType(String.class);
        commandLineOptions.put("broker-list", brokerListOpt);
    
    
        ArgumentAcceptingOptionSpec<Integer>  numMessagesOpt = commandLineParser.accepts("num-messages", "REQUIRED: The number of messages to produce.")
                .withRequiredArg()
                .describedAs("num-messages")
                .ofType(Integer.class);
        commandLineOptions.put("num-messages", numMessagesOpt);

        ArgumentAcceptingOptionSpec<Long>  throughputOpt = commandLineParser.accepts("throughput", "REQUIRED: Average message throughput, in messages/sec.")
                .withRequiredArg()
                .describedAs("throughput")
                .ofType(Long.class);
        commandLineOptions.put("throughput", throughputOpt);
    
        OptionSpecBuilder helpOpt = commandLineParser.accepts("help", "Print this message.");
        commandLineOptions.put("help", helpOpt);
    }
  
    /** Validate command-line arguments and parse into properties object. */
    public void parseCommandLineArgs(String[] args) throws IOException {
  
        OptionSpec[] requiredArgs = new OptionSpec[]{commandLineOptions.get("topic"),
                                                     commandLineOptions.get("broker-list"),
                                                     commandLineOptions.get("num-messages"),
                                                     commandLineOptions.get("throughput")};
    
        OptionSet options = commandLineParser.parse(args);
        if (options.has(commandLineOptions.get("help"))) {
            commandLineParser.printHelpOn(System.out);
            System.exit(0);
        }
        checkRequiredArgs(commandLineParser, options, requiredArgs);
    
        this.numMessages = (Integer) options.valueOf("num-messages");
        this.topic = (String) options.valueOf("topic");
        this.throughput = (Long) options.valueOf("throughput");
    
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf("broker-list"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, "-1");
    
        // No producer retries
        producerProps.put("retries", "0");
    }
  
    private static void checkRequiredArgs(
            OptionParser parser, OptionSet options, OptionSpec[] required) throws IOException
    {
        for (OptionSpec arg : required) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument \"" + arg + "\"");
                parser.printHelpOn(System.err);
                System.exit(1);
            }
        }
    }
  
    /**
     * Produce a message with given value and no key.
     */
    public void send(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
        try {
            producer.send(record, new PrintInfoCallback(key, value));
        } catch (Exception e) {

            synchronized (System.out) {
                System.out.println(errorString(e, key, value, System.currentTimeMillis()));
            }
        }
    }
  
    /** Need to close the producer to flush any remaining messages if we're in async mode. */
    public void close() {
        producer.close();
    }
  
    /**
     * Return JSON string encapsulating basic information about the exception, as well
     * as the key and value which triggered the exception.
     */
    String errorString(Exception e, String key, String value, Long nowMs) {
        assert e != null : "Expected non-null exception.";
    
        JSONObject obj = new JSONObject();
        obj.put("class", this.getClass().toString());
        obj.put("name", "producer_send_error");
        
        obj.put("time_ms", nowMs);
        obj.put("exception", e.getClass().toString());
        obj.put("message", e.getMessage());
        obj.put("topic", this.topic);
        obj.put("key", key);
        obj.put("value", value);
        return obj.toJSONString();
    }
  
    String successString(RecordMetadata recordMetadata, String key, String value, Long nowMs) {
        assert recordMetadata != null : "Expected non-null recordMetadata object.";
    
        JSONObject obj = new JSONObject();
        obj.put("class", this.getClass().toString());
        obj.put("name", "producer_send_success");
        
        obj.put("time_ms", nowMs);
        obj.put("topic", this.topic);
        obj.put("partition", recordMetadata.partition());
        obj.put("offset", recordMetadata.offset());
        obj.put("key", key);
        obj.put("value", value);
        return obj.toJSONString();
    }
  
    /**
     * Callback which prints errors to stdout when the producer fails to send.
     */
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
                    System.out.println(successString(recordMetadata, this.key, this.value, System.currentTimeMillis()));
                } else {
                    System.out.println(errorString(e, this.key, this.value, System.currentTimeMillis()));
                }
            }
        }
    }
  
    public static void main(String[] args) throws IOException {
    
        VerifiableProducer producer = new VerifiableProducer(args);

        long sleepTimeNs = NS_PER_SEC / producer.throughput;
        long sleepDeficitNs = 0;
        long startMs = System.currentTimeMillis();
        
        for (int i = 0; i < producer.numMessages; i++) {
            long sendStartMs = System.currentTimeMillis();
            producer.send(null, String.format("%d", i));
            
            // throttle message throughput by sleeping, on average,
            // (NS_PER_SEC / producer.throughput) nanoseconds between each sent message
            if (producer.throughput > 0) {
                float elapsedMs = (sendStartMs - startMs) / 1000.f;
                if (elapsedMs > 0 && i / elapsedMs > producer.throughput) {
                    sleepDeficitNs += sleepTimeNs;
                    
                    // If enough sleep deficit has accumulated, sleep a little
                    if (sleepDeficitNs >= MIN_SLEEP_NS) {
                        long sleepMs = sleepDeficitNs / 1000000;
                        long sleepNs = sleepDeficitNs - sleepMs * 1000000;
                        
                        long sleepStartNs = System.nanoTime();
                        try {
                            Thread.sleep(sleepMs, (int) sleepNs);
                            sleepDeficitNs = 0;
                        } catch (InterruptedException e) {
                            // If sleep is cut short, reduce deficit by the amount of
                            // time we actually spent sleeping
                            long sleepElapsedNs = System.nanoTime() - sleepStartNs;
                            if (sleepElapsedNs <= sleepDeficitNs) {
                                sleepDeficitNs -= sleepElapsedNs;
                            }
                        }
                        
                    }
                }
            }
        }
        producer.close();
        
        long stopMs = System.currentTimeMillis();
        double avgThroughput = 1000 * ((producer.numMessages) / (double) (stopMs - startMs));

        JSONObject obj = new JSONObject();
        obj.put("class", producer.getClass().toString());
        obj.put("name", "tool_data");
        obj.put("target_throughput", producer.throughput);
        obj.put("avg_throughput", avgThroughput);
        System.out.println(obj.toJSONString());
    }
}
