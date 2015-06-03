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
 * When used as a command-line tool, it produces increasing integers. It will produce a 
 * fixed number of messages unless the default max-messages -1 is used, in which case
 * it produces indefinitely.
 *  
 * If logging is left enabled, log output on stdout can be easily ignored by checking
 * whether a given line is valid JSON.
 */
public class VerifiableProducer {

    OptionParser commandLineParser;
    Map<String, OptionSpec<?>> commandLineOptions = new HashMap<String, OptionSpec<?>>();
  
    String topic;
    private Properties producerProps = new Properties();
    private Producer<String, String> producer;
    // If maxMessages < 0, produce until the process is killed externally
    private long maxMessages = -1;
    
    // Number of messages for which acks were received
    private long numAcked = 0;
    
    // Number of send attempts
    private long numSent = 0;
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
                .required()
                .describedAs("topic")
                .ofType(String.class);
        commandLineOptions.put("topic", topicOpt);
    
        ArgumentAcceptingOptionSpec<String>  brokerListOpt = commandLineParser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
                .withRequiredArg()
                .required()
                .describedAs("broker-list")
                .ofType(String.class);
        commandLineOptions.put("broker-list", brokerListOpt);
    
    
        ArgumentAcceptingOptionSpec<String>  numMessagesOpt = commandLineParser.accepts("max-messages", "Produce this many messages. Default: -1, produces messages until the process is killed externally.")
                .withOptionalArg()
                .defaultsTo("-1")
                .describedAs("max-messages")
                .ofType(String.class);
        commandLineOptions.put("max-messages", numMessagesOpt);

        ArgumentAcceptingOptionSpec<String>  throughputOpt = commandLineParser.accepts("throughput", "Average message throughput, in messages/sec. Default: -1, results in no throttling.")
                .withOptionalArg()
                .defaultsTo("-1")
                .describedAs("throughput")
                .ofType(String.class);
        commandLineOptions.put("throughput", throughputOpt);

        ArgumentAcceptingOptionSpec<String>  acksOpt = commandLineParser.accepts("acks", "number of acks required. Default: -1")
                .withOptionalArg()
                .defaultsTo("-1")
                .describedAs("acks")
                .ofType(String.class);
        commandLineOptions.put("acks", acksOpt);
    
        OptionSpecBuilder helpOpt = commandLineParser.accepts("help", "Print this message.");
        commandLineOptions.put("help", helpOpt);
    }
  
    /** Validate command-line arguments and parse into properties object. */
    public void parseCommandLineArgs(String[] args) throws IOException {

        OptionSet options = commandLineParser.parse(args);
        if (options.has(commandLineOptions.get("help"))) {
            commandLineParser.printHelpOn(System.out);
            System.exit(0);
        }

        this.maxMessages = Integer.parseInt((String) options.valueOf("max-messages"));
        this.topic = (String) options.valueOf("topic");
        this.throughput = Long.parseLong((String) options.valueOf("throughput"));
    
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf("broker-list"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put(ProducerConfig.ACKS_CONFIG, options.valueOf("acks"));
    
        // No producer retries
        producerProps.put("retries", "0");
    }
  
    /**
     * Produce a message with given value and no key.
     */
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
                    VerifiableProducer.this.numAcked++;
                    System.out.println(successString(recordMetadata, this.key, this.value, System.currentTimeMillis()));
                } else {
                    System.out.println(errorString(e, this.key, this.value, System.currentTimeMillis()));
                }
            }
        }
    }
  
    public static void main(String[] args) throws IOException {
        
        final VerifiableProducer producer = new VerifiableProducer(args);
        final long startMs = System.currentTimeMillis();
        boolean infinite = producer.maxMessages < 0;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                producer.close();

                long stopMs = System.currentTimeMillis();
                double avgThroughput = 1000 * ((producer.numAcked) / (double) (stopMs - startMs));

                JSONObject obj = new JSONObject();
                obj.put("class", producer.getClass().toString());
                obj.put("name", "tool_data");
                obj.put("sent", producer.numSent);
                obj.put("acked", producer.numAcked);
                obj.put("target_throughput", producer.throughput);
                obj.put("avg_throughput", avgThroughput);
                System.out.println(obj.toJSONString());
            }
        });
        
        MessageThroughputThrottler throttler = new MessageThroughputThrottler(producer.throughput, startMs);
        for (int i = 0; i < producer.maxMessages || infinite; i++) {
            long sendStartMs = System.currentTimeMillis();
            producer.send(null, String.format("%d", i));
            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }
    }
}
