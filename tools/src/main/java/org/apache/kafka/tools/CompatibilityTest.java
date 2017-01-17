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

import static net.sourceforge.argparse4j.impl.Arguments.store;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ObsoleteBrokerException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CompatibilityTest {
    public static class CompatibilityTestDeserializer implements Deserializer<byte[]>, ClusterResourceListener {
        static volatile boolean expectClusterId = false;

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            // nothing to do
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }

        @Override
        public void close() {
            // nothing to do
        }

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            if (expectClusterId) {
                if (clusterResource.clusterId() == null) {
                    throw new RuntimeException("expected cluster id to be " +
                            "supported, but it was null.");
                }
            } else {
                if (clusterResource.clusterId() != null) {
                    throw new RuntimeException("expected cluster id to be " +
                            "null, but it was supported.");
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
        String bootstrapServer = res.getString("bootstrap_server");
        String topic = res.getString("topic");
        boolean offsetsForTimesSupported = res.getBoolean("offsets_for_times_supported");
        CompatibilityTestDeserializer.expectClusterId =
                res.getBoolean("cluster_id_supported");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        testProduce(topic, producerProps);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.tools.CompatibilityTest$CompatibilityTestDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        testConsume(topic, consumerProps, offsetsForTimesSupported);

    }

    public static void testProduce(String topic, Properties props) throws Exception {
        byte[] payload = new byte[] {0x61, 0x62, 0x63};
        ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topic, payload);
        CompatibilityTestSenderCompletion cb =
                new CompatibilityTestSenderCompletion();
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<byte[], byte[]>(props);
        producer.send(record, cb);
        cb.verify();
        producer.close();
    }

    private static class OffsetsForTime {
        Map<TopicPartition, OffsetAndTimestamp> result;

        @Override
        public String toString() {
            return Utils.mkString(result);
        }
    }

    public static void testConsume(String topic, Properties props,
                                   boolean offsetsForTimesSupported) throws Exception {
        byte[] payload = new byte[] {0x61, 0x62, 0x63};
        CompatibilityTestSenderCompletion cb =
                new CompatibilityTestSenderCompletion();
        final KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<byte[], byte[]>(props);
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos.size() < 1) {
            throw new RuntimeException("expected at least one partition for topic " + topic);
        }
        final Map<TopicPartition, Long> timestampsToSearch =
                new HashMap<TopicPartition, Long>();
        for (PartitionInfo partitionInfo: partitionInfos) {
            timestampsToSearch.put(
                    new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), 0L);
        }

        final OffsetsForTime offsetsForTime = new OffsetsForTime();
        tryFeature("offsetsForTimes", offsetsForTimesSupported,
                new Runnable() {
                    @Override
                    public void run() {
                        offsetsForTime.result =
                                consumer.offsetsForTimes(timestampsToSearch);
                    }
                },
                new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("offsetsForTime = " + offsetsForTime.result);
                    }
                });

        // Whether or not offsetsForTimes works, beginningOffsets and endOffsets
        // should work.
        consumer.beginningOffsets(timestampsToSearch.keySet());
        consumer.endOffsets(timestampsToSearch.keySet());

        consumer.close();
    }

    private static void tryFeature(String featureName, boolean supported,
                                   Runnable invoker, Runnable resultTester) {
        try {
            invoker.run();
        } catch (ObsoleteBrokerException e) {
            if (supported) {
                throw new RuntimeException("Expected " + featureName +
                        " to be supported, but it wasn't.", e);
            }
            return;
        }
        if (!supported) {
            throw new RuntimeException("Did not expect " + featureName +
                    " to be supported, but it was.");
        }
        resultTester.run();
    }

    private static class CompatibilityTestSenderCompletion implements Callback {
        private CountDownLatch latch = new CountDownLatch(1);
        volatile private boolean done = false;
        volatile private Exception exception;

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            this.done = true;
            this.exception = exception;
            latch.countDown();
        }

        void verify() throws Exception {
            while (!done) {
                latch.await();
            }
            if (exception != null) {
                throw exception;
            }
        }
    }

    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("compatibility-test")
                .defaultHelp(true)
                .description("This tool is used to verify compatibility guarantees.");
        parser.addArgument("--topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("the compatibility test will produce messages to this topic");
        parser.addArgument("--bootstrap-server")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("BOOTSTRAP_SERVER")
                .help("The server(s) to use for bootstrapping");
        parser.addArgument("--offsets-for-times-supported")
                .action(store())
                .required(true)
                .type(Boolean.class)
                .metavar("OFFSETS_FOR_TIMES_SUPPORTED")
                .help("True if KafkaConsumer#offsetsForTimes is supported by the " +
                        "current broker version");
        parser.addArgument("--cluster-id-supported")
                .action(store())
                .required(true)
                .type(Boolean.class)
                .metavar("CLUSTER_ID_SUPPORTED")
                .help("True if cluster IDs are supported.  " +
                      "False if cluster ID always appears as null.");
        return parser;
    }
}
