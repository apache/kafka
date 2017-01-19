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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ObsoleteBrokerException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.Future;

public class CompatibilityTest {
    private static final Logger log = LoggerFactory.getLogger(CompatibilityTest.class);

    static class TestConfig {
        final String bootstrapServer;
        final String topic;
        final boolean offsetsForTimesSupported;
        final boolean expectClusterId;

        TestConfig(Namespace res) {
            this.bootstrapServer = res.getString("bootstrapServer");
            this.topic = res.getString("topic");
            this.offsetsForTimesSupported = res.getBoolean("offsetsForTimesSupported");
            this.expectClusterId = res.getBoolean("clusterIdSupported");
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("compatibility-test")
            .defaultHelp(true)
            .description("This tool is used to verify compatibility guarantees.");
        parser.addArgument("--topic")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("topic")
            .metavar("TOPIC")
            .help("the compatibility test will produce messages to this topic");
        parser.addArgument("--bootstrap-server")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("bootstrapServer")
            .metavar("BOOTSTRAP_SERVER")
            .help("The server(s) to use for bootstrapping");
        parser.addArgument("--offsets-for-times-supported")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("offsetsForTimesSupported")
            .metavar("OFFSETS_FOR_TIMES_SUPPORTED")
            .help("True if KafkaConsumer#offsetsForTimes is supported by the current broker version");
        parser.addArgument("--cluster-id-supported")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("clusterIdSupported")
            .metavar("CLUSTER_ID_SUPPORTED")
            .help("True if cluster IDs are supported.  False if cluster ID always appears as null.");
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
        TestConfig testConfig = new TestConfig(res);
        CompatibilityTest compatibilityTest = new CompatibilityTest(testConfig);
        try {
            compatibilityTest.run();
        } catch (Throwable t) {
            System.out.printf("FAILED: Caught exception %s\n\n", t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
        System.out.printf("SUCCESS.\n");
        System.exit(0);
    }

    private static byte[] asByteArray(long a) {
        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(a);
        return buf.array();
    }

    private static byte[] asByteArray(long a, long b) {
        ByteBuffer buf = ByteBuffer.allocate(16);
        buf.putLong(a);
        buf.putLong(b);
        return buf.array();
    }

    private static String toHexString(byte[] buf) {
        StringBuilder bld = new StringBuilder();
        for (byte b: buf) {
            bld.append(String.format("%02x", b));
        }
        return bld.toString();
    }

    private static void compareArrays(byte[] a, byte[] b) {
        if (!Arrays.equals(a, b)) {
            throw new RuntimeException("Arrays did not match: expected " + toHexString(a) +
                ", got " + toHexString(b));
        }
    }

    private final TestConfig testConfig;

    private final byte[] message1;

    private final byte[] message2;

    CompatibilityTest(TestConfig testConfig) {
        this.testConfig = testConfig;
        long curTime = Time.SYSTEM.milliseconds();
        this.message1 = asByteArray(curTime);
        this.message2 = asByteArray(curTime, curTime);
    }

    void run() throws Exception {
        long prodTimeMs = Time.SYSTEM.milliseconds();
        testProduce();
        testConsume(prodTimeMs);
    }

    public void testProduce() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        ByteArraySerializer serializer = new ByteArraySerializer();
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps, serializer, serializer);
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(testConfig.topic, message1);
        Future<RecordMetadata> future1 = producer.send(record1, null);
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(testConfig.topic, message2);
        Future<RecordMetadata> future2 = producer.send(record2, null);
        producer.flush();
        future1.get();
        future2.get();
        producer.close();
    }

    private static class OffsetsForTime {
        Map<TopicPartition, OffsetAndTimestamp> result;

        @Override
        public String toString() {
            return Utils.mkString(result);
        }
    }

    public static class CompatibilityTestDeserializer implements Deserializer<byte[]>, ClusterResourceListener {
        private final boolean expectClusterId;

        CompatibilityTestDeserializer(boolean expectClusterId) {
            this.expectClusterId = expectClusterId;
        }

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

    public void testConsume(final long prodTimeMs) throws Exception {
        Properties consumerProps = new Properties();
        consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        CompatibilityTestDeserializer deserializer = new CompatibilityTestDeserializer(testConfig.expectClusterId);
        final KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<byte[], byte[]>(consumerProps, deserializer, deserializer);
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(testConfig.topic);
        if (partitionInfos.size() < 1)
            throw new RuntimeException("expected at least one partition for topic " + testConfig.topic);
        final Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        final LinkedList<TopicPartition> topicPartitions = new LinkedList<>();
        for (PartitionInfo partitionInfo: partitionInfos) {
            TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
            timestampsToSearch.put(topicPartition, prodTimeMs);
            topicPartitions.add(topicPartition);
        }
        final OffsetsForTime offsetsForTime = new OffsetsForTime();
        tryFeature("offsetsForTimes", testConfig.offsetsForTimesSupported,
                new Runnable() {
                    @Override
                    public void run() {
                        offsetsForTime.result = consumer.offsetsForTimes(timestampsToSearch);
                    }
                },
                new Runnable() {
                    @Override
                    public void run() {
                        log.info("offsetsForTime = {}", offsetsForTime.result);
                    }
                });
        // Whether or not offsetsForTimes works, beginningOffsets and endOffsets
        // should work.
        consumer.beginningOffsets(timestampsToSearch.keySet());
        consumer.endOffsets(timestampsToSearch.keySet());

        consumer.assign(topicPartitions);
        consumer.seekToBeginning(topicPartitions);
        final Iterator<byte[]> iter = new Iterator<byte[]>() {
            private final int timeoutMs = 10000;
            private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = null;
            private byte[] next = null;

            private byte[] fetchNext() {
                while (true) {
                    long curTime = Time.SYSTEM.milliseconds();
                    if (curTime - prodTimeMs > timeoutMs)
                        throw new RuntimeException("Timed out after " + timeoutMs + " ms.");
                    if (recordIter == null) {
                        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
                        recordIter = records.iterator();
                    }
                    if (recordIter.hasNext())
                        return recordIter.next().value();
                    recordIter = null;
                }
            }

            @Override
            public boolean hasNext() {
                if (next != null)
                    return true;
                next = fetchNext();
                return next != null;
            }

            @Override
            public byte[] next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                byte[] cur = next;
                next = null;
                return cur;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        byte[] next = iter.next();
        try {
            compareArrays(message1, next);
            log.debug("Found first message...");
        } catch (RuntimeException e) {
            throw new RuntimeException("The first message in this topic was not ours.  Please use a new " +
                "topic when running this program.\n");
        }
        next = iter.next();
        try {
            compareArrays(message2, next);
            log.debug("Found second message...");
        } catch (RuntimeException e) {
            throw new RuntimeException("The first message in this topic was not ours.  Please use a new " +
                "topic when running this program.\n");
        }
        log.debug("Closing consumer.");
        consumer.close();
        log.info("Closed consumer.");
    }

    private void tryFeature(String featureName, boolean supported, Runnable invoker, Runnable resultTester) {
        try {
            invoker.run();
            log.info("Successfully used feature {}", featureName);
        } catch (ObsoleteBrokerException e) {
            log.info("Got ObsoleteBrokerException when attempting to use feature {}", featureName);
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
}
