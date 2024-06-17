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

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * ClientCompatibilityTest is invoked by the ducktape test client_compatibility_features_test.py to validate
 * client behavior when various broker versions are in use.  It runs various client operations and tests whether they
 * are supported or not.
 */
public class ClientCompatibilityTest {
    private static final Logger log = LoggerFactory.getLogger(ClientCompatibilityTest.class);

    static class TestConfig {
        final String bootstrapServer;
        final String topic;
        final boolean offsetsForTimesSupported;
        final boolean expectClusterId;
        final boolean expectRecordTooLargeException;
        final int numClusterNodes;
        final boolean createTopicsSupported;
        final boolean describeAclsSupported;
        final boolean describeConfigsSupported;
        final boolean idempotentProducerSupported;

        TestConfig(Namespace res) {
            this.bootstrapServer = res.getString("bootstrapServer");
            this.topic = res.getString("topic");
            this.offsetsForTimesSupported = res.getBoolean("offsetsForTimesSupported");
            this.expectClusterId = res.getBoolean("clusterIdSupported");
            this.expectRecordTooLargeException = res.getBoolean("expectRecordTooLargeException");
            this.numClusterNodes = res.getInt("numClusterNodes");
            this.createTopicsSupported = res.getBoolean("createTopicsSupported");
            this.describeAclsSupported = res.getBoolean("describeAclsSupported");
            this.describeConfigsSupported = res.getBoolean("describeConfigsSupported");
            this.idempotentProducerSupported = res.get("idempotentProducerSupported");
        }
    }

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("client-compatibility-test")
            .defaultHelp(true)
            .description("This tool is used to verify client compatibility guarantees.");
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
        parser.addArgument("--expect-record-too-large-exception")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("expectRecordTooLargeException")
            .metavar("EXPECT_RECORD_TOO_LARGE_EXCEPTION")
            .help("True if we should expect a RecordTooLargeException when trying to read from a topic " +
                  "that contains a message that is bigger than " + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG +
                  ".  This is pre-KIP-74 behavior.");
        parser.addArgument("--num-cluster-nodes")
            .action(store())
            .required(true)
            .type(Integer.class)
            .dest("numClusterNodes")
            .metavar("NUM_CLUSTER_NODES")
            .help("The number of cluster nodes we should expect to see from the AdminClient.");
        parser.addArgument("--create-topics-supported")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("createTopicsSupported")
            .metavar("CREATE_TOPICS_SUPPORTED")
            .help("Whether we should be able to create topics via the AdminClient.");
        parser.addArgument("--describe-acls-supported")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("describeAclsSupported")
            .metavar("DESCRIBE_ACLS_SUPPORTED")
            .help("Whether describeAcls is supported in the AdminClient.");
        parser.addArgument("--describe-configs-supported")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("describeConfigsSupported")
            .metavar("DESCRIBE_CONFIGS_SUPPORTED")
            .help("Whether describeConfigs is supported in the AdminClient.");
        parser.addArgument("--idempotent-producer-supported")
            .action(store())
            .required(true)
            .type(Boolean.class)
            .dest("idempotentProducerSupported")
            .metavar("IDEMPOTENT_PRODUCER_SUPPORTED")
            .help("Whether the producer supports idempotency.");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                Exit.exit(0);
            } else {
                parser.handleError(e);
                Exit.exit(1);
            }
        }
        TestConfig testConfig = new TestConfig(res);
        ClientCompatibilityTest test = new ClientCompatibilityTest(testConfig);
        try {
            test.run();
        } catch (Throwable t) {
            System.out.printf("FAILED: Caught exception %s%n%n", t.getMessage());
            t.printStackTrace();
            Exit.exit(1);
        }
        System.out.println("SUCCESS.");
        Exit.exit(0);
    }

    private static String toHexString(byte[] buf) {
        StringBuilder bld = new StringBuilder();
        for (byte b : buf) {
            bld.append(String.format("%02x", b));
        }
        return bld.toString();
    }

    private static void compareArrays(byte[] a, byte[] b) {
        if (!Arrays.equals(a, b)) {
            throw new RuntimeException("Arrays did not match: expected " + toHexString(a) + ", got " + toHexString(b));
        }
    }

    private final TestConfig testConfig;

    private final byte[] message1;

    private final byte[] message2;

    ClientCompatibilityTest(TestConfig testConfig) {
        this.testConfig = testConfig;
        long curTime = Time.SYSTEM.milliseconds();

        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putLong(curTime);
        this.message1 = buf.array();

        ByteBuffer buf2 = ByteBuffer.allocate(4096);
        for (long i = 0; i < buf2.capacity(); i += 8) {
            buf2.putLong(curTime + i);
        }
        this.message2 = buf2.array();
    }

    void run() throws Throwable {
        long prodTimeMs = Time.SYSTEM.milliseconds();
        testAdminClient();
        testProduce();
        testConsume(prodTimeMs);
    }

    public void testProduce() throws Exception {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        if (!testConfig.idempotentProducerSupported) {
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        }
        ByteArraySerializer serializer = new ByteArraySerializer();
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps, serializer, serializer);
        ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(testConfig.topic, message1);
        Future<RecordMetadata> future1 = producer.send(record1);
        ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(testConfig.topic, message2);
        Future<RecordMetadata> future2 = producer.send(record2);
        producer.flush();
        future1.get();
        future2.get();
        producer.close();
    }

    void testAdminClient() throws Throwable {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        try (final Admin client = Admin.create(adminProps)) {
            while (true) {
                Collection<Node> nodes = client.describeCluster().nodes().get();
                if (nodes.size() == testConfig.numClusterNodes) {
                    break;
                } else if (nodes.size() > testConfig.numClusterNodes) {
                    throw new KafkaException("Expected to see " + testConfig.numClusterNodes +
                        " nodes, but saw " + nodes.size());
                }
                Thread.sleep(1);
                log.info("Saw only {} cluster nodes.  Waiting to see {}.",
                    nodes.size(), testConfig.numClusterNodes);
            }

            testDescribeConfigsMethod(client);

            tryFeature("createTopics", testConfig.createTopicsSupported,
                () -> {
                    try {
                        client.createTopics(Collections.singleton(
                            new NewTopic("newtopic", 1, (short) 1))).all().get();
                    } catch (ExecutionException e) {
                        throw e.getCause();
                    }
                },
                () ->  createTopicsResultTest(client, Collections.singleton("newtopic"))
            );

            while (true) {
                Collection<TopicListing> listings = client.listTopics().listings().get();
                if (!testConfig.createTopicsSupported)
                    break;

                if (topicExists(listings, "newtopic"))
                    break;

                Thread.sleep(1);
                log.info("Did not see newtopic.  Retrying listTopics...");
            }

            tryFeature("describeAclsSupported", testConfig.describeAclsSupported,
                () -> {
                    try {
                        client.describeAcls(AclBindingFilter.ANY).values().get();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof SecurityDisabledException)
                            return;
                        throw e.getCause();
                    }
                });
        }
    }

    private void testDescribeConfigsMethod(final Admin client) throws Throwable {
        tryFeature("describeConfigsSupported", testConfig.describeConfigsSupported,
            () -> {
                try {
                    Collection<Node> nodes = client.describeCluster().nodes().get();

                    final ConfigResource configResource = new ConfigResource(
                        ConfigResource.Type.BROKER,
                        nodes.iterator().next().idString()
                    );

                    Map<ConfigResource, Config> brokerConfig =
                        client.describeConfigs(Collections.singleton(configResource)).all().get();

                    if (brokerConfig.get(configResource).entries().isEmpty()) {
                        throw new KafkaException("Expected to see config entries, but got zero entries");
                    }
                } catch (ExecutionException e) {
                    throw e.getCause();
                }
            });
    }

    private void createTopicsResultTest(Admin client, Collection<String> topics)
            throws InterruptedException, ExecutionException {
        while (true) {
            try {
                client.describeTopics(topics).allTopicNames().get();
                break;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException)
                    continue;
                throw e;
            }
        }
    }

    private boolean topicExists(Collection<TopicListing> listings, String topicName) {
        boolean foundTopic = false;
        for (TopicListing listing : listings) {
            if (listing.name().equals(topicName)) {
                if (listing.isInternal())
                    throw new KafkaException(String.format("Did not expect %s to be an internal topic.", topicName));
                foundTopic = true;
            }
        }
        return foundTopic;
    }

    private static class OffsetsForTime {
        Map<TopicPartition, OffsetAndTimestamp> result;

        @Override
        public String toString() {
            return result.toString();
        }
    }

    public static class ClientCompatibilityTestDeserializer implements Deserializer<byte[]>, ClusterResourceListener {
        private final boolean expectClusterId;

        ClientCompatibilityTestDeserializer(boolean expectClusterId) {
            this.expectClusterId = expectClusterId;
        }

        @Override
        public byte[] deserialize(String topic, byte[] data) {
            return data;
        }

        @Override
        public void onUpdate(ClusterResource clusterResource) {
            if (expectClusterId) {
                if (clusterResource.clusterId() == null) {
                    throw new RuntimeException("Expected cluster id to be supported, but it was null.");
                }
            } else {
                if (clusterResource.clusterId() != null) {
                    throw new RuntimeException("Expected cluster id to be null, but it was supported.");
                }
            }
        }
    }

    public void testConsume(final long prodTimeMs) throws Throwable {
        Properties consumerProps = new Properties();
        consumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testConfig.bootstrapServer);
        consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 512);
        ClientCompatibilityTestDeserializer deserializer =
            new ClientCompatibilityTestDeserializer(testConfig.expectClusterId);
        try (final KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps, deserializer, deserializer)) {
            final List<PartitionInfo> partitionInfos = consumer.partitionsFor(testConfig.topic);
            if (partitionInfos.isEmpty())
                throw new RuntimeException("Expected at least one partition for topic " + testConfig.topic);
            final Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
            final LinkedList<TopicPartition> topicPartitions = new LinkedList<>();
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                timestampsToSearch.put(topicPartition, prodTimeMs);
                topicPartitions.add(topicPartition);
            }
            final OffsetsForTime offsetsForTime = new OffsetsForTime();
            tryFeature("offsetsForTimes", testConfig.offsetsForTimesSupported,
                () -> offsetsForTime.result = consumer.offsetsForTimes(timestampsToSearch),
                () -> log.info("offsetsForTime = {}", offsetsForTime.result));
            // Whether or not offsetsForTimes works, beginningOffsets and endOffsets
            // should work.
            consumer.beginningOffsets(timestampsToSearch.keySet());
            consumer.endOffsets(timestampsToSearch.keySet());

            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);
            final Iterator<byte[]> iter = new Iterator<byte[]>() {
                private static final int TIMEOUT_MS = 10000;
                private Iterator<ConsumerRecord<byte[], byte[]>> recordIter = null;
                private byte[] next = null;

                private byte[] fetchNext() {
                    while (true) {
                        long curTime = Time.SYSTEM.milliseconds();
                        if (curTime - prodTimeMs > TIMEOUT_MS)
                            throw new RuntimeException("Timed out after " + TIMEOUT_MS + " ms.");
                        if (recordIter == null) {
                            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
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
                throw new RuntimeException("The first message in this topic was not ours. Please use a new topic when " +
                        "running this program.");
            }
            try {
                next = iter.next();
                if (testConfig.expectRecordTooLargeException) {
                    throw new RuntimeException("Expected to get a RecordTooLargeException when reading a record " +
                            "bigger than " + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
                }
                try {
                    compareArrays(message2, next);
                } catch (RuntimeException e) {
                    System.out.println("The second message in this topic was not ours. Please use a new " +
                        "topic when running this program.");
                    Exit.exit(1);
                }
            } catch (RecordTooLargeException e) {
                log.debug("Got RecordTooLargeException", e);
                if (!testConfig.expectRecordTooLargeException)
                    throw new RuntimeException("Got an unexpected RecordTooLargeException when reading a record " +
                        "bigger than " + ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG);
            }
            log.debug("Closing consumer.");
        }
        log.info("Closed consumer.");
    }

    private interface Invoker {
        void invoke() throws Throwable;
    }

    private interface ResultTester {
        void test() throws Throwable;
    }

    private void tryFeature(String featureName, boolean supported, Invoker invoker) throws Throwable {
        tryFeature(featureName, supported, invoker, () -> { });
    }

    private void tryFeature(String featureName, boolean supported, Invoker invoker, ResultTester resultTester)
            throws Throwable {
        try {
            invoker.invoke();
            log.info("Successfully used feature {}", featureName);
        } catch (UnsupportedVersionException e) {
            log.info("Got UnsupportedVersionException when attempting to use feature {}", featureName);
            if (supported) {
                throw new RuntimeException("Expected " + featureName + " to be supported, but it wasn't.", e);
            }
            return;
        }
        if (!supported) {
            throw new RuntimeException("Did not expect " + featureName + " to be supported, but it was.");
        }
        resultTester.test();
    }
}
