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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Helper functions for writing unit tests
 */
public class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    public static final File IO_TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

    public static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String DIGITS = "0123456789";
    public static final String LETTERS_AND_DIGITS = LETTERS + DIGITS;

    /* A consistent random number generator to make tests repeatable */
    public static final Random SEEDED_RANDOM = new Random(192348092834L);
    public static final Random RANDOM = new Random();
    public static final long DEFAULT_MAX_WAIT_MS = 15000;

    public static Cluster singletonCluster() {
        return clusterWith(1);
    }

    public static Cluster singletonCluster(final Map<String, Integer> topicPartitionCounts) {
        return clusterWith(1, topicPartitionCounts);
    }

    public static Cluster singletonCluster(final String topic, final int partitions) {
        return clusterWith(1, topic, partitions);
    }

    public static Cluster clusterWith(int nodes) {
        return clusterWith(nodes, new HashMap<String, Integer>());
    }

    public static Cluster clusterWith(final int nodes, final Map<String, Integer> topicPartitionCounts) {
        final Node[] ns = new Node[nodes];
        for (int i = 0; i < nodes; i++)
            ns[i] = new Node(i, "localhost", 1969);
        final List<PartitionInfo> parts = new ArrayList<>();
        for (final Map.Entry<String, Integer> topicPartition : topicPartitionCounts.entrySet()) {
            final String topic = topicPartition.getKey();
            final int partitions = topicPartition.getValue();
            for (int i = 0; i < partitions; i++)
                parts.add(new PartitionInfo(topic, i, ns[i % ns.length], ns, ns));
        }
        return new Cluster("kafka-cluster", asList(ns), parts, Collections.emptySet(), Topic.INTERNAL_TOPICS);
    }

    public static MetadataResponse metadataUpdateWith(final int numNodes,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith("kafka-cluster", numNodes, topicPartitionCounts);
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith(clusterId, numNodes, Collections.emptyMap(), topicPartitionCounts, MetadataResponse.PartitionMetadata::new);
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts) {
        return metadataUpdateWith(clusterId, numNodes, topicErrors, topicPartitionCounts, MetadataResponse.PartitionMetadata::new);
    }

    public static MetadataResponse metadataUpdateWith(final String clusterId,
                                                      final int numNodes,
                                                      final Map<String, Errors> topicErrors,
                                                      final Map<String, Integer> topicPartitionCounts,
                                                      final PartitionMetadataSupplier partitionSupplier) {
        final List<Node> nodes = new ArrayList<>(numNodes);
        for (int i = 0; i < numNodes; i++)
            nodes.add(new Node(i, "localhost", 1969 + i));

        List<MetadataResponse.TopicMetadata> topicMetadata = new ArrayList<>();
        for (Map.Entry<String, Integer> topicPartitionCountEntry : topicPartitionCounts.entrySet()) {
            String topic = topicPartitionCountEntry.getKey();
            int numPartitions = topicPartitionCountEntry.getValue();

            List<MetadataResponse.PartitionMetadata> partitionMetadata = new ArrayList<>(numPartitions);
            for (int i = 0; i < numPartitions; i++) {
                Node leader = nodes.get(i % nodes.size());
                List<Node> replicas = Collections.singletonList(leader);
                partitionMetadata.add(partitionSupplier.supply(
                        Errors.NONE, i, leader, Optional.empty(), replicas, replicas, replicas));
            }

            topicMetadata.add(new MetadataResponse.TopicMetadata(Errors.NONE, topic,
                    Topic.isInternal(topic), partitionMetadata));
        }

        for (Map.Entry<String, Errors> topicErrorEntry : topicErrors.entrySet()) {
            String topic = topicErrorEntry.getKey();
            topicMetadata.add(new MetadataResponse.TopicMetadata(topicErrorEntry.getValue(), topic,
                    Topic.isInternal(topic), Collections.emptyList()));
        }

        return new MetadataResponse(nodes, clusterId, 0, topicMetadata);
    }

    @FunctionalInterface
    public interface PartitionMetadataSupplier {
        MetadataResponse.PartitionMetadata supply(Errors error,
                              int partition,
                              Node leader,
                              Optional<Integer> leaderEpoch,
                              List<Node> replicas,
                              List<Node> isr,
                              List<Node> offlineReplicas);
    }

    public static Cluster clusterWith(final int nodes, final String topic, final int partitions) {
        return clusterWith(nodes, Collections.singletonMap(topic, partitions));
    }

    /**
     * Generate an array of random bytes
     *
     * @param size The size of the array
     */
    public static byte[] randomBytes(final int size) {
        final byte[] bytes = new byte[size];
        SEEDED_RANDOM.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generate a random string of letters and digits of the given length
     *
     * @param len The length of the string
     * @return The random string
     */
    public static String randomString(final int len) {
        final StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; i++)
            b.append(LETTERS_AND_DIGITS.charAt(SEEDED_RANDOM.nextInt(LETTERS_AND_DIGITS.length())));
        return b.toString();
    }

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        final File file = File.createTempFile("kafka", ".tmp");
        file.deleteOnExit();

        return file;
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with the given prefix.
     *
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(final String prefix) {
        return tempDirectory(null, prefix);
    }

    /**
     * Create a temporary relative directory in the default temporary-file directory with a
     * prefix of "kafka-"
     *
     * @return the temporary directory just created.
     */
    public static File tempDirectory() {
        return tempDirectory(null);
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     * @param parent The parent folder path name, if null using the default temporary-file directory
     * @param prefix The prefix of the temporary directory, if null using "kafka-" as default prefix
     */
    public static File tempDirectory(final Path parent, String prefix) {
        final File file;
        prefix = prefix == null ? "kafka-" : prefix;
        try {
            file = parent == null ?
                Files.createTempDirectory(prefix).toFile() : Files.createTempDirectory(parent, prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }
        file.deleteOnExit();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Utils.delete(file);
                } catch (IOException e) {
                    log.error("Error deleting {}", file.getAbsolutePath(), e);
                }
            }
        });

        return file;
    }

    public static Properties producerConfig(final String bootstrapServers,
                                            final Class keySerializer,
                                            final Class valueSerializer,
                                            final Properties additional) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.putAll(additional);
        return properties;
    }

    public static Properties producerConfig(final String bootstrapServers, final Class keySerializer, final Class valueSerializer) {
        return producerConfig(bootstrapServers, keySerializer, valueSerializer, new Properties());
    }

    public static Properties consumerConfig(final String bootstrapServers,
                                            final String groupId,
                                            final Class keyDeserializer,
                                            final Class valueDeserializer,
                                            final Properties additional) {

        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        consumerConfig.putAll(additional);
        return consumerConfig;
    }

    public static Properties consumerConfig(final String bootstrapServers,
                                            final String groupId,
                                            final Class keyDeserializer,
                                            final Class valueDeserializer) {
        return consumerConfig(bootstrapServers,
            groupId,
            keyDeserializer,
            valueDeserializer,
            new Properties());
    }

    /**
     * returns consumer config with random UUID for the Group ID
     */
    public static Properties consumerConfig(final String bootstrapServers, final Class keyDeserializer, final Class valueDeserializer) {
        return consumerConfig(bootstrapServers,
            UUID.randomUUID().toString(),
            keyDeserializer,
            valueDeserializer,
            new Properties());
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final TestCondition testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, () -> conditionDetails);
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final TestCondition testCondition, final Supplier<String> conditionDetailsSupplier) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, conditionDetailsSupplier);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs, String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, maxWaitMs, () -> conditionDetails);
    }
    
    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final TestCondition testCondition, final long maxWaitMs, Supplier<String> conditionDetailsSupplier) throws InterruptedException {
        final long startTime = System.currentTimeMillis();

        boolean testConditionMet;
        while (!(testConditionMet = testCondition.conditionMet()) && ((System.currentTimeMillis() - startTime) < maxWaitMs)) {
            Thread.sleep(Math.min(maxWaitMs, 100L));
        }

        // don't re-evaluate testCondition.conditionMet() because this might slow down some tests significantly (this
        // could be avoided by making the implementations more robust, but we have a large number of such implementations
        // and it's easier to simply avoid the issue altogether)
        if (!testConditionMet) {
            String conditionDetailsSupplied = conditionDetailsSupplier != null ? conditionDetailsSupplier.get() : null;
            String conditionDetails = conditionDetailsSupplied != null ? conditionDetailsSupplied : "";
            throw new AssertionError("Condition not met within timeout " + maxWaitMs + ". " + conditionDetails);
        }
    }

    /**
     * Checks if a cluster id is valid.
     * @param clusterId
     */
    public static void isValidClusterId(String clusterId) {
        assertNotNull(clusterId);

        // Base 64 encoded value is 22 characters
        assertEquals(clusterId.length(), 22);

        Pattern clusterIdPattern = Pattern.compile("[a-zA-Z0-9_\\-]+");
        Matcher matcher = clusterIdPattern.matcher(clusterId);
        assertTrue(matcher.matches());

        // Convert into normal variant and add padding at the end.
        String originalClusterId = String.format("%s==", clusterId.replace("_", "/").replace("-", "+"));
        byte[] decodedUuid = Base64.getDecoder().decode(originalClusterId);

        // We expect 16 bytes, same as the input UUID.
        assertEquals(decodedUuid.length, 16);

        //Check if it can be converted back to a UUID.
        try {
            ByteBuffer uuidBuffer = ByteBuffer.wrap(decodedUuid);
            new UUID(uuidBuffer.getLong(), uuidBuffer.getLong()).toString();
        } catch (Exception e) {
            fail(clusterId + " cannot be converted back to UUID.");
        }
    }

    /**
     * Checks the two iterables for equality by first converting both to a list.
     */
    public static <T> void checkEquals(Iterable<T> it1, Iterable<T> it2) {
        assertEquals(toList(it1), toList(it2));
    }

    public static <T> void checkEquals(Iterator<T> it1, Iterator<T> it2) {
        assertEquals(Utils.toList(it1), Utils.toList(it2));
    }

    public static <T> void checkEquals(Set<T> c1, Set<T> c2, String firstDesc, String secondDesc) {
        if (!c1.equals(c2)) {
            Set<T> missing1 = new HashSet<>(c2);
            missing1.removeAll(c1);
            Set<T> missing2 = new HashSet<>(c1);
            missing2.removeAll(c2);
            fail(String.format("Sets not equal, missing %s=%s, missing %s=%s", firstDesc, missing1, secondDesc, missing2));
        }
    }

    public static <T> List<T> toList(Iterable<? extends T> iterable) {
        List<T> list = new ArrayList<>();
        for (T item : iterable)
            list.add(item);
        return list;
    }

    public static ByteBuffer toBuffer(Struct struct) {
        ByteBuffer buffer = ByteBuffer.allocate(struct.sizeOf());
        struct.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    public static void assertFutureError(Future<?> future, Class<? extends Throwable> exceptionClass)
        throws InterruptedException {
        try {
            future.get();
            fail("Expected a " + exceptionClass.getSimpleName() + " exception, but got success.");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertEquals("Expected a " + exceptionClass.getSimpleName() + " exception, but got " +
                    cause.getClass().getSimpleName(),
                exceptionClass, cause.getClass());
        }
    }

    public static ApiKeys apiKeyFrom(NetworkReceive networkReceive) {
        return RequestHeader.parse(networkReceive.payload().duplicate()).apiKey();
    }
}
