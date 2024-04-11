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

import org.apache.kafka.clients.MetadataSnapshot;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordVersion;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.ByteBufferChannel;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
    public static final long DEFAULT_POLL_INTERVAL_MS = 100;
    public static final long DEFAULT_MAX_WAIT_MS = 15000;

    public static Cluster singletonCluster() {
        return clusterWith(1);
    }

    public static Cluster singletonCluster(final String topic, final int partitions) {
        return clusterWith(1, topic, partitions);
    }

    public static Cluster clusterWith(int nodes) {
        return clusterWith(nodes, new HashMap<>());
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
        return new Cluster("kafka-cluster", asList(ns), parts, Collections.emptySet(), Collections.emptySet());
    }

    public static Cluster clusterWith(final int nodes, final String topic, final int partitions) {
        return clusterWith(nodes, Collections.singletonMap(topic, partitions));
    }

    /**
     * Test utility function to get MetadataSnapshot with configured nodes and partitions.
     * @param nodes number of nodes in the cluster
     * @param topicPartitionCounts map of topic -> # of partitions
     * @return a MetadataSnapshot with number of nodes, partitions as per the input.
     */

    public static MetadataSnapshot metadataSnapshotWith(final int nodes, final Map<String, Integer> topicPartitionCounts) {
        final Node[] ns = new Node[nodes];
        Map<Integer, Node> nodesById = new HashMap<>();
        for (int i = 0; i < nodes; i++) {
            ns[i] = new Node(i, "localhost", 1969);
            nodesById.put(ns[i].id(), ns[i]);
        }
        final List<PartitionMetadata> partsMetadatas = new ArrayList<>();
        for (final Map.Entry<String, Integer> topicPartition : topicPartitionCounts.entrySet()) {
            final String topic = topicPartition.getKey();
            final int partitions = topicPartition.getValue();
            for (int i = 0; i < partitions; i++) {
                TopicPartition tp = new TopicPartition(topic, partitions);
                Node node = ns[i % ns.length];
                partsMetadatas.add(new PartitionMetadata(Errors.NONE, tp, Optional.of(node.id()), Optional.empty(), null, null, null));
            }
        }
        return new MetadataSnapshot("kafka-cluster", nodesById, partsMetadatas, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), null, Collections.emptyMap());
    }

    /**
     * Test utility function to get MetadataSnapshot of cluster with configured, and 0 partitions.
     * @param nodes number of nodes in the cluster.
     * @return a MetadataSnapshot of cluster with number of nodes in the input.
     */
    public static MetadataSnapshot metadataSnapshotWith(int nodes) {
        return metadataSnapshotWith(nodes, new HashMap<>());
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
     * Create an empty file in the default temporary-file directory, using the given prefix and suffix
     * to generate its name.
     * @throws IOException
     */
    public static File tempFile(final String prefix, final String suffix) throws IOException {
        final File file = Files.createTempFile(prefix, suffix).toFile();
        file.deleteOnExit();
        return file;
    }

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        return tempFile("kafka", ".tmp");
    }

    /**
     * Create a file with the given contents in the default temporary-file directory,
     * using `kafka` as the prefix and `tmp` as the suffix to generate its name.
     */
    public static File tempFile(final String contents) throws IOException {
        final File file = tempFile();
        Files.write(file.toPath(), contents.getBytes(StandardCharsets.UTF_8));
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
     * Create a temporary directory under the given root directory.
     * The root directory is removed on JVM exit if it doesn't already exist
     * when this function is invoked.
     *
     * @param root path to create temporary directory under
     * @return the temporary directory created within {@code root}
     */
    public static File tempRelativeDir(String root) {
        File rootFile = new File(root);
        if (rootFile.mkdir()) {
            rootFile.deleteOnExit();
        }
        return tempDirectory(rootFile.toPath(), null);
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

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    public static Properties producerConfig(final String bootstrapServers,
                                            final Class<?> keySerializer,
                                            final Class<?> valueSerializer,
                                            final Properties additional) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.putAll(additional);
        return properties;
    }

    public static Properties producerConfig(final String bootstrapServers, final Class<?> keySerializer, final Class<?> valueSerializer) {
        return producerConfig(bootstrapServers, keySerializer, valueSerializer, new Properties());
    }

    public static Properties requiredConsumerConfig() {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return consumerConfig;
    }

    public static Properties consumerConfig(final String bootstrapServers,
                                            final String groupId,
                                            final Class<?> keyDeserializer,
                                            final Class<?> valueDeserializer,
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
                                            final Class<?> keyDeserializer,
                                            final Class<?> valueDeserializer) {
        return consumerConfig(bootstrapServers,
            groupId,
            keyDeserializer,
            valueDeserializer,
            new Properties());
    }

    /**
     * returns consumer config with random UUID for the Group ID
     */
    public static Properties consumerConfig(final String bootstrapServers, final Class<?> keyDeserializer, final Class<?> valueDeserializer) {
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
        waitForCondition(testCondition, maxWaitMs, DEFAULT_POLL_INTERVAL_MS, conditionDetailsSupplier);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} with a polling interval of {@code pollIntervalMs}
     * and throw assertion failure otherwise. This should be used instead of {@code Thread.sleep} whenever possible
     * as it allows a longer timeout to be used without unnecessarily increasing test time (as the condition is
     * checked frequently). The longer timeout is needed to avoid transient failures due to slow or overloaded
     * machines.
     */
    public static void waitForCondition(
        final TestCondition testCondition,
        final long maxWaitMs,
        final long pollIntervalMs,
        Supplier<String> conditionDetailsSupplier
    ) throws InterruptedException {
        retryOnExceptionWithTimeout(maxWaitMs, pollIntervalMs, () -> {
            String conditionDetailsSupplied = conditionDetailsSupplier != null ? conditionDetailsSupplier.get() : null;
            String conditionDetails = conditionDetailsSupplied != null ? conditionDetailsSupplied : "";
            assertTrue(testCondition.conditionMet(),
                "Condition not met within timeout " + maxWaitMs + ". " + conditionDetails);
        });
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs the total time in milliseconds to wait for {@code runnable} to complete successfully.
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final long timeoutMs,
                                                   final ValuelessCallable runnable) throws InterruptedException {
        retryOnExceptionWithTimeout(timeoutMs, DEFAULT_POLL_INTERVAL_MS, runnable);
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the default timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final ValuelessCallable runnable) throws InterruptedException {
        retryOnExceptionWithTimeout(DEFAULT_MAX_WAIT_MS, DEFAULT_POLL_INTERVAL_MS, runnable);
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param timeoutMs the total time in milliseconds to wait for {@code runnable} to complete successfully.
     * @param pollIntervalMs the interval in milliseconds to wait between invoking {@code runnable}.
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    public static void retryOnExceptionWithTimeout(final long timeoutMs,
                                                   final long pollIntervalMs,
                                                   final ValuelessCallable runnable) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + timeoutMs;

        while (true) {
            try {
                runnable.call();
                return;
            } catch (final NoRetryException e) {
                throw e;
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(String.format("Assertion failed with an exception after %s ms", timeoutMs), e);
                }
            }
            Thread.sleep(Math.min(pollIntervalMs, timeoutMs));
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

    public static <T> Set<T> toSet(Collection<T> collection) {
        return new HashSet<>(collection);
    }

    public static ByteBuffer toBuffer(Send send) {
        ByteBufferChannel channel = new ByteBufferChannel(send.size());
        try {
            assertEquals(send.size(), send.writeTo(channel));
            channel.close();
            return channel.buffer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static ByteBuffer toBuffer(UnalignedRecords records) {
        return toBuffer(records.toSend());
    }

    public static Set<TopicPartition> generateRandomTopicPartitions(int numTopic, int numPartitionPerTopic) {
        Set<TopicPartition> tps = new HashSet<>();
        for (int i = 0; i < numTopic; i++) {
            String topic = randomString(32);
            for (int j = 0; j < numPartitionPerTopic; j++) {
                tps.add(new TopicPartition(topic, j));
            }
        }
        return tps;
    }

    /**
     * Assert that a future raises an expected exception cause type. Return the exception cause
     * if the assertion succeeds; otherwise raise AssertionError.
     *
     * @param future The future to await
     * @param exceptionCauseClass Class of the expected exception cause
     * @param <T> Exception cause type parameter
     * @return The caught exception cause
     */
    public static <T extends Throwable> T assertFutureThrows(Future<?> future, Class<T> exceptionCauseClass) {
        ExecutionException exception = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(exceptionCauseClass, exception.getCause(),
            "Unexpected exception cause " + exception.getCause());
        return exceptionCauseClass.cast(exception.getCause());
    }

    public static <T extends Throwable> void assertFutureThrows(
        Future<?> future,
        Class<T> expectedCauseClassApiException,
        String expectedMessage
    ) {
        T receivedException = assertFutureThrows(future, expectedCauseClassApiException);
        assertEquals(expectedMessage, receivedException.getMessage());
    }

    public static void assertFutureError(Future<?> future, Class<? extends Throwable> exceptionClass)
        throws InterruptedException {
        try {
            future.get();
            fail("Expected a " + exceptionClass.getSimpleName() + " exception, but got success.");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertEquals(exceptionClass, cause.getClass(),
                "Expected a " + exceptionClass.getSimpleName() + " exception, but got " +
                    cause.getClass().getSimpleName());
        }
    }

    public static ApiKeys apiKeyFrom(NetworkReceive networkReceive) {
        return RequestHeader.parse(networkReceive.payload().duplicate()).apiKey();
    }

    public static <T> void assertOptional(Optional<T> optional, Consumer<T> assertion) {
        if (optional.isPresent()) {
            assertion.accept(optional.get());
        } else {
            fail("Missing value from Optional");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fieldValue(Object o, Class<?> clazz, String fieldName)  {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(o);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    /**
     * Returns true if both iterators have same elements in the same order.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param <T>       type of element in the iterators.
     */
    public static <T> boolean sameElementsWithOrder(Iterator<T> iterator1,
                                                    Iterator<T> iterator2) {
        while (iterator1.hasNext()) {
            if (!iterator2.hasNext()) {
                return false;
            }

            if (!Objects.equals(iterator1.next(), iterator2.next())) {
                return false;
            }
        }

        return !iterator2.hasNext();
    }

    /**
     * Returns true if both the iterators have same set of elements irrespective of order and duplicates.
     *
     * @param iterator1 first iterator.
     * @param iterator2 second iterator.
     * @param <T>       type of element in the iterators.
     */
    public static <T> boolean sameElementsWithoutOrder(Iterator<T> iterator1,
                                                       Iterator<T> iterator2) {
        // Check both the iterators have the same set of elements irrespective of order and duplicates.
        Set<T> allSegmentsSet = new HashSet<>();
        iterator1.forEachRemaining(allSegmentsSet::add);
        Set<T> expectedSegmentsSet = new HashSet<>();
        iterator2.forEachRemaining(expectedSegmentsSet::add);

        return allSegmentsSet.equals(expectedSegmentsSet);
    }

    public static ApiVersionsResponse defaultApiVersionsResponse(
            ApiMessageType.ListenerType listenerType
    ) {
        return defaultApiVersionsResponse(0, listenerType);
    }

    public static ApiVersionsResponse defaultApiVersionsResponse(
            int throttleTimeMs,
            ApiMessageType.ListenerType listenerType
    ) {
        return createApiVersionsResponse(
                throttleTimeMs,
                ApiVersionsResponse.filterApis(RecordVersion.current(), listenerType, true, true),
                Features.emptySupportedFeatures(),
                false
        );
    }

    public static ApiVersionsResponse defaultApiVersionsResponse(
            int throttleTimeMs,
            ApiMessageType.ListenerType listenerType,
            boolean enableUnstableLastVersion
    ) {
        return createApiVersionsResponse(
                throttleTimeMs,
                ApiVersionsResponse.filterApis(RecordVersion.current(), listenerType, enableUnstableLastVersion, true),
                Features.emptySupportedFeatures(),
                false
        );
    }

    public static ApiVersionsResponse createApiVersionsResponse(
            int throttleTimeMs,
            ApiVersionsResponseData.ApiVersionCollection apiVersions
    ) {
        return createApiVersionsResponse(throttleTimeMs, apiVersions, Features.emptySupportedFeatures(), false);
    }

    public static ApiVersionsResponse createApiVersionsResponse(
            int throttleTimeMs,
            ApiVersionsResponseData.ApiVersionCollection apiVersions,
            Features<SupportedVersionRange> latestSupportedFeatures,
            boolean zkMigrationEnabled
    ) {
        return ApiVersionsResponse.createApiVersionsResponse(
                throttleTimeMs,
                apiVersions,
                latestSupportedFeatures,
                Collections.emptyMap(),
                ApiVersionsResponse.UNKNOWN_FINALIZED_FEATURES_EPOCH,
                zkMigrationEnabled);
    }
}
