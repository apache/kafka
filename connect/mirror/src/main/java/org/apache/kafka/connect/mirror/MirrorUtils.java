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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;

/** Internal utility methods. */
public final class MirrorUtils {

    public static final String SOURCE_CLUSTER_KEY = "cluster";
    public static final String TOPIC_KEY = "topic";
    public static final String PARTITION_KEY = "partition";
    public static final String OFFSET_KEY = "offset";
    private static final Logger log = LoggerFactory.getLogger(MirrorUtils.class);

    // utility class
    private MirrorUtils() {}

    static KafkaProducer<byte[], byte[]> newProducer(Map<String, Object> props) {
        return new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer());
    }

    static KafkaConsumer<byte[], byte[]> newConsumer(Map<String, Object> props) {
        return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    static String encodeTopicPartition(TopicPartition topicPartition) {
        return topicPartition.toString();
    }

    static Map<String, Object> wrapPartition(TopicPartition topicPartition, String sourceClusterAlias) {
        Map<String, Object> wrapped = new HashMap<>();
        wrapped.put(TOPIC_KEY, topicPartition.topic());
        wrapped.put(PARTITION_KEY, topicPartition.partition());
        wrapped.put(SOURCE_CLUSTER_KEY, sourceClusterAlias);
        return wrapped;
    }

    public static Map<String, Object> wrapOffset(long offset) {
        return Collections.singletonMap(OFFSET_KEY, offset);
    }

    public static TopicPartition unwrapPartition(Map<String, ?> wrapped) {
        String topic = (String) wrapped.get(TOPIC_KEY);
        int partition = (Integer) wrapped.get(PARTITION_KEY);
        return new TopicPartition(topic, partition);
    }

    static Long unwrapOffset(Map<String, ?> wrapped) {
        if (wrapped == null || wrapped.get(OFFSET_KEY) == null) {
            return -1L;
        }
        return (Long) wrapped.get(OFFSET_KEY);
    }


    /**
     * Validate a specific key in a source partition that may be written to the offsets topic for one of the MM2 connectors.
     * This method ensures that the key is present in the source partition map and that its value is a string.
     *
     * @see org.apache.kafka.connect.source.SourceConnector#alterOffsets(Map, Map)
     * @see SourceRecord#sourcePartition()
     *
     * @param sourcePartition the to-be-validated source partition; may not be null
     * @param key the key to check for in the source partition; may not be null
     *
     * @throws ConnectException if the offset is invalid
     */
    static void validateSourcePartitionString(Map<String, ?> sourcePartition, String key) {
        Objects.requireNonNull(sourcePartition, "Source partition may not be null");
        Objects.requireNonNull(key, "Key may not be null");

        if (!sourcePartition.containsKey(key))
            throw new ConnectException(String.format(
                    "Source partition %s is missing the '%s' key, which is required",
                    sourcePartition,
                    key
            ));

        Object value = sourcePartition.get(key);
        if (!(value instanceof String)) {
            throw new ConnectException(String.format(
                    "Source partition %s has an invalid value %s for the '%s' key, which must be a string",
                    sourcePartition,
                    value,
                    key
            ));
        }
    }

    /**
     * Validate the {@link #PARTITION_KEY partition key} in a source partition that may be written to the offsets topic
     * for one of the MM2 connectors.
     * This method ensures that the key is present in the source partition map and that its value is a non-negative integer.
     * <p/>
     * Note that the partition key most likely refers to a partition in a Kafka topic, whereas the term "source partition" refers
     * to a {@link SourceRecord#sourcePartition() source partition} that is stored in a Kafka Connect worker's internal offsets
     * topic (or, if running in standalone mode, offsets file).
     *
     * @see org.apache.kafka.connect.source.SourceConnector#alterOffsets(Map, Map)
     * @see SourceRecord#sourcePartition()
     *
     * @param sourcePartition the to-be-validated source partition; may not be null
     *
     * @throws ConnectException if the partition is invalid
     */
    static void validateSourcePartitionPartition(Map<String, ?> sourcePartition) {
        Objects.requireNonNull(sourcePartition, "Source partition may not be null");

        if (!sourcePartition.containsKey(PARTITION_KEY))
            throw new ConnectException(String.format(
                    "Source partition %s is missing the '%s' key, which is required",
                    sourcePartition,
                    PARTITION_KEY
            ));

        Object value = sourcePartition.get(PARTITION_KEY);
        // The value may be encoded as a long but as long as it fits inside a 32-bit integer, that's fine
        if (!(value instanceof Integer || value instanceof Long) || ((Number) value).longValue() > Integer.MAX_VALUE) {
            throw new ConnectException(String.format(
                    "Source partition %s has an invalid value %s for the '%s' key, which must be an integer",
                    sourcePartition,
                    value,
                    PARTITION_KEY
            ));
        }

        if (((Number) value).intValue() < 0) {
            throw new ConnectException(String.format(
                    "Source partition %s has an invalid value %s for the '%s' key, which cannot be negative",
                    sourcePartition,
                    value,
                    PARTITION_KEY
            ));
        }
    }

    /**
     * Validate a source offset that may be written to the offsets topic for one of the MM2 connectors.
     *
     * @see org.apache.kafka.connect.source.SourceConnector#alterOffsets(Map, Map)
     * @see SourceRecord#sourceOffset()
     *
     * @param sourcePartition the corresponding {@link SourceRecord#sourcePartition() source partition} for the offset;
     *                        may not be null
     * @param sourceOffset the to-be-validated source offset; may be null (which is considered valid)
     * @param onlyOffsetZero whether the "offset" value in the source offset map must be zero;
     *                       if {@code true}, then only zero is permitted; if {@code false}, then any non-negative
     *                       value is permitted
     *
     * @throws ConnectException if the offset is invalid
     */
    static void validateSourceOffset(Map<String, ?> sourcePartition, Map<String, ?> sourceOffset, boolean onlyOffsetZero) {
        Objects.requireNonNull(sourcePartition, "Source partition may not be null");

        if (sourceOffset == null) {
            return;
        }

        if (!sourceOffset.containsKey(OFFSET_KEY)) {
            throw new ConnectException(String.format(
                    "Source offset %s for source partition %s is missing the '%s' key, which is required",
                    sourceOffset,
                    sourcePartition,
                    OFFSET_KEY
            ));
        }

        Object offset = sourceOffset.get(OFFSET_KEY);
        if (!(offset instanceof Integer || offset instanceof Long)) {
            throw new ConnectException(String.format(
                    "Source offset %s for source partition %s has an invalid value %s for the '%s' key, which must be an integer",
                    sourceOffset,
                    sourcePartition,
                    offset,
                    OFFSET_KEY
            ));
        }

        long offsetValue = ((Number) offset).longValue();
        if (onlyOffsetZero && offsetValue != 0) {
            throw new ConnectException(String.format(
                    "Source offset %s for source partition %s has an invalid value %s for the '%s' key; the only accepted value is 0",
                    sourceOffset,
                    sourcePartition,
                    offset,
                    OFFSET_KEY
            ));
        } else if (!onlyOffsetZero && offsetValue < 0) {
            throw new ConnectException(String.format(
                    "Source offset %s for source partition %s has an invalid value %s for the '%s' key, which cannot be negative",
                    sourceOffset,
                    sourcePartition,
                    offset,
                    OFFSET_KEY
            ));
        }
    }

    static TopicPartition decodeTopicPartition(String topicPartitionString) {
        int sep = topicPartitionString.lastIndexOf('-');
        String topic = topicPartitionString.substring(0, sep);
        String partitionString = topicPartitionString.substring(sep + 1);
        int partition = Integer.parseInt(partitionString);
        return new TopicPartition(topic, partition);
    }

    // returns null if given empty list
    static Pattern compilePatternList(List<String> fields) {
        if (fields.isEmpty()) {
            // The empty pattern matches _everything_, but a blank
            // config property should match _nothing_.
            return null;
        } else {
            String joined = String.join("|", fields);
            return Pattern.compile(joined);
        }
    }

    static Pattern compilePatternList(String fields) {
        return compilePatternList(Arrays.asList(fields.split("\\W*,\\W*")));
    }

    static void createCompactedTopic(String topicName, short partitions, short replicationFactor, Admin admin) {
        NewTopic topicDescription = TopicAdmin.defineTopic(topicName).
                compacted().
                partitions(partitions).
                replicationFactor(replicationFactor).
                build();

        CreateTopicsOptions args = new CreateTopicsOptions().validateOnly(false);
        try {
            admin.createTopics(singleton(topicDescription), args).values().get(topicName).get();
            log.info("Created topic '{}'", topicName);
        } catch (InterruptedException e) {
            Thread.interrupted();
            throw new ConnectException("Interrupted while attempting to create/find topic '" + topicName + "'", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicExistsException) {
                log.debug("Unable to create topic '{}' since it already exists.", topicName);
                return;
            }
            if (cause instanceof UnsupportedVersionException) {
                log.debug("Unable to create topic '{}' since the brokers do not support the CreateTopics API." +
                                " Falling back to assume topic exists or will be auto-created by the broker.",
                        topicName);
                return;
            }
            if (cause instanceof TopicAuthorizationException) {
                log.debug("Not authorized to create topic(s) '{}' upon the brokers." +
                                " Falling back to assume topic(s) exist or will be auto-created by the broker.",
                        topicName);
                return;
            }
            if (cause instanceof ClusterAuthorizationException) {
                log.debug("Not authorized to create topic '{}'." +
                                " Falling back to assume topic exists or will be auto-created by the broker.",
                        topicName);
                return;
            }
            if (cause instanceof InvalidConfigurationException) {
                throw new ConnectException("Unable to create topic '" + topicName + "': " + cause.getMessage(),
                        cause);
            }
            if (cause instanceof TimeoutException) {
                // Timed out waiting for the operation to complete
                throw new ConnectException("Timed out while checking for or creating topic '" + topicName + "'." +
                        " This could indicate a connectivity issue, unavailable topic partitions, or if" +
                        " this is your first use of the topic it may have taken too long to create.", cause);
            }
            throw new ConnectException("Error while attempting to create/find topic '" + topicName + "'", e);
        }

    }

    static void createSinglePartitionCompactedTopic(String topicName, short replicationFactor, Admin admin) {
        createCompactedTopic(topicName, (short) 1, replicationFactor, admin);
    }

    static <T> T adminCall(Callable<T> callable, Supplier<String> errMsg)
            throws ExecutionException, InterruptedException {
        try {
            return callable.call();
        } catch (ExecutionException | InterruptedException e) {
            Throwable cause = e.getCause();
            if (cause instanceof TopicAuthorizationException ||
                    cause instanceof ClusterAuthorizationException ||
                    cause instanceof GroupAuthorizationException) {
                log.error("{} occurred while trying to {}", cause.getClass().getSimpleName(), errMsg.get());
            }
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
