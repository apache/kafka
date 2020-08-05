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

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.util.TopicAdmin;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.Collections;
import java.util.regex.Pattern;

/** Internal utility methods. */
final class MirrorUtils {

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
        wrapped.put("topic", topicPartition.topic());
        wrapped.put("partition", topicPartition.partition());
        wrapped.put("cluster", sourceClusterAlias);
        return wrapped;
    }

    static Map<String, Object> wrapOffset(long offset) {
        return Collections.singletonMap("offset", offset);
    }

    static TopicPartition unwrapPartition(Map<String, ?> wrapped) {
        String topic = (String) wrapped.get("topic");
        int partition = (Integer) wrapped.get("partition");
        return new TopicPartition(topic, partition);
    }

    static Long unwrapOffset(Map<String, ?> wrapped) {
        if (wrapped == null || wrapped.get("offset") == null) {
            return -1L;
        }
        return (Long) wrapped.get("offset");
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

    static void createCompactedTopic(String topicName, short partitions, short replicationFactor, Map<String, Object> adminProps) {
        NewTopic topicDescription = TopicAdmin.defineTopic(topicName).
                compacted().
                partitions(partitions).
                replicationFactor(replicationFactor).
                build();

        try (TopicAdmin admin = new TopicAdmin(adminProps)) {
            admin.createTopics(topicDescription);
        }
    }

    static void createSinglePartitionCompactedTopic(String topicName, short replicationFactor, Map<String, Object> adminProps) {
        createCompactedTopic(topicName, (short) 1, replicationFactor, adminProps);
    }
}
