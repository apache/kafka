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
package org.apache.kafka.tools.streams;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Future;

public final class StreamsGroupCommandTestUtils {

    private StreamsGroupCommandTestUtils() {
    }

    public static KafkaStreams getStartedStreams(final Properties streamsConfig, final StreamsBuilder builder, final boolean clean) {
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        if (clean) {
            streams.cleanUp();
        }
        streams.start();
        return streams;
    }

    public static void startApplicationAndWaitUntilRunning(final KafkaStreams streams) throws InterruptedException {
        streams.start();
        TestUtils.waitForCondition(
            () -> streams.state() == KafkaStreams.State.RUNNING,
            "Streams application did not reach the RUNNING state.");
    }

    public static void produce(final ClusterInstance cluster,
                               final String topic,
                               final Optional<Integer> partition,
                               final List<KeyValueTimestamp<String, String>> toProduce) {
        try (Producer<String, String> producer = cluster.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            List<Future<RecordMetadata>> futures = toProduce.stream()
                .map(record -> producer.send(new ProducerRecord<>(
                    topic, partition.orElse(null), record.timestamp(), record.key(), record.value())))
                .toList();
            producer.flush();
            futures.forEach(future -> Assertions.assertDoesNotThrow(() -> future.get()));
        }
    }
}
