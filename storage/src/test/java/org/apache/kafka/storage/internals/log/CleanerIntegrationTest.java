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
package org.apache.kafka.storage.internals.log;

import com.yammer.metrics.core.Gauge;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
public class CleanerIntegrationTest {

    @Timeout(300)
    @ClusterTest(types = Type.CO_KRAFT)
    public void testCleanerSegmentCompactionOverflow(ClusterInstance cluster) throws Exception {
        String topic = "compaction-overflow-test";
        try (Admin admin = cluster.admin()) {
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            newTopic.configs(Map.of(
                    "cleanup.policy", "compact",
                    "compression.type", "lz4",
                    "segment.bytes", String.valueOf(Integer.MAX_VALUE - 1),
                    "min.cleanable.dirty.ratio", "0.01"
            ));
            admin.createTopics(List.of(newTopic)).all().get();
            cluster.waitTopicCreation(topic, 1);
        }
        var valueBytes = new byte[10240];
        var random = new Random();
        random.nextBytes(valueBytes);
        var producers = IntStream.range(0, 5).mapToObj(__ -> CompletableFuture.runAsync(() -> {
            try (var producer = cluster.producer(Map.of(
                    ProducerConfig.COMPRESSION_LZ4_LEVEL_CONFIG, "17",
                    ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"))) {
                for (int i = 0; i < 60_000; i++) {
                    byte[] data = Uuid.randomUuid().toString().getBytes();
                    if (Math.random() < 0.1)
                        producer.send(new ProducerRecord<>(topic, data, null));
                    else
                        producer.send(new ProducerRecord<>(topic, data, valueBytes));
                }
            }
        })).toList();
        producers.forEach(CompletableFuture::join);
        var metrics = KafkaYammerMetrics.defaultRegistry().allMetrics();
        metrics.forEach((name, metric) -> {
            if (name.getName().contains("uncleanable-partitions-count")) {
                Gauge<Integer> value = (Gauge<Integer>) metric;
                assertEquals(1, value.value());
            }
        });
    }
}
