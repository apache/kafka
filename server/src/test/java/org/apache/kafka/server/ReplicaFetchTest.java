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
package org.apache.kafka.server;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import java.util.List;
import java.util.Map;

@ClusterTestDefaults(brokers = 2, types = {Type.KRAFT})
public class ReplicaFetchTest {

    private final ClusterInstance cluster;

    ReplicaFetchTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @ClusterTest
    public void testReplicaFetcherThread() throws Exception {
        var topic1 = "foo";
        var topic2 = "bar";
        var partition = 0;
        var testMessageList1 = List.of("test1", "test2", "test3", "test4");
        var testMessageList2 = List.of("test5", "test6", "test7", "test8");

        // create topics with replication factor 2 and await leadership
        cluster.createTopic(topic1, 1, (short) 2);
        cluster.createTopic(topic2, 1, (short) 2);

        // send test messages to leader
        try (var producer = cluster.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            for (var m : testMessageList1) {
                producer.send(new ProducerRecord<>(topic1, m, m)).get();
            }
            for (var m : testMessageList2) {
                producer.send(new ProducerRecord<>(topic2, m, m)).get();
            }
        }

        TestUtils.waitForCondition(
            () -> {
                for (var topic : List.of(topic1, topic2)) {
                    var tp = new TopicPartition(topic, partition);
                    long expectedOffset = -1;
                    for (var broker : cluster.brokers().values()) {
                        var logEndOffset = broker.logManager().getLog(tp).map(log -> log.logEndOffset()).orElse(0L);
                        if (expectedOffset == -1) {
                            expectedOffset = logEndOffset;
                        }
                        if (expectedOffset <= 0 || logEndOffset != expectedOffset) {
                            return false;
                        }
                    }
                }
                return true;
            },
            "Broker logs should be identical"
        );
    }
}
