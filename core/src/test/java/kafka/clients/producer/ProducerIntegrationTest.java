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
package kafka.clients.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.test.api.ClusterFeature;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.ClusterTests;
import org.apache.kafka.server.common.Feature;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

@ExtendWith(ClusterTestExtensions.class)
public class ProducerIntegrationTest {

    @ClusterTests({
        @ClusterTest(brokers = 3, features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 0)}),
        @ClusterTest(brokers = 3, features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 1)}),
        @ClusterTest(brokers = 3, features = {
            @ClusterFeature(feature = Feature.TRANSACTION_VERSION, version = 2)})
    })
    public void testTransactionWithoutSend(ClusterInstance cluster) {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "foobar");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "test");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        try (Producer<byte[], byte[]> producer = cluster.producer(properties)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.commitTransaction();
        }
    }
}
