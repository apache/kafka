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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Integration test for MirrorMaker2 in which source records are emitted with a transactional producer,
 * which interleaves transaction commit messages into the source topic which are not propagated downstream.
 */
public class MirrorConnectorsIntegrationTransactionsTest extends MirrorConnectorsIntegrationBaseTest {

    private Map<String, Object> producerProps = new HashMap<>();

    @BeforeEach
    @Override
    public void startClusters() throws Exception {
        primaryBrokerProps.put("transaction.state.log.replication.factor", "1");
        backupBrokerProps.put("transaction.state.log.replication.factor", "1");
        primaryBrokerProps.put("transaction.state.log.min.isr", "1");
        backupBrokerProps.put("transaction.state.log.min.isr", "1");
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "embedded-kafka-0");
        super.startClusters();
    }

    /**
     * Produce records with a short-lived transactional producer to interleave transaction markers in the topic.
     */
    @Override
    protected void produce(EmbeddedKafkaCluster cluster, String topic, Integer partition, String key, String value) {
        ProducerRecord<byte[], byte[]> msg = new ProducerRecord<>(topic, partition, key == null ? null : key.getBytes(), value == null ? null : value.getBytes());
        try (Producer<byte[], byte[]> producer = cluster.createProducer(producerProps)) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(msg).get(120000, TimeUnit.MILLISECONDS);
            producer.commitTransaction();
        } catch (Exception e) {
            throw new KafkaException("Could not produce message: " + msg, e);
        }
    }
}
