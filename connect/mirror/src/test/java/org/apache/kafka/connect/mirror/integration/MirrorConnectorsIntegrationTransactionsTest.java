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

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.BeforeEach;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Integration test for MirrorMaker2 in which source records are emitted with a transactional producer,
 * which interleaves transaction commit messages into the source topic which are not propagated downstream.
 */
public class MirrorConnectorsIntegrationTransactionsTest extends MirrorConnectorsIntegrationBaseTest {

    @BeforeEach
    @Override
    public void startClusters() throws Exception {
        primaryBrokerProps.put("transaction.state.log.replication.factor", "1");
        backupBrokerProps.put("transaction.state.log.replication.factor", "1");
        primaryBrokerProps.put("transaction.state.log.min.isr", "1");
        backupBrokerProps.put("transaction.state.log.min.isr", "1");
        super.startClusters();
    }

    @Override
    protected Producer<byte[], byte[]> initializeProducer(EmbeddedConnectCluster cluster) {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "embedded-kafka-0");
        KafkaProducer<byte[], byte[]> producer = cluster.kafka().createProducer(producerProps);
        producer.initTransactions();
        return producer;
    }


    @Override
    protected void produceMessages(Producer<byte[], byte[]> producer, List<ProducerRecord<byte[], byte[]>> records) {
        try {
            producer.beginTransaction();
            super.produceMessages(producer, records);
            producer.commitTransaction();
        } catch (RuntimeException e) {
            producer.abortTransaction();
            throw e;
        }
    }
}
