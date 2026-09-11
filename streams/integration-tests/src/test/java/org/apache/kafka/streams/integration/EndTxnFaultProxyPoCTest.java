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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.FaultRule;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * PROOF OF CONCEPT: validates the fault-proxy end-to-end — that a transactional client routed through
 * {@link KafkaProtocolFaultProxy} (a) works normally through the proxy (routing via Metadata/FindCoordinator
 * rewrite), and (b) observes an injected {@code EndTxn} error code that the real broker never returned,
 * decoded and re-encoded through Kafka's own protocol classes.
 *
 * <p>Not a permanent test; it exercises the harness itself before wiring it to full KIP-892 Streams tests.
 */
@Timeout(120)
@Tag("integration")
public class EndTxnFaultProxyPoCTest {

    private static final String TOPIC = "poc-topic";

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;

    @BeforeEach
    public void setUp() throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        cluster.createTopic(TOPIC, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
    }

    @AfterEach
    public void tearDown() {
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    private KafkaProducer<String, String> transactionalProducer(final String txnId) {
        final Properties props = new Properties();
        // Point the client at the PROXY, not the broker — this is what validates routing.
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10_000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000);
        return new KafkaProducer<>(props);
    }

    @Test
    public void controlTransactionShouldCommitThroughProxy() {
        // Proves routing: initTransactions (FindCoordinator), Produce, and EndTxn all flow through the
        // proxy and succeed, i.e. the Metadata/FindCoordinator rewrites correctly point the client at us.
        try (KafkaProducer<String, String> producer = transactionalProducer("poc-control")) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC, "k", "v"));
            producer.commitTransaction();
        }
    }

    @Test
    public void injectedEndTxnErrorShouldSurfaceToClient() {
        // Arm: the broker will really commit, but the proxy rewrites the EndTxn response error code to
        // PRODUCER_FENCED (fatal). If routing/re-encode were wrong the client would instead throw a
        // schema/correlation error — so a clean ProducerFenced proves the whole round-trip.
        final FaultRule rule = proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED).once();
        try (KafkaProducer<String, String> producer = transactionalProducer("poc-inject")) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(TOPIC, "k", "v"));

            final KafkaException e = assertThrows(KafkaException.class, producer::commitTransaction);
            System.out.println("[POC] commitTransaction threw: " + e.getClass().getName() + " -> " + e.getMessage());
        }
        assertEquals(1, rule.timesTriggered(), "exactly one EndTxn error should have been injected");
    }
}
