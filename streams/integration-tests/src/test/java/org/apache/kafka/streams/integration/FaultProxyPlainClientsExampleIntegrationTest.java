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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.faultproxy.FaultRule;
import org.apache.kafka.test.faultproxy.KafkaProtocolFaultProxy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(120)
public class FaultProxyPlainClientsExampleIntegrationTest {

    @Test
    public void producerRetriesAroundInjectedProduceErrorAndConsumerReadsAll() throws Exception {
        final int numRecords = 50;
        final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        try {
            final String topic = "plain-example";
            cluster.createTopic(topic, 1, 1);

            try (final KafkaProtocolFaultProxy proxy =
                     KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers())) {

                // Fault the FIRST produce response with a retriable error; the producer recovers on retry.
                final FaultRule produceError =
                    proxy.injectError(ApiKeys.PRODUCE, Errors.NOT_ENOUGH_REPLICAS).once();

                // --- Producer: point bootstrap.servers at the PROXY ---
                final Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
                // retries + delivery.timeout.ms default high enough to absorb the one injected error.
                try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                    for (int i = 0; i < numRecords; i++) {
                        // .get() surfaces the send result; the retriable error is retried internally,
                        // so this still returns success once the (single) injected fault has passed.
                        producer.send(new ProducerRecord<>(topic, "k" + (i % 3), "v" + i)).get();
                    }
                }
                assertTrue(produceError.timesTriggered() >= 1, "PRODUCE error never fired");

                // --- Consumer: also point bootstrap.servers at the PROXY ---
                final Properties consumerProps = new Properties();
                consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
                consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "plain-example-group");
                consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
                    consumer.subscribe(singletonList(topic));
                    int consumed = 0;
                    final long deadline = System.currentTimeMillis() + 60_000L;
                    while (consumed < numRecords && System.currentTimeMillis() < deadline) {
                        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                        consumed += records.count();
                    }
                    assertEquals(numRecords, consumed, "consumer did not read every record back");
                }
            }
        } finally {
            cluster.stop();
        }
    }
}