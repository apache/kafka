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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.test.faultproxy.FaultRule;
import org.apache.kafka.test.faultproxy.KafkaProtocolFaultProxy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(120)
public class FaultProxyDelayIntegrationTest {

    @Test
    public void shouldDelayASingleProduceResponse() throws Exception {
        final long delayMs = 2000L;
        final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        try {
            final String topic = "delay-example";
            cluster.createTopic(topic, 1, 1);

            try (final KafkaProtocolFaultProxy proxy =
                     KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers())) {

                // Deterministically delay only the FIRST produce response.
                final FaultRule delay =
                    proxy.delayOn(ApiKeys.PRODUCE, Duration.ofMillis(delayMs)).once();

                final Properties producerProps = new Properties();
                producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
                producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                producerProps.put(ProducerConfig.ACKS_CONFIG, "all");

                try (final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {
                    // The first send blocks on the (delayed) produce response; time it. Metadata/connection
                    // setup only adds time, so the elapsed lower bound is safe against flakiness.
                    final long start = System.nanoTime();
                    producer.send(new ProducerRecord<>(topic, "k", "v0")).get();
                    final long firstMs = Duration.ofNanos(System.nanoTime() - start).toMillis();

                    // A comfortable lower bound below the injected delay (Thread.sleep guarantees >= delayMs).
                    assertTrue(firstMs >= delayMs - 250,
                        "first produce should have been delayed ~" + delayMs + "ms but took " + firstMs + "ms");

                    // A subsequent send is not delayed (once()): it should be far faster than the delay.
                    final long start2 = System.nanoTime();
                    producer.send(new ProducerRecord<>(topic, "k", "v1")).get();
                    final long secondMs = Duration.ofNanos(System.nanoTime() - start2).toMillis();
                    assertTrue(secondMs < delayMs,
                        "second produce should not be delayed but took " + secondMs + "ms");
                }

                assertEquals(1, delay.timesTriggered(), "delay should have fired exactly once");
            }
        } finally {
            cluster.stop();
        }
    }
}
