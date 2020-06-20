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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.tests.StreamsUpgradeTest;
import org.apache.kafka.test.IntegrationTest;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

import static org.apache.kafka.test.TestUtils.retryOnExceptionWithTimeout;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Category(IntegrationTest.class)
public class StreamsUpgradeTestIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeClass
    public static void setup() {
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, 1, "data");
    }

    @Test
    public void testVersionProbingUpgrade() throws InterruptedException {
        final KafkaStreams kafkaStreams1 = StreamsUpgradeTest.buildStreams(mkProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
            )
        ));
        final KafkaStreams kafkaStreams2 = StreamsUpgradeTest.buildStreams(mkProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
            )
        ));
        final KafkaStreams kafkaStreams3 = StreamsUpgradeTest.buildStreams(mkProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
            )
        ));
        startSync(kafkaStreams1, kafkaStreams2, kafkaStreams3);

        // first roll
        kafkaStreams1.close();
        final AtomicInteger usedVersion4 = new AtomicInteger();
        final KafkaStreams kafkaStreams4 = buildFutureStreams(usedVersion4);
        startSync(kafkaStreams4);
        assertThat(usedVersion4.get(), is(LATEST_SUPPORTED_VERSION));

        // second roll
        kafkaStreams2.close();
        final AtomicInteger usedVersion5 = new AtomicInteger();
        final KafkaStreams kafkaStreams5 = buildFutureStreams(usedVersion5);
        startSync(kafkaStreams5);
        assertThat(usedVersion5.get(), is(LATEST_SUPPORTED_VERSION));

        // third roll, upgrade complete
        kafkaStreams3.close();
        final AtomicInteger usedVersion6 = new AtomicInteger();
        final KafkaStreams kafkaStreams6 = buildFutureStreams(usedVersion6);
        startSync(kafkaStreams6);
        retryOnExceptionWithTimeout(() -> assertThat(usedVersion6.get(), is(LATEST_SUPPORTED_VERSION + 1)));
        retryOnExceptionWithTimeout(() -> assertThat(usedVersion5.get(), is(LATEST_SUPPORTED_VERSION + 1)));
        retryOnExceptionWithTimeout(() -> assertThat(usedVersion4.get(), is(LATEST_SUPPORTED_VERSION + 1)));

        kafkaStreams4.close(Duration.ZERO);
        kafkaStreams5.close(Duration.ZERO);
        kafkaStreams6.close(Duration.ZERO);
        kafkaStreams4.close();
        kafkaStreams5.close();
        kafkaStreams6.close();
    }

    private static KafkaStreams buildFutureStreams(final AtomicInteger usedVersion4) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        properties.put("test.future.metadata", usedVersion4);
        return StreamsUpgradeTest.buildStreams(properties);
    }

    private static void startSync(final KafkaStreams... kafkaStreams) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(kafkaStreams.length);
        for (final KafkaStreams streams : kafkaStreams) {
            streams.setStateListener((newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });
        }
        for (final KafkaStreams streams : kafkaStreams) {
            streams.start();
        }
        latch.await();
    }
}
