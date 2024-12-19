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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.PositionBound;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@Tag("integration")
@Timeout(600)
public class ConsistencyVectorIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static int port = 0;
    private static final String INPUT_TOPIC_NAME = "input-topic";
    private static final String TABLE_NAME = "source-table";
    private static final int KEY = 1;
    private static final int NUMBER_OF_MESSAGES = 100;

    public EmbeddedKafkaCluster cluster;

    private final List<KafkaStreams> streamsToCleanup = new ArrayList<>();
    private MockTime mockTime;

    @BeforeEach
    public void before() throws InterruptedException, IOException {
        final Properties props = new Properties();
        props.setProperty(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        props.setProperty(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true");
        cluster = new EmbeddedKafkaCluster(NUM_BROKERS, props);
        cluster.start();
        cluster.createTopic(INPUT_TOPIC_NAME, 1, 1);
        mockTime = cluster.time;
    }

    @AfterEach
    public void after() {
        for (final KafkaStreams kafkaStreams : streamsToCleanup) {
            kafkaStreams.close(Duration.ofSeconds(60));
        }
        cluster.stop();
    }

    @Test
    public void shouldHaveSamePositionBoundActiveAndStandBy(final TestInfo testInfo) throws Exception {
        final Semaphore semaphore = new Semaphore(0);

        final StreamsBuilder builder = new StreamsBuilder();

        builder.table(INPUT_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()),
                      Materialized.<Integer, Integer, KeyValueStore<Bytes, byte[]>>as(TABLE_NAME)
                                  .withCachingDisabled()
               )
               .toStream()
               .peek((k, v) -> semaphore.release());

        final String safeTestName = safeUniqueTestName(testInfo);
        final KafkaStreams kafkaStreams1 = createKafkaStreams(builder, streamsConfiguration(safeTestName));
        final KafkaStreams kafkaStreams2 = createKafkaStreams(builder, streamsConfiguration(safeTestName));
        final List<KafkaStreams> kafkaStreamsList = Arrays.asList(kafkaStreams1, kafkaStreams2);

        try {
            startApplicationAndWaitUntilRunning(kafkaStreamsList);

            produceValueRange();

            // Assert that all messages in the first batch were processed in a timely manner
            assertThat(
                "Did not process all message in time.",
                semaphore.tryAcquire(NUMBER_OF_MESSAGES, 120, TimeUnit.SECONDS), is(equalTo(true))
            );

            // Assert that both active and standby have the same position bound
            final StateQueryRequest<Integer> request =
                StateQueryRequest
                    .inStore(TABLE_NAME)
                    .withQuery(KeyQuery.<Integer, Integer>withKey(KEY))
                    .withPositionBound(PositionBound.unbounded());

            checkPosition(request, kafkaStreams1);
            checkPosition(request, kafkaStreams2);
        } finally {
            kafkaStreams1.close();
            kafkaStreams2.close();
        }
    }

    private void checkPosition(final StateQueryRequest<Integer> request,
                               final KafkaStreams kafkaStreams1) throws InterruptedException {
        final long maxWaitMs = TestUtils.DEFAULT_MAX_WAIT_MS;
        final long expectedEnd = System.currentTimeMillis() + maxWaitMs;

        while (true) {
            final StateQueryResult<Integer> stateQueryResult =
                IntegrationTestUtils.iqv2WaitForResult(
                    kafkaStreams1,
                    request
                );
            final QueryResult<Integer> queryResult =
                stateQueryResult.getPartitionResults().get(0);
            if (queryResult.isSuccess() && queryResult.getResult() != null) {
                // invariant: each value is also at the equivalent offset
                assertThat(
                    "Result:" + queryResult,
                    queryResult.getPosition(),
                    is(
                        Position.emptyPosition()
                                .withComponent(INPUT_TOPIC_NAME, 0, queryResult.getResult())
                    )
                );

                if (queryResult.getResult() == NUMBER_OF_MESSAGES - 1) {
                    // we're at the end of the input.
                    return;
                }
            } else {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new RuntimeException(
                        "Test timed out in " + maxWaitMs);
                }
            }

            // we're not done yet, so sleep a bit and test again.
            Thread.sleep(maxWaitMs / 10);
        }

    }

    private KafkaStreams createKafkaStreams(final StreamsBuilder builder, final Properties config) {
        final KafkaStreams streams = new KafkaStreams(builder.build(config), config);
        streamsToCleanup.add(streams);
        return streams;
    }

    private void produceValueRange() {
        final Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(
            INPUT_TOPIC_NAME,
            IntStream.range(0, NUMBER_OF_MESSAGES)
                     .mapToObj(i -> KeyValue.pair(KEY, i))
                     .collect(Collectors.toList()),
            producerProps,
            mockTime
        );
    }

    private Properties streamsConfiguration(final String safeTestName) {
        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        config.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:" + (++port));
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        config.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.IntegerSerde.class);
        config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 200);
        config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        config.put(InternalConfig.IQ_CONSISTENCY_OFFSET_VECTOR_ENABLED, true);
        return config;
    }
}
