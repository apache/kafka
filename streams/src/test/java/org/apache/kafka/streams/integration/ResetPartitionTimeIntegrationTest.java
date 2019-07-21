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

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStartedStreams;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class ResetPartitionTimeIntegrationTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        3,
        mkProperties(mkMap()),
        0L
    );
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final int DEFAULT_TIMEOUT = 100;
    private final boolean eosEnabled;
    private static long lastRecordedTimestamp = -1L;

    @Parameters(name = "{index}: eosEnabled={0}")
    public static Collection<Object[]> parameters() {
        return asList(
            new Object[] {false},
            new Object[] {true}
        );
    }

    public ResetPartitionTimeIntegrationTest(final boolean eosEnabled) {
        this.eosEnabled = eosEnabled;
    }

    @Test
    public void testPartitionTimeAfterKStreamReset() throws InterruptedException {
        final String testId = "-shouldRecoverPartitionTimeAfterReset";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputRaw = "output-raw" + testId;

        // create multiple partitions as a trap, in case the buffer doesn't properly set the
        // partition on the records, but instead relies on the default key partitioner
        cleanStateBeforeTest(CLUSTER, 2, input, outputRaw);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> valueCounts = builder
            .stream(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE));

        valueCounts
            .to(outputRaw, Produced.with(STRING_SERDE, STRING_SERDE));

        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MaxTimestampExtractor.class);
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.POLL_MS_CONFIG, Integer.toString(DEFAULT_TIMEOUT));
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(DEFAULT_TIMEOUT));
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosEnabled ? EXACTLY_ONCE : AT_LEAST_ONCE);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        

        KafkaStreams driver = getStartedStreams(streamsConfig, builder, true);
        try {
            // start sending some records to have partition time committed 
            System.out.println("MY STANDARD OUT: Started sending some records");
            produceSynchronouslyToPartitionZero(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", 1000),
                    new KeyValueTimestamp<>("k2", "v2", 2000),
                    new KeyValueTimestamp<>("k3", "v3", 5000)
                )
            );
            System.out.println("MY STANDARD OUT: Is now about to verify output");
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", 1000),
                    new KeyValueTimestamp<>("k2", "v2", 2000),
                    new KeyValueTimestamp<>("k3", "v3", 5000)
                )
            );
            assertThat(lastRecordedTimestamp, is(5000L));
            lastRecordedTimestamp = -1L;
            Thread.sleep(1000); // wait for commit to finish

            // restart && reset the driver
            driver.close();
            assertThat(driver.state(), is(KafkaStreams.State.NOT_RUNNING));
            driver.cleanUp();
            driver = new KafkaStreams(builder.build(), streamsConfig);
            driver.start();

            // resend some records and retrieve the last committed timestamp
            produceSynchronouslyToPartitionZero(
                input,
                asList(
                    new KeyValueTimestamp<>("k4", "v4", 4998),
                    new KeyValueTimestamp<>("k5", "v5", 4999)
                )
            );
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("k4", "v4", 4998),
                    new KeyValueTimestamp<>("k5", "v5", 4999)
                )
            );
            // verify that the lastRecordedTimestamp is 5000
            assertThat(lastRecordedTimestamp, is(5000L));
        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    public static final class MaxTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long maxTimestamp) {
            System.out.println("MY STANDARD OUT: Record came from partition: " + (new TopicPartition(record.topic(), record.partition()).toString()));
            if (record.timestamp() > maxTimestamp) {
                // we do so because maxTimestamp will by RecordQueue logic turn into this record's timestamp
                lastRecordedTimestamp = record.timestamp();
            } else {
                lastRecordedTimestamp = maxTimestamp;
            }
            System.out.println("MY STANDARD OUT: TimestampExtractor: partition time being returned: " + record.timestamp() + " and lastRecordedTimestamp: " + lastRecordedTimestamp + " for partition " + 
                (new TopicPartition(record.topic(), record.partition()).toString()));
            return record.timestamp();
        }
    }

    private void verifyOutput(final String topic, final List<KeyValueTimestamp<String, String>> keyValueTimestamps) {
        final Properties properties = mkProperties(
            mkMap(
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName()),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName())
            )
        );
        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);
    }

    private static void produceSynchronouslyToPartitionZero(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.of(0), toProduce);
    }
}
