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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.junit.Assert.assertTrue;


/**
 * Test the unclean shutdown behavior around state store cleanup.
 */
@RunWith(Parameterized.class)
@Category(IntegrationTest.class)
public class EOSUncleanShutdownIntegrationTest {

    @SuppressWarnings("deprecation")
    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.EXACTLY_ONCE_V2}
        });
    }

    @Parameterized.Parameter
    public String eosConfig;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
        STREAMS_CONFIG.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        STREAMS_CONFIG.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        STREAMS_CONFIG.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);
        STREAMS_CONFIG.put(StreamsConfig.STATE_DIR_CONFIG, TEST_FOLDER.getRoot().getPath());
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @ClassRule
    public static final TemporaryFolder TEST_FOLDER = new TemporaryFolder(TestUtils.tempDirectory());

    private static final Properties STREAMS_CONFIG = new Properties();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Long COMMIT_INTERVAL = 100L;

    private static final int RECORD_TOTAL = 3;

    @Test
    public void shouldWorkWithUncleanShutdownWipeOutStateStore() throws InterruptedException {
        final String appId = "shouldWorkWithUncleanShutdownWipeOutStateStore";
        STREAMS_CONFIG.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        STREAMS_CONFIG.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);

        final String input = "input-topic";
        cleanStateBeforeTest(CLUSTER, input);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> inputStream = builder.stream(input);

        final AtomicInteger recordCount = new AtomicInteger(0);

        final KTable<String, String> valueCounts = inputStream
            .groupByKey()
            .aggregate(
                () -> "()",
                (key, value, aggregate) -> aggregate + ",(" + key + ": " + value + ")",
                Materialized.as("aggregated_value"));

        valueCounts.toStream().peek((key, value) -> {
            if (recordCount.incrementAndGet() >= RECORD_TOTAL) {
                throw new IllegalStateException("Crash on the " + RECORD_TOTAL + " record");
            }
        });

        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        final KafkaStreams driver =  new KafkaStreams(builder.build(), STREAMS_CONFIG);
        driver.cleanUp();
        driver.start();

        // Task's StateDir
        final File taskStateDir = new File(String.join("/", TEST_FOLDER.getRoot().getPath(), appId, "0_0"));
        final File taskCheckpointFile = new File(taskStateDir, ".checkpoint");

        try {
            IntegrationTestUtils.produceSynchronously(producerConfig, false, input, Optional.empty(),
                singletonList(new KeyValueTimestamp<>("k1", "v1", 0L)));

            // wait until the first request is processed and some files are created in it
            TestUtils.waitForCondition(() -> taskStateDir.exists() && taskStateDir.isDirectory() && taskStateDir.list().length > 0,
                "Failed awaiting CreateTopics first request failure");
            IntegrationTestUtils.produceSynchronously(producerConfig, false, input, Optional.empty(),
                asList(new KeyValueTimestamp<>("k2", "v2", 1L),
                    new KeyValueTimestamp<>("k3", "v3", 2L)));

            TestUtils.waitForCondition(() -> recordCount.get() == RECORD_TOTAL,
                "Expected " + RECORD_TOTAL + " records processed but only got " + recordCount.get());
        } finally {
            TestUtils.waitForCondition(() -> driver.state().equals(State.ERROR),
                "Expected ERROR state but driver is on " + driver.state());

            driver.close();

            // Although there is an uncaught exception,
            // case 1: the state directory is cleaned up without any problems.
            // case 2: The state directory is not cleaned up, for it does not include any checkpoint file.
            // case 3: The state directory is not cleaned up, for it includes a checkpoint file but it is empty.
            assertTrue(!taskStateDir.exists()
                || (taskStateDir.exists() && taskStateDir.list().length > 0 && !taskCheckpointFile.exists())
                || (taskCheckpointFile.exists() && taskCheckpointFile.length() == 0L));

            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }
}
