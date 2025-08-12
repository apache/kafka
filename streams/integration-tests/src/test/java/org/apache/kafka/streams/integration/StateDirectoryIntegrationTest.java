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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(600)
public class StateDirectoryIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Test
    public void testCleanUpStateDirIfEmpty(final TestInfo testInfo) throws InterruptedException {
        final String uniqueTestName = safeUniqueTestName(testInfo);

        // Create Topic
        final String input = uniqueTestName + "-input";
        CLUSTER.createTopic(input);

        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(ProducerConfig.ACKS_CONFIG, "all"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName())
        ));

        try (final KafkaProducer<String, String> producer =
                 new KafkaProducer<>(producerConfig, Serdes.String().serializer(), Serdes.String().serializer())) {
            // Create Test Records
            producer.send(new ProducerRecord<>(input, "a"));
            producer.send(new ProducerRecord<>(input, "b"));
            producer.send(new ProducerRecord<>(input, "c"));

            // Create Topology
            final String storeName = uniqueTestName + "-input-table";

            final StreamsBuilder builder = new StreamsBuilder();
            builder.table(
                input,
                Materialized
                    .<String, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            );
            final Topology topology = builder.build();

            // State Store Directory
            final String stateDir = TestUtils.tempDirectory(uniqueTestName).getPath();

            // Create KafkaStreams instance
            final String applicationId = uniqueTestName + "-app";
            final Properties streamsConfig = mkProperties(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, stateDir),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
            ));

            final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);

            // Create StateListener
            final CountDownLatch runningLatch = new CountDownLatch(1);
            final CountDownLatch notRunningLatch = new CountDownLatch(1);

            final KafkaStreams.StateListener stateListener = (newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    runningLatch.countDown();
                }
                if (newState == KafkaStreams.State.NOT_RUNNING) {
                    notRunningLatch.countDown();
                }
            };
            streams.setStateListener(stateListener);

            // Application state directory
            final File appDir = new File(stateDir, applicationId);

            // Validate application state directory is created.
            streams.start();
            try {
                runningLatch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                throw new RuntimeException("Streams didn't start in time.", e);
            }

            assertTrue((new File(stateDir)).exists());  // State directory exists
            assertTrue(appDir.exists());    // Application state directory Exists

            // Validate StateStore directory is deleted.
            streams.close();
            try {
                notRunningLatch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                throw new RuntimeException("Streams didn't cleaned up in time.", e);
            }

            streams.cleanUp();

            assertTrue((new File(stateDir)).exists());  // Root state store exists

            // case 1: the state directory is cleaned up without any problems.
            // case 2: The state directory is not cleaned up, for it does not include any checkpoint file.
            // case 3: The state directory is not cleaned up, for it includes a checkpoint file but it is empty.
            assertTrue(appDir.exists()
                || Arrays.stream(appDir.listFiles())
                    .filter(
                        (File f) -> f.isDirectory() && f.listFiles().length > 0 && !(new File(f, ".checkpoint")).exists()
                    ).findFirst().isPresent()
                || Arrays.stream(appDir.listFiles())
                    .filter(
                        (File f) -> f.isDirectory() && (new File(f, ".checkpoint")).length() == 0L
                    ).findFirst().isPresent()
            );
        } finally {
            CLUSTER.deleteAllTopics();
        }
    }

    @Test
    public void testNotCleanUpStateDirIfNotEmpty(final TestInfo testInfo) throws InterruptedException {
        final String uniqueTestName = safeUniqueTestName(testInfo);

        // Create Topic
        final String input = uniqueTestName + "-input";
        CLUSTER.createTopic(input);

        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(ProducerConfig.ACKS_CONFIG, "all"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName())
        ));

        try (final KafkaProducer<String, String> producer =
                 new KafkaProducer<>(producerConfig, Serdes.String().serializer(), Serdes.String().serializer())) {
            // Create Test Records
            producer.send(new ProducerRecord<>(input, "a"));
            producer.send(new ProducerRecord<>(input, "b"));
            producer.send(new ProducerRecord<>(input, "c"));

            // Create Topology
            final String storeName = uniqueTestName + "-input-table";

            final StreamsBuilder builder = new StreamsBuilder();
            builder.table(
                input,
                Materialized
                    .<String, String, KeyValueStore<Bytes, byte[]>>as(storeName)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            );
            final Topology topology = builder.build();

            // State Store Directory
            final String stateDir = TestUtils.tempDirectory(uniqueTestName).getPath();

            // Create KafkaStreams instance
            final String applicationId = uniqueTestName + "-app";
            final Properties streamsConfig = mkProperties(mkMap(
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, applicationId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, stateDir),
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
            ));

            final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);

            // Create StateListener
            final CountDownLatch runningLatch = new CountDownLatch(1);
            final CountDownLatch notRunningLatch = new CountDownLatch(1);

            final KafkaStreams.StateListener stateListener = (newState, oldState) -> {
                if (newState == KafkaStreams.State.RUNNING) {
                    runningLatch.countDown();
                }
                if (newState == KafkaStreams.State.NOT_RUNNING) {
                    notRunningLatch.countDown();
                }
            };
            streams.setStateListener(stateListener);

            // Application state directory
            final File appDir = new File(stateDir, applicationId);

            // Validate application state directory is created.
            streams.start();
            try {
                runningLatch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                throw new RuntimeException("Streams didn't start in time.", e);
            }

            assertTrue((new File(stateDir)).exists());  // State directory exists
            assertTrue(appDir.exists());    // Application state directory Exists

            try {
                final File dummyFile = new File(appDir, "dummy");
                Files.createFile(dummyFile.toPath());
            } catch (final IOException e) {
                throw new RuntimeException("Failed to create dummy file.", e);
            }

            // Validate StateStore directory is deleted.
            streams.close();
            try {
                notRunningLatch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                throw new RuntimeException("Streams didn't cleaned up in time.", e);
            }

            streams.cleanUp();

            assertTrue((new File(stateDir)).exists());  // Root state store exists
            assertTrue(appDir.exists());    // Application state store exists
        } finally {
            CLUSTER.deleteAllTopics();
        }
    }
}
