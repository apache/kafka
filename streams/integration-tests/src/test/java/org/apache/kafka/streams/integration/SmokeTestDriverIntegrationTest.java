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
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.tests.SmokeTestClient;
import org.apache.kafka.streams.tests.SmokeTestDriver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.tests.SmokeTestDriver.generate;
import static org.apache.kafka.streams.tests.SmokeTestDriver.verify;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class SmokeTestDriverIntegrationTest {
    private static EmbeddedKafkaCluster cluster = null;
    public TestInfo testInfo;
    private ArrayList<SmokeTestClient> clients = new ArrayList<>();

    @BeforeAll
    public static void startCluster() throws IOException {
        cluster = new EmbeddedKafkaCluster(3);
        cluster.start();
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
        cluster = null;
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) {
        this.testInfo = testInfo;
    }

    @AfterEach
    public void shutDown(final TestInfo testInfo) {
        // Clean up clients in case the test failed or timed out
        for (final SmokeTestClient client : clients) {
            if (!client.closed() && !client.error()) {
                client.close();
            }
        }
    }

    private static class Driver extends Thread {
        private final String bootstrapServers;
        private final int numKeys;
        private final int maxRecordsPerKey;
        private Exception exception = null;
        private SmokeTestDriver.VerificationResult result;

        private Driver(final String bootstrapServers, final int numKeys, final int maxRecordsPerKey) {
            this.bootstrapServers = bootstrapServers;
            this.numKeys = numKeys;
            this.maxRecordsPerKey = maxRecordsPerKey;
        }

        @Override
        public void run() {
            try {
                final Map<String, Set<Integer>> allData =
                    generate(bootstrapServers, numKeys, maxRecordsPerKey, Duration.ofSeconds(20));
                result = verify(bootstrapServers, allData, maxRecordsPerKey, false);

            } catch (final Exception ex) {
                this.exception = ex;
            }
        }

        public Exception exception() {
            return exception;
        }

        SmokeTestDriver.VerificationResult result() {
            return result;
        }

    }

    // In this test, we try to keep creating new stream, and closing the old one, to maintain only 3 streams alive.
    // During the new stream added and old stream left, the stream process should still complete without issue.
    // We set 2 timeout condition to fail the test before passing the verification:
    // (1) 10 min timeout, (2) 30 tries of polling without getting any data
    // The processing thread variations where disabled since they triggered a race condition, see KAFKA-19696
    @ParameterizedTest
    @CsvSource({
        "false, true",
        "false, false"
    })
    public void shouldWorkWithRebalance(
        final boolean processingThreadsEnabled,
        final boolean streamsProtocolEnabled
    ) throws InterruptedException {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new AssertionError("Test called exit(). code:" + statusCode + " message:" + message);
        });
        Exit.setHaltProcedure((statusCode, message) -> {
            throw new AssertionError("Test called halt(). code:" + statusCode + " message:" + message);
        });
        int numClientsCreated = 0;
        int numDataRecordsProcessed = 0;
        final int numKeys = 10;
        final int maxRecordsPerKey = 1000;

        IntegrationTestUtils.cleanStateBeforeTest(cluster, SmokeTestDriver.topics());

        final String bootstrapServers = cluster.bootstrapServers();
        final Driver driver = new Driver(bootstrapServers, numKeys, maxRecordsPerKey);
        driver.start();
        System.out.println("started driver");


        final Properties props = new Properties();
        final String appId = safeUniqueTestName(testInfo);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(InternalConfig.PROCESSING_THREADS_ENABLED, processingThreadsEnabled);
        if (streamsProtocolEnabled) {
            props.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault()));
            // decrease the session timeout so that we can trigger the rebalance soon after old client left closed
            cluster.setGroupSessionTimeout(appId, 10000);
            cluster.setGroupHeartbeatTimeout(appId, 1000);
        } else {
            // decrease the session timeout so that we can trigger the rebalance soon after old client left closed
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
        }

        // cycle out Streams instances as long as the test is running.
        while (driver.isAlive()) {
            // take a nap
            Thread.sleep(1000);

            // add a new client
            final SmokeTestClient smokeTestClient = new SmokeTestClient("streams-" + numClientsCreated++);
            clients.add(smokeTestClient);
            smokeTestClient.start(props);

            // let the oldest client die of "natural causes"
            if (clients.size() >= 3) {
                final SmokeTestClient client = clients.remove(0);

                client.closeAsync();
                while (!client.closed()) {
                    assertFalse(client.error(), "The streams application seems to have crashed.");
                    Thread.sleep(100);
                }
                numDataRecordsProcessed += client.totalDataRecordsProcessed();
            }
        }

        try {
            // wait for verification to finish
            driver.join();
        } finally {
            // whether or not the assertions failed, tell all the streams instances to stop
            for (final SmokeTestClient client : clients) {
                client.closeAsync();
            }

            // then, wait for them to stop
            for (final SmokeTestClient client : clients) {
                while (!client.closed()) {
                    assertFalse(client.error(), "The streams application seems to have crashed.");
                    Thread.sleep(100);
                }
                numDataRecordsProcessed += client.totalDataRecordsProcessed();
            }
        }

        // check to make sure that it actually succeeded
        if (driver.exception() != null) {
            driver.exception().printStackTrace();
            throw new AssertionError(driver.exception());
        }
        assertTrue(driver.result().passed(), driver.result().result());

        // The one extra record is a record that the driver produces to flush suppress
        final int expectedRecords = numKeys * maxRecordsPerKey + 1;

        // We check that we did no have to reprocess any records, which would indicate a bug since everything
        // runs locally in this test.
        assertEquals(expectedRecords, numDataRecordsProcessed,
            String.format("It seems we had to reprocess records, expected %d records, processed %d records.",
                expectedRecords,
                numDataRecordsProcessed)
        );
    }
}
