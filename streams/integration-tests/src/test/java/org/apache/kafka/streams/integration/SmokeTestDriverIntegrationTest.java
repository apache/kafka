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
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.tests.SmokeTestClient;
import org.apache.kafka.streams.tests.SmokeTestDriver;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.tests.SmokeTestDriver.generate;
import static org.apache.kafka.streams.tests.SmokeTestDriver.verify;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(600)
@Tag("integration")
public class SmokeTestDriverIntegrationTest {
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
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
                result = verify(bootstrapServers, allData, maxRecordsPerKey);

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
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "true, true"})
    public void shouldWorkWithRebalance(final boolean stateUpdaterEnabled, final boolean processingThreadsEnabled) throws InterruptedException {
        Exit.setExitProcedure((statusCode, message) -> {
            throw new AssertionError("Test called exit(). code:" + statusCode + " message:" + message);
        });
        Exit.setHaltProcedure((statusCode, message) -> {
            throw new AssertionError("Test called halt(). code:" + statusCode + " message:" + message);
        });
        int numClientsCreated = 0;
        final ArrayList<SmokeTestClient> clients = new ArrayList<>();

        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, SmokeTestDriver.topics());

        final String bootstrapServers = CLUSTER.bootstrapServers();
        final Driver driver = new Driver(bootstrapServers, 10, 1000);
        driver.start();
        System.out.println("started driver");


        final Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(InternalConfig.STATE_UPDATER_ENABLED, stateUpdaterEnabled);
        props.put(InternalConfig.PROCESSING_THREADS_ENABLED, processingThreadsEnabled);
        // decrease the session timeout so that we can trigger the rebalance soon after old client left closed
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

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
                    Thread.sleep(100);
                }
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
                    Thread.sleep(100);
                }
            }
        }

        // check to make sure that it actually succeeded
        if (driver.exception() != null) {
            driver.exception().printStackTrace();
            throw new AssertionError(driver.exception());
        }
        assertTrue(driver.result().passed(), driver.result().result());
    }
}
