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

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.tests.SmokeTestClient;
import org.apache.kafka.streams.tests.SmokeTestDriver;
import org.apache.kafka.test.IntegrationTest;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.tests.SmokeTestDriver.generate;
import static org.apache.kafka.streams.tests.SmokeTestDriver.verify;

@Category(IntegrationTest.class)
public class SmokeTestDriverIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);


    private static class Driver extends Thread {
        private String bootstrapServers;
        private int numKeys;
        private int maxRecordsPerKey;
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

    @Test
    public void shouldWorkWithRebalance() throws InterruptedException {
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
        Assert.assertTrue(driver.result().result(), driver.result().passed());
    }
}
