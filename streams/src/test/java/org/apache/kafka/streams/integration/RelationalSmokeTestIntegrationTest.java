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
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.tests.RelationalSmokeTest;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Category(IntegrationTest.class)
public class RelationalSmokeTestIntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalSmokeTestIntegrationTest.class);

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);


    private static final class Driver extends Thread {
        private final String bootstrapServers;
        private final int numArticles;
        private final int numComments;
        private final Duration produceDuration;
        private Exception exception = null;
        private boolean passed;

        private Driver(final String bootstrapServers,
                       final int numArticles,
                       final int numComments,
                       final Duration produceDuration) {
            this.bootstrapServers = bootstrapServers;
            this.numArticles = numArticles;
            this.numComments = numComments;
            this.produceDuration = produceDuration;
        }

        @Override
        public void run() {
            try {
                final RelationalSmokeTest.DataSet dataSet =
                    RelationalSmokeTest.DataSet.generate(numArticles, numComments);
                dataSet.produce(bootstrapServers, produceDuration);
                Thread.sleep(1000);
                passed = RelationalSmokeTest.App.verifySync(bootstrapServers,
                                                            Instant.now().plus(Duration.ofMinutes(10)));
            } catch (final InterruptedException e) {
                exception = e;
            }
        }

        public Exception exception() {
            return exception;
        }

        boolean passed() {
            return passed;
        }
    }

    @Before
    public void setUp() {
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, 2, RelationalSmokeTest.topics());
    }

    @Test
    public void shouldWorkOnSingleNodeWithoutBounce() throws InterruptedException {
        final String appId = "RelationalSmokeTestIntegration-" + UUID.randomUUID();

        final RelationalSmokeTest.DataSet dataSet = RelationalSmokeTest.DataSet.generate(2, 2);
        dataSet.produce(CLUSTER.bootstrapServers(), Duration.ZERO);

        // take a nap
        Thread.sleep(1000);

        final boolean passed;
        try (final KafkaStreams ignored = RelationalSmokeTest.App.startSync(CLUSTER.bootstrapServers(),
                                                                            appId,
                                                                            appId + "-0",
                                                                            TestUtils.tempDirectory().getAbsolutePath())) {
            passed = RelationalSmokeTest.App.verifySync(
                CLUSTER.bootstrapServers(),
                Instant.now().plus(Duration.ofMinutes(5))
            );
        }

        Assert.assertTrue(passed);
    }

    @Test
    public void shouldWorkWithRebalance() throws InterruptedException {
        int bounces = 0;
        int creates = 0;
        final Instant start = Instant.now();
        int numClientsCreated = 0;
        final List<KafkaStreams> clients = new ArrayList<>();

        final String appId = "RelationalSmokeTestIntegration-" + UUID.randomUUID();

        final Driver driver = new Driver(
            CLUSTER.bootstrapServers(),
            1000,
            10_000,
            Duration.ofSeconds(20)
        );
        driver.start();
        LOG.info("started driver");


        // cycle out Streams instances a few times
        while (driver.isAlive() && bounces < 3) {
            // take a nap
            Thread.sleep(1000);

            // add a new client
            final String id = appId + "-" + numClientsCreated++;
            clients.add(
                RelationalSmokeTest.App.startSync(
                    CLUSTER.bootstrapServers(),
                    appId,
                    id,
                    TestUtils.tempDirectory().getAbsolutePath()
                )
            );
            creates++;
            LOG.info("Create #{}", creates);

            // let the oldest client die of "natural causes"
            if (clients.size() >= 3) {
                clients.remove(0).close(Duration.ZERO);
                bounces++;
                LOG.info("Bounce #{}", bounces);
            }
        }
        try {
            // wait for verification to finish
            driver.join(IntegrationTestUtils.DEFAULT_TIMEOUT);
        } finally {
            // whether or not the assertions failed, tell all the streams instances to stop
            for (final KafkaStreams client : clients) {
                client.close(Duration.ZERO);
            }

            // then, wait for them to stop
            for (final KafkaStreams client : clients) {
                client.close();
            }
        }

        // check to make sure that it actually succeeded
        if (driver.exception() != null) {
            driver.exception().printStackTrace();
            throw new AssertionError(driver.exception());
        }
        Assert.assertTrue(driver.passed());

        LOG.info("completed with {} bounces in {}", bounces, Duration.between(start, Instant.now()));
    }

}
