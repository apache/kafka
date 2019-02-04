package org.apache.kafka.streams.tests;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.streams.tests.SmokeTestDriver.generate;
import static org.apache.kafka.streams.tests.SmokeTestDriver.verify;

public class SmokeTestDriverTest {
    private static class Driver extends Thread {
        private String bootstrapServers;
        private int numKeys;
        private int maxRecordsPerKey;
        private Exception exception = null;
        private boolean success;

        private Driver(final String bootstrapServers, final int numKeys, final int maxRecordsPerKey) {
            this.bootstrapServers = bootstrapServers;
            this.numKeys = numKeys;
            this.maxRecordsPerKey = maxRecordsPerKey;
        }

        @Override
        public void run() {
            try {
                final Map<String, Set<Integer>> allData = generate(bootstrapServers, numKeys, maxRecordsPerKey, true);
                success = verify(bootstrapServers, allData, maxRecordsPerKey);

            } catch (final Exception ex) {
                this.exception = ex;
            }
        }

        public Exception exception() {
            return exception;
        }

        boolean success() {
            return success;
        }

    }

    @Test
    public void shouldWorkWithRebalance() throws InterruptedException, IOException {
        int numClientsCreated = 0;
        final ArrayList<SmokeTestClient> clients = new ArrayList<>();
        try (final EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(3)) {
            embeddedKafkaCluster.start();
            embeddedKafkaCluster.createTopics("data", "echo", "max", "min", "dif", "sum", "cnt", "avg", "wcnt", "tagg");

            final String bootstrapServers = embeddedKafkaCluster.bootstrapServers();
            final Driver driver = new Driver(bootstrapServers, 10, 1000);
            driver.start();
            System.out.println("started streams");


            final Properties props = new Properties();
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            // cycle out Streams instances as long as the test is running.
            while (driver.isAlive()) {
                // wait for the last added client to start
                if (!clients.isEmpty()) {
                    while (!clients.get(clients.size() - 1).started()) {
                        Thread.sleep(100);
                    }
                }

                // take a nap
                Thread.sleep(1000);

                // add a new client
                final SmokeTestClient smokeTestClient = new SmokeTestClient("streams" + numClientsCreated++);
                clients.add(smokeTestClient);
                smokeTestClient.start(props);

                // let the oldest client die of "natural causes"
                if (clients.size() >= 3) {
                    clients.remove(0).closeAsync();
                }
            }
            driver.join();
            Assert.assertNull(driver.exception());
            Assert.assertTrue(driver.success());
        } finally {
            for (final SmokeTestClient client : clients) {
                client.closeAsync();
            }
            for (final SmokeTestClient client : clients) {
                client.close();
            }
        }
    }

}
