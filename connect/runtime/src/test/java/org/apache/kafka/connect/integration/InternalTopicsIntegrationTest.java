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
package org.apache.kafka.connect.integration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for the creation of internal topics.
 */
@Category(IntegrationTest.class)
public class InternalTopicsIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(InternalTopicsIntegrationTest.class);

    private EmbeddedConnectCluster connect;
    Map<String, String> workerProps = new HashMap<>();
    Properties brokerProps = new Properties();

    @Before
    public void setup() {
        // setup Kafka broker properties
        brokerProps.put("auto.create.topics.enable", String.valueOf(false));
    }

    @After
    public void close() {
        // stop all Connect, Kafka and Zk threads.
        connect.stop();
    }

    @Test
    public void testCreateInternalTopicsWithDefaultSettings() throws InterruptedException {
        int numWorkers = 1;
        int numBrokers = 3;
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the Connect cluster
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Brokers did not start in time.");
        connect.assertions().assertExactlyNumWorkersAreUp(numWorkers, "Worker did not start in time.");
        log.info("Completed startup of {} Kafka brokers and {} Connect workers", numBrokers, numWorkers);

        // Check the topics
        log.info("Verifying the internal topics for Connect");
        connect.assertions().assertTopicsExist(configTopic(), offsetTopic(), statusTopic());
        assertInternalTopicSettings();

        // Remove the Connect worker
        log.info("Stopping the Connect worker");
        connect.removeWorker();

        // And restart
        log.info("Starting the Connect worker");
        connect.startConnect();

        // Check the topics
        log.info("Verifying the internal topics for Connect");
        connect.assertions().assertTopicsExist(configTopic(), offsetTopic(), statusTopic());
        assertInternalTopicSettings();

        connect.stop();
    }

    @Test
    public void testCreateInternalTopicsWithFewerReplicasThanBrokers() throws InterruptedException {
        workerProps.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "2");
        workerProps.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        int numWorkers = 1;
        int numBrokers = 2;
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the Connect cluster
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Broker did not start in time.");
        connect.assertions().assertAtLeastNumWorkersAreUp(numWorkers, "Worker did not start in time.");
        log.info("Completed startup of {} Kafka brokers and {} Connect workers", numBrokers, numWorkers);

        // Check the topics
        log.info("Verifying the internal topics for Connect");
        connect.assertions().assertTopicsExist(configTopic(), offsetTopic(), statusTopic());
        assertInternalTopicSettings();
    }

    @Test
    public void testFailToCreateInternalTopicsWithMoreReplicasThanBrokers() throws InterruptedException {
        workerProps.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "3");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "2");
        workerProps.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        int numWorkers = 1;
        int numBrokers = 1;
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the brokers and Connect, but Connect should fail to create config and offset topic
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Broker did not start in time.");
        log.info("Completed startup of {} Kafka broker. Expected Connect worker to fail", numBrokers);

        // Verify that the offset and config topic don't exist;
        // the status topic may have been created if timing was right but we don't care
        log.info("Verifying the internal topics for Connect");
        connect.assertions().assertTopicsDoNotExist(configTopic(), offsetTopic());
    }

    protected void assertInternalTopicSettings() throws InterruptedException {
        DistributedConfig config = new DistributedConfig(workerProps);
        connect.assertions().assertTopicSettings(
                configTopic(),
                config.getShort(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG),
                1,
                "Config topic does not have the expected settings"
        );
        connect.assertions().assertTopicSettings(
                statusTopic(),
                config.getShort(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG),
                config.getInt(DistributedConfig.STATUS_STORAGE_PARTITIONS_CONFIG),
                "Status topic does not have the expected settings"
        );
        connect.assertions().assertTopicSettings(
                offsetTopic(),
                config.getShort(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG),
                config.getInt(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG),
                "Offset topic does not have the expected settings"
        );
    }

    protected String configTopic() {
        return workerProps.get(DistributedConfig.CONFIG_TOPIC_CONFIG);
    }

    protected String offsetTopic() {
        return workerProps.get(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG);
    }

    protected String statusTopic() {
        return workerProps.get(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG);
    }
}
