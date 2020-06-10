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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.WorkerHandle;
import org.apache.kafka.test.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;

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

    @Test
    public void testFailToStartWhenInternalTopicsAreNotCompacted() throws InterruptedException {
        // Change the broker default cleanup policy to something Connect doesn't like
        brokerProps.put("log." + TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        // Start out using the improperly configured topics
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "bad-config");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "bad-offset");
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "bad-status");
        workerProps.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProps.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        int numWorkers = 0;
        int numBrokers = 1;
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the brokers but not Connect
        log.info("Starting {} Kafka brokers, but no Connect workers yet", numBrokers);
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Broker did not start in time.");
        log.info("Completed startup of {} Kafka broker. Expected Connect worker to fail", numBrokers);

        // Create the good topics
        connect.kafka().createTopic("good-config", 1, 1, compactCleanupPolicy());
        connect.kafka().createTopic("good-offset", 1, 1, compactCleanupPolicy());
        connect.kafka().createTopic("good-status", 1, 1, compactCleanupPolicy());

        // Create the poorly-configured topics
        connect.kafka().createTopic("bad-config", 1, 1, deleteCleanupPolicy());
        connect.kafka().createTopic("bad-offset", 1, 1, compactAndDeleteCleanupPolicy());
        connect.kafka().createTopic("bad-status", 1, 1, noTopicSettings());

        // Check the topics
        log.info("Verifying the internal topics for Connect were manually created");
        connect.assertions().assertTopicsExist("good-config", "good-offset", "good-status", "bad-config", "bad-offset", "bad-status");

        // Try to start one worker, with three bad topics
        WorkerHandle worker = connect.addWorker(); // should have failed to start before returning
        assertFalse(worker.isRunning());
        assertFalse(connect.allWorkersRunning());
        assertFalse(connect.anyWorkersRunning());
        connect.removeWorker(worker);

        // We rely upon the fact that we can change the worker properties before the workers are started
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "good-config");

        // Try to start one worker, with two bad topics remaining
        worker = connect.addWorker(); // should have failed to start before returning
        assertFalse(worker.isRunning());
        assertFalse(connect.allWorkersRunning());
        assertFalse(connect.anyWorkersRunning());
        connect.removeWorker(worker);

        // We rely upon the fact that we can change the worker properties before the workers are started
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "good-offset");

        // Try to start one worker, with one bad topic remaining
        worker = connect.addWorker(); // should have failed to start before returning
        assertFalse(worker.isRunning());
        assertFalse(connect.allWorkersRunning());
        assertFalse(connect.anyWorkersRunning());
        connect.removeWorker(worker);
        // We rely upon the fact that we can change the worker properties before the workers are started
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "good-status");

        // Try to start one worker, now using all good internal topics
        connect.addWorker();
        connect.assertions().assertAtLeastNumWorkersAreUp(1, "Worker did not start in time.");
    }

    @Test
    public void testStartWhenInternalTopicsCreatedManuallyWithCompactForBrokersDefaultCleanupPolicy() throws InterruptedException {
        // Change the broker default cleanup policy to compact, which is good for Connect
        brokerProps.put("log." + TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        // Start out using the properly configured topics
        workerProps.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "config-topic");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "offset-topic");
        workerProps.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        workerProps.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProps.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        workerProps.put(DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        int numWorkers = 0;
        int numBrokers = 1;
        connect = new EmbeddedConnectCluster.Builder().name("connect-cluster-1")
                                                      .workerProps(workerProps)
                                                      .numWorkers(numWorkers)
                                                      .numBrokers(numBrokers)
                                                      .brokerProps(brokerProps)
                                                      .build();

        // Start the brokers but not Connect
        log.info("Starting {} Kafka brokers, but no Connect workers yet", numBrokers);
        connect.start();
        connect.assertions().assertExactlyNumBrokersAreUp(numBrokers, "Broker did not start in time.");
        log.info("Completed startup of {} Kafka broker. Expected Connect worker to fail", numBrokers);

        // Create the valid internal topics w/o topic settings, so these will use the broker's
        // broker's log.cleanup.policy=compact (set above)
        connect.kafka().createTopic("config-topic", 1, 1, noTopicSettings());
        connect.kafka().createTopic("offset-topic", 1, 1, noTopicSettings());
        connect.kafka().createTopic("status-topic", 1, 1, noTopicSettings());

        // Check the topics
        log.info("Verifying the internal topics for Connect were manually created");
        connect.assertions().assertTopicsExist("config-topic", "offset-topic", "status-topic");

        // Try to start one worker using valid internal topics
        connect.addWorker();
        connect.assertions().assertAtLeastNumWorkersAreUp(1, "Worker did not start in time.");
    }

    protected Map<String, String> compactCleanupPolicy() {
        return Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    }

    protected Map<String, String> deleteCleanupPolicy() {
        return Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
    }

    protected Map<String, String> noTopicSettings() {
        return Collections.emptyMap();
    }

    protected Map<String, String> compactAndDeleteCleanupPolicy() {
        Map<String, String> config = new HashMap<>();
        config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE + "," + TopicConfig.CLEANUP_POLICY_COMPACT);
        return config;
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
