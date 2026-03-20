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
package org.apache.kafka.server;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.metadata.LeaderConstants;
import org.apache.kafka.metadata.MetadataCache;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Meter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.server.TestUtils.awaitLeaderChange;
import static org.apache.kafka.server.TestUtils.produceMessage;
import static org.apache.kafka.server.TestUtils.waitForPartitionMetadata;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
        brokers = 2,
        serverProperties = {
            // trim the retry count and backoff interval to reduce test execution time
            @ClusterConfigProperty(key = ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG, value = "10"),
            @ClusterConfigProperty(key = GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1")
        }
)
public class UncleanLeaderElectionTest {

    private static final Logger LOG = LoggerFactory.getLogger(UncleanLeaderElectionTest.class);

    private static final int BROKER_ID_0 = 0;
    private static final int BROKER_ID_1 = 1;
    private static final Random RANDOM = new Random();
    private static final String TOPIC = "topic" + RANDOM.nextLong();
    private static final int PARTITION_ID = 0;
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC, PARTITION_ID);

    private final ClusterInstance cluster;
    private Admin admin;

    public UncleanLeaderElectionTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() {
        admin = cluster.admin();
        // temporarily set ReplicationControlManager logger to a higher level so that tests run quietly
        Configurator.setLevel(ReplicationControlManager.class.getName(), Level.FATAL);
    }

    @AfterEach
    public void teardown() {
        Utils.closeQuietly(admin, "admin client");
        // restore log level
        Configurator.setLevel(ReplicationControlManager.class.getName(), Level.ERROR);
    }

    private void disableEligibleLeaderReplicas() throws Exception {
        admin.updateFeatures(
                    Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME, new FeatureUpdate((short) 0, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE))
        ).all().get();
    }

    @ClusterTest(
         serverProperties = {
             @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true")
         }
    )
    public void testUncleanLeaderElectionEnabledClassic() throws Exception {
        testUncleanLeaderElectionEnabled(GroupProtocol.CLASSIC);
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true")
        }
    )
    public void testUncleanLeaderElectionEnabledConsumer() throws Exception {
        testUncleanLeaderElectionEnabled(GroupProtocol.CONSUMER);
    }

    private void testUncleanLeaderElectionEnabled(GroupProtocol groupProtocol) throws Exception {
        disableEligibleLeaderReplicas();

        // create topic with 1 partition, 2 replicas, one on each broker
        NewTopic newTopic = new NewTopic(TOPIC, Map.of(PARTITION_ID, List.of(BROKER_ID_0, BROKER_ID_1)));
        admin.createTopics(List.of(newTopic)).all().get();

        verifyUncleanLeaderElectionEnabled(groupProtocol);
    }

    @ClusterTest
    public void testUncleanLeaderElectionDisabledClassic() throws Exception {
        testUncleanLeaderElectionDisabled(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testUncleanLeaderElectionDisabledConsumer() throws Exception {
        testUncleanLeaderElectionDisabled(GroupProtocol.CONSUMER);
    }

    private void testUncleanLeaderElectionDisabled(GroupProtocol groupProtocol) throws Exception {
        // unclean leader election is disabled by default
        disableEligibleLeaderReplicas();

        // create topic with 1 partition, 2 replicas, one on each broker
        NewTopic newTopic = new NewTopic(TOPIC, Map.of(PARTITION_ID, List.of(BROKER_ID_0, BROKER_ID_1)));
        admin.createTopics(List.of(newTopic)).all().get();

        verifyUncleanLeaderElectionDisabled(groupProtocol);
    }

    @ClusterTest
    public void testUncleanLeaderElectionEnabledByTopicOverrideClassic() throws Exception {
        testUncleanLeaderElectionEnabledByTopicOverride(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testUncleanLeaderElectionEnabledByTopicOverrideConsumer() throws Exception {
        testUncleanLeaderElectionEnabledByTopicOverride(GroupProtocol.CONSUMER);
    }

    private void testUncleanLeaderElectionEnabledByTopicOverride(GroupProtocol groupProtocol) throws Exception {
        disableEligibleLeaderReplicas();

        // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election enabled
        NewTopic newTopic = new NewTopic(TOPIC, Map.of(PARTITION_ID, List.of(BROKER_ID_0, BROKER_ID_1)))
                .configs(Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"));
        admin.createTopics(List.of(newTopic)).all().get();

        verifyUncleanLeaderElectionEnabled(groupProtocol);
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true")
        }
    )
    public void testUncleanLeaderElectionDisabledByTopicOverrideClassic() throws Exception {
        testUncleanLeaderElectionDisabledByTopicOverride(GroupProtocol.CLASSIC);
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, value = "true")
        }
    )
    public void testUncleanLeaderElectionDisabledByTopicOverrideConsumer() throws Exception {
        testUncleanLeaderElectionDisabledByTopicOverride(GroupProtocol.CONSUMER);
    }

    private void testUncleanLeaderElectionDisabledByTopicOverride(GroupProtocol groupProtocol) throws Exception {
        disableEligibleLeaderReplicas();

        // create topic with 1 partition, 2 replicas, one on each broker, and unclean leader election disabled
        NewTopic newTopic = new NewTopic(TOPIC, Map.of(PARTITION_ID, List.of(BROKER_ID_0, BROKER_ID_1)))
                .configs(Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false"));
        admin.createTopics(List.of(newTopic)).all().get();

        verifyUncleanLeaderElectionDisabled(groupProtocol);
    }

    @ClusterTest
    public void testUncleanLeaderElectionInvalidTopicOverride() throws Exception {
        disableEligibleLeaderReplicas();

        // create topic with an invalid value for unclean leader election
        NewTopic newTopic = new NewTopic(TOPIC, Map.of(PARTITION_ID, List.of(BROKER_ID_0, BROKER_ID_1)))
                .configs(Map.of(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "invalid"));
        Throwable e = assertThrows(ExecutionException.class, () -> admin.createTopics(List.of(newTopic)).all().get());
        assertInstanceOf(InvalidConfigurationException.class, e.getCause());
    }

    private void verifyUncleanLeaderElectionEnabled(GroupProtocol groupProtocol) throws Exception {
        // wait until leader is elected
        int leaderId = awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.empty());
        LOG.debug("Leader for {} is elected to be: {}", TOPIC, leaderId);
        assertTrue(leaderId == BROKER_ID_0 || leaderId == BROKER_ID_1,
                "Leader id is set to expected value for topic: " + TOPIC);

        // the non-leader broker is the follower
        int followerId = leaderId == BROKER_ID_0 ? BROKER_ID_1 : BROKER_ID_0;
        LOG.debug("Follower for {} is: {}", TOPIC, followerId);

        produceMessage(cluster, TOPIC, "first");
        waitForPartitionMetadata(cluster.brokers().values(), TOPIC, PARTITION_ID);
        assertEquals(List.of("first"), consumeAllMessages(1, groupProtocol));

        // shutdown follower broker
        KafkaBroker follower = cluster.brokers().get(followerId);
        follower.shutdown();
        follower.awaitShutdown();

        produceMessage(cluster, TOPIC, "second");
        assertEquals(List.of("first", "second"), consumeAllMessages(2, groupProtocol));

        //verify that unclean election metric count is 0
        assertEquals(0L, getLeaderElectionCount());

        // shutdown leader and then restart follower
        KafkaBroker leader = cluster.brokers().get(leaderId);
        leader.shutdown();
        leader.awaitShutdown();
        follower.startup();

        // wait until new leader is (uncleanly) elected
        awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.of(followerId), 30000);
        assertEquals(1L, getLeaderElectionCount());

        produceMessage(cluster, TOPIC, "third");

        // second message was lost due to unclean election
        assertEquals(List.of("first", "third"), consumeAllMessages(2, groupProtocol));
    }

    private void verifyUncleanLeaderElectionDisabled(GroupProtocol groupProtocol) throws Exception {
        // wait until leader is elected
        int leaderId = awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.empty());
        LOG.debug("Leader for {} is elected to be: {}", TOPIC, leaderId);
        assertTrue(leaderId == BROKER_ID_0 || leaderId == BROKER_ID_1,
                "Leader id is set to expected value for topic: " + TOPIC);

        // the non-leader broker is the follower
        int followerId = leaderId == BROKER_ID_0 ? BROKER_ID_1 : BROKER_ID_0;
        LOG.debug("Follower for {} is: {}", TOPIC, followerId);

        produceMessage(cluster, TOPIC, "first");
        waitForPartitionMetadata(cluster.brokers().values(), TOPIC, PARTITION_ID);
        assertEquals(List.of("first"), consumeAllMessages(1, groupProtocol));

        // shutdown follower broker
        KafkaBroker follower = cluster.brokers().get(followerId);
        follower.shutdown();
        follower.awaitShutdown();

        produceMessage(cluster, TOPIC, "second");
        assertEquals(List.of("first", "second"), consumeAllMessages(2, groupProtocol));

        //remove any previous unclean election metric
        assertEquals(0L, getLeaderElectionCount());

        // shutdown leader and then restart follower
        KafkaBroker leader = cluster.brokers().get(leaderId);
        leader.shutdown();
        leader.awaitShutdown();
        follower.startup();

        // verify that unclean election to non-ISR follower does not occur.
        // That is, leader should be NO_LEADER(-1) and the ISR should have only old leaderId.
        waitForNoLeaderAndIsrHasOldLeaderId(follower.replicaManager().metadataCache(), leaderId);
        assertEquals(0L, getLeaderElectionCount());

        // message production and consumption should both fail while leader is down
        Throwable e = assertThrows(ExecutionException.class, () ->
                produceMessage(cluster, TOPIC, "third", 1000, 1000));
        assertInstanceOf(TimeoutException.class, e.getCause());

        assertEquals(List.of(), consumeAllMessages(0, groupProtocol));

        // restart leader temporarily to send a successfully replicated message
        leader.startup();
        awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.of(leaderId));

        produceMessage(cluster, TOPIC, "third");
        //make sure follower server joins the ISR
        waitForCondition(
                () -> {
                    Optional<LeaderAndIsr> partitionInfoOpt = follower.metadataCache().getLeaderAndIsr(TOPIC, PARTITION_ID);
                    return partitionInfoOpt.isPresent() && partitionInfoOpt.get().isr().contains(followerId);
                }, DEFAULT_MAX_WAIT_MS, "Inconsistent metadata after first server startup");

        leader.shutdown();
        leader.awaitShutdown();

        // verify clean leader transition to ISR follower
        awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.of(followerId));
        // verify messages can be consumed from ISR follower that was just promoted to leader
        assertEquals(List.of("first", "second", "third"), consumeAllMessages(3, groupProtocol));
    }

    private long getLeaderElectionCount() {
        Meter meter = (Meter) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
                .filter(entry -> entry.getKey().getName().endsWith("UncleanLeaderElectionsPerSec"))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Unable to find metric UncleanLeaderElectionsPerSec"));
        return meter.count();
    }

    private List<String> consumeAllMessages(int numMessages, GroupProtocol groupProtocol) throws Exception {
        Map<String, Object> consumerConfig = Map.of(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.GROUP_ID_CONFIG, "group" + RANDOM.nextLong(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name()
        );

        try (Consumer<String, String> consumer = cluster.consumer(consumerConfig)) {
            consumer.assign(List.of(TOPIC_PARTITION));
            consumer.seekToBeginning(List.of(TOPIC_PARTITION));
            return TestUtils.consumeRecords(consumer, numMessages).stream().map(ConsumerRecord::value).toList();
        }
    }

    @ClusterTest
    public void testTopicUncleanLeaderElectionEnableWithAlterTopicConfigsClassic() throws Exception {
        testTopicUncleanLeaderElectionEnableWithAlterTopicConfigs(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testTopicUncleanLeaderElectionEnableWithAlterTopicConfigsConsumer() throws Exception {
        testTopicUncleanLeaderElectionEnableWithAlterTopicConfigs(GroupProtocol.CONSUMER);
    }

    private void testTopicUncleanLeaderElectionEnableWithAlterTopicConfigs(GroupProtocol groupProtocol) throws Exception {
        disableEligibleLeaderReplicas();

        // create topic with 1 partition, 2 replicas, one on each broker
        NewTopic newTopic = new NewTopic(TOPIC, Map.of(PARTITION_ID, List.of(BROKER_ID_0, BROKER_ID_1)));
        admin.createTopics(List.of(newTopic)).all().get();

        // wait until leader is elected
        int leaderId = awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.empty());

        // the non-leader broker is the follower
        int followerId = leaderId == BROKER_ID_0 ? BROKER_ID_1 : BROKER_ID_0;

        produceMessage(cluster, TOPIC, "first");
        waitForPartitionMetadata(cluster.brokers().values(), TOPIC, PARTITION_ID);
        assertEquals(List.of("first"), consumeAllMessages(1, groupProtocol));

        // Verify the "unclean.leader.election.enable" won't be triggered even if it is enabled/disabled dynamically,
        // because the leader is still alive
        Map<String, String> newProps = Map.of(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true");
        alterTopicConfigs(newProps).all().get();
        // leader should not change to followerId
        awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.of(leaderId), 10000);

        newProps = Map.of(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
        alterTopicConfigs(newProps).all().get();
        // leader should not change to followerId
        awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.of(leaderId), 10000);

        // shutdown follower server
        KafkaBroker follower = cluster.brokers().get(followerId);
        follower.shutdown();
        follower.awaitShutdown();

        produceMessage(cluster, TOPIC, "second");
        assertEquals(List.of("first", "second"), consumeAllMessages(2, groupProtocol));

        // verify that unclean election metric count is 0
        assertEquals(0L, getLeaderElectionCount());

        // shutdown leader and then restart follower
        KafkaBroker leader = cluster.brokers().get(leaderId);
        leader.shutdown();
        leader.awaitShutdown();
        follower.startup();

        // verify that unclean election to non-ISR follower does not occur.
        // That is, leader should be NO_LEADER(-1) and the ISR should have only old leaderId.
        waitForNoLeaderAndIsrHasOldLeaderId(follower.replicaManager().metadataCache(), leaderId);
        assertEquals(0L, getLeaderElectionCount());

        // message production and consumption should both fail while leader is down
        Throwable e = assertThrows(ExecutionException.class, () ->
                produceMessage(cluster, TOPIC, "third", 1000, 1000));
        assertEquals(TimeoutException.class, e.getCause().getClass());

        assertEquals(List.of(), consumeAllMessages(0, groupProtocol));

        // Enable unclean leader election for topic
        newProps = Map.of(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true");
        alterTopicConfigs(newProps).all().get();

        // wait until new leader is (uncleanly) elected
        awaitLeaderChange(cluster, TOPIC_PARTITION, Optional.of(followerId), 30000);
        assertEquals(1L, getLeaderElectionCount());

        produceMessage(cluster, TOPIC, "third");

        // second message was lost due to unclean election
        assertEquals(List.of("first", "third"), consumeAllMessages(2, groupProtocol));
    }

    private AlterConfigsResult alterTopicConfigs(Map<String, String> configs) {
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC);
        Collection<AlterConfigOp> configEntries = configs.entrySet().stream()
                .map(e ->
                        new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
                .toList();
        return admin.incrementalAlterConfigs(Map.of(configResource, configEntries));
    }

    private void waitForNoLeaderAndIsrHasOldLeaderId(MetadataCache metadataCache, int leaderId) throws InterruptedException {
        waitForCondition(
                () -> metadataCache.getLeaderAndIsr(TOPIC, PARTITION_ID)
                        .filter(leaderAndIsr -> leaderAndIsr.leader() == LeaderConstants.NO_LEADER)
                        .filter(leaderAndIsr -> leaderAndIsr.isr().equals(Set.of(leaderId)))
                        .isPresent(),
                DEFAULT_MAX_WAIT_MS,
                "Timed out waiting for broker metadata cache updates the info for topic partition:" + TOPIC_PARTITION);
    }

}
