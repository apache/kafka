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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.FinalizedVersionRange;
import org.apache.kafka.clients.admin.LogDirDescription;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.admin.UpdateFeaturesResult;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.server.config.ServerLogConfigs.CORDONED_LOG_DIRS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
        disksPerBroker = 2
)
public class CordonedLogDirsIntegrationTest {

    private static final String TOPIC1 = "topic1";
    private static final String TOPIC2 = "topic2";
    private static final ConfigResource BROKER_0 = new ConfigResource(ConfigResource.Type.BROKER, "0");
    private final ClusterInstance clusterInstance;
    private List<String> logDirsBroker0;

    public CordonedLogDirsIntegrationTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() {
        logDirsBroker0 = clusterInstance.brokers().get(0).config().logDirs();
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_2_IV1)
    public void testFeatureNotEnabled() throws Exception {
        testFeatureNotEnabled(List.of());
    }

    @ClusterTest(
        metadataVersion = MetadataVersion.IBP_4_2_IV1,
        serverProperties = {
            @ClusterConfigProperty(key = CORDONED_LOG_DIRS_CONFIG, value = "*")
        }
    )
    public void testFeatureNotEnabledStaticConfig() throws Exception {
        testFeatureNotEnabled(logDirsBroker0);
    }

    private void testFeatureNotEnabled(List<String> initialCordonedLogDirs) throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            // When the metadata version does not support cordoning log dirs:
            // 1. we can create a topic, even if cordon.log.dirs is statically set
            admin.createTopics(newTopic(TOPIC1)).all().get();
            // 2. no log dirs are marked as cordoned
            assertCordonedLogDirs(admin, List.of());
            // 3. we can't dynamically configure cordoned.log.dirs
            Throwable ee = assertThrows(ExecutionException.class, () ->
                admin.incrementalAlterConfigs(cordonedDirsConfig("")).all().get());
            assertInstanceOf(InvalidConfigurationException.class, ee.getCause());

            // Update the metadata version to support cordoning log dirs
            short metadataVersion = MetadataVersion.IBP_4_3_IV0.featureLevel();
            UpdateFeaturesResult updateResult = admin.updateFeatures(
                    Map.of("metadata.version", new FeatureUpdate(metadataVersion, FeatureUpdate.UpgradeType.UPGRADE)),
                    new UpdateFeaturesOptions());
            updateResult.all().get();
            TestUtils.waitForCondition(() -> {
                FinalizedVersionRange versionRange = admin.describeFeatures().featureMetadata().get().finalizedFeatures().get(MetadataVersion.FEATURE_NAME);
                return versionRange.maxVersionLevel() == metadataVersion && versionRange.minVersionLevel() == metadataVersion;
            }, 10_000, "Unable to update the metadata version.");
            Thread.sleep(clusterInstance.brokers().get(0).config().brokerHeartbeatIntervalMs());

            if (initialCordonedLogDirs.isEmpty()) {
                // if no initial cordoned log dirs, this has not changed, and we can cordon log dirs
                assertCordonedLogDirs(admin, List.of());
                setCordonedLogDirs(admin, logDirsBroker0);
                initialCordonedLogDirs = logDirsBroker0;
            }
            // The statically or dynamically configured log dirs are now marked as cordoned
            assertCordonedLogDirs(admin, initialCordonedLogDirs);

            // As all log dirs are cordoned, we can't create a topic
            Set<NewTopic> newTopics = newTopic(TOPIC2);
            ee = assertThrows(ExecutionException.class, () ->
                admin.createTopics(newTopics).all().get());
            assertInstanceOf(InvalidReplicationFactorException.class, ee.getCause());
            // We can't create partitions either
            Map<String, NewPartitions> newPartitions = Map.of(TOPIC1, NewPartitions.increaseTo(2));
            ee = assertThrows(ExecutionException.class, () ->
                admin.createPartitions(newPartitions).all().get());
            assertInstanceOf(InvalidReplicationFactorException.class, ee.getCause());

            // After uncordoning log dirs, we can create topics and partitions again
            setCordonedLogDirs(admin, List.of());
            admin.createTopics(newTopics).all().get();
            admin.createPartitions(newPartitions).all().get();
        }
    }

    @ClusterTest()
    public void testCordonUncordonLogDirs() throws Exception {
        try (Admin admin = clusterInstance.admin()) {
            // No initial cordoned log dirs
            assertCordonedLogDirs(admin, List.of());

            // We can create topics
            admin.createTopics(newTopic(TOPIC1)).all().get();

            // Cordon all log dirs
            setCordonedLogDirs(admin, logDirsBroker0);
            assertCordonedLogDirs(admin, logDirsBroker0);

            // We can't create new topics or partitions
            Set<NewTopic> newTopics = newTopic(TOPIC2);
            Throwable ee = assertThrows(ExecutionException.class, () ->
                admin.createTopics(newTopics).all().get()
            );
            assertInstanceOf(InvalidReplicationFactorException.class, ee.getCause());
            Map<String, NewPartitions> newPartitions = Map.of(TOPIC1, NewPartitions.increaseTo(2));
            ee = assertThrows(ExecutionException.class, () ->
                admin.createPartitions(newPartitions).all().get()
            );
            assertInstanceOf(InvalidReplicationFactorException.class, ee.getCause());

            // Uncordon all log dirs
            setCordonedLogDirs(admin, List.of(logDirsBroker0.get(0)));
            assertCordonedLogDirs(admin, List.of(logDirsBroker0.get(0)));

            // We can create topics and partitions again
            admin.createTopics(newTopics).all().get();
            admin.createPartitions(newPartitions).all().get();
        }
    }

    @ClusterTest(
        serverProperties = {
            @ClusterConfigProperty(key = CORDONED_LOG_DIRS_CONFIG, value = "*")
        }
    )
    public void testStaticCordonUncordonLogDirs() throws Exception {
        Set<NewTopic> newTopics = newTopic(TOPIC1);
        try (Admin admin = clusterInstance.admin()) {
            // All log dirs are statically cordoned, so we can't create topics
            Throwable ee = assertThrows(ExecutionException.class, () ->
                admin.createTopics(newTopics).all().get()
            );
            assertInstanceOf(InvalidReplicationFactorException.class, ee.getCause());

            // Uncordon log dirs
            setCordonedLogDirs(admin, List.of());

            // We can't create topics again
            admin.createTopics(newTopics).all().get();
        }
    }

    @ClusterTest
    public void testReassignWithCordonedLogDirs() throws Exception {
        TopicPartitionReplica replica = new TopicPartitionReplica(TOPIC1, 0, 0);
        try (Admin admin = clusterInstance.admin()) {
            admin.createTopics(newTopic(TOPIC1)).all().get();

            // Find the log dir that does not host the replica and cordon it
            AtomicReference<String> logDir = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                DescribeReplicaLogDirsResult.ReplicaLogDirInfo info = admin.describeReplicaLogDirs(List.of(replica)).all().get().get(replica);
                logDir.set(info.getCurrentReplicaLogDir());
                return info.getCurrentReplicaLogDir() != null;
            }, 10_000, "Unable to find logdir for topic " + replica.topic());
            assertNotNull(logDir.get());
            String otherLogDir = logDirsBroker0.stream().filter(dir -> !dir.equals(logDir.get())).findFirst().get();
            setCordonedLogDirs(admin, List.of(otherLogDir));

            // We can't move the replica to the now cordoned log dir
            Throwable ee = assertThrows(ExecutionException.class, () ->
                admin.alterReplicaLogDirs(Map.of(replica, otherLogDir)).all().get()
            );
            assertInstanceOf(InvalidReplicaAssignmentException.class, ee.getCause());

            // After uncordoning the log dir, we can move the replica on it
            setCordonedLogDirs(admin, List.of());
            admin.alterReplicaLogDirs(Map.of(replica, otherLogDir)).all().get();
        }
    }

    private Map<ConfigResource, Collection<AlterConfigOp>> cordonedDirsConfig(String value) {
        return Map.of(
                BROKER_0,
                Set.of(new AlterConfigOp(new ConfigEntry(CORDONED_LOG_DIRS_CONFIG, value), AlterConfigOp.OpType.SET))
        );
    }

    private void setCordonedLogDirs(Admin admin, List<String> logDirs) throws ExecutionException, InterruptedException {
        String logDirsStr = String.join(",", logDirs);
        admin.incrementalAlterConfigs(cordonedDirsConfig(logDirsStr)).all().get();
        TestUtils.waitForCondition(() -> {
            Map<ConfigResource, Config> describeConfigs = admin.describeConfigs(Set.of(BROKER_0)).all().get();
            Config config = describeConfigs.get(BROKER_0);
            return logDirsStr.equals(config.get(CORDONED_LOG_DIRS_CONFIG).value());
        }, 10_000, "Unable to set the " + CORDONED_LOG_DIRS_CONFIG + " configuration.");
    }

    private Set<NewTopic> newTopic(String name) {
        return Set.of(new NewTopic(name, 1, (short) clusterInstance.brokers().size()));
    }

    private void assertCordonedLogDirs(Admin admin, List<String> expectedCordoned) throws ExecutionException, InterruptedException {
        Map<Integer, Map<String, LogDirDescription>> logDescriptionsPerBroker = admin.describeLogDirs(clusterInstance.brokerIds()).allDescriptions().get();
        for (Map.Entry<Integer, Map<String, LogDirDescription>> logDescriptions : logDescriptionsPerBroker.entrySet()) {
            for (Map.Entry<String, LogDirDescription> logDescription : logDescriptions.getValue().entrySet()) {
                if (logDescriptions.getKey().equals(0) && expectedCordoned.contains(logDescription.getKey())) {
                    assertTrue(logDescription.getValue().isCordoned());
                } else {
                    assertFalse(logDescription.getValue().isCordoned());
                }
            }
        }
    }
}
