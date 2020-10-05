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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings({"ConstantConditions", "DanglingJavadoc"})
public class AdminTest {
    private static final Admin ADMIN = Admin.create(mkMap(mkEntry("bootstrap.servers", "127.0.0.1:0")));

    private static void assertNpe(final @NotNull Executable exe) {
        assertThrows(NullPointerException.class, exe);
    }

    @Test
    public void testCreateNullability() {
        assertNpe(() -> Admin.create((Properties) null));
        assertNpe(() -> Admin.create((Map<String, Object>) null));
        assertNpe(() -> Admin.create(mkMap(mkEntry(null, null))));
        assertNpe(() -> Admin.create(mkMap(mkEntry(null, "value"))));
        assertNpe(() -> {
            try {
                Admin.create(Utils.mkMap(mkEntry("bootstrap.servers", null)));
            } catch (KafkaException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    public void testCloseNullability() {
        assertNpe(() -> ADMIN.close(null));
    }

    @Test
    public void testCreateTopicsNullability() {
        assertNpe(() -> ADMIN.createTopics(null));
        assertNpe(() -> ADMIN.createTopics(singletonList(null)));
        assertNpe(() -> ADMIN.createTopics(singletonList(new NewTopic("valid.topic.name", 1, (short) 1)), null));
    }

    @Test
    public void testDeleteTopicsNullability() {
        assertNpe(() -> ADMIN.deleteTopics(null));
        assertNpe(() -> ADMIN.deleteTopics(singletonList("valid.topic.name"), null));

        try {
            ADMIN.deleteTopics(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidTopicException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testListTopicsNullability() {
        assertNpe(() -> ADMIN.listTopics(null));
    }

    @Test
    public void testDescribeTopicsNullability() {
        assertNpe(() -> ADMIN.describeTopics(null));
        assertNpe(() -> ADMIN.describeTopics(emptyList(), null));

        try {
            ADMIN.describeTopics(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidTopicException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testDescribeClusterNullability() {
        assertNpe(() -> ADMIN.describeCluster(null));
    }

    @Test
    public void testDescribeAclsNullability() {
        assertNpe(() -> ADMIN.describeAcls(null));
        assertNpe(() -> ADMIN.describeAcls(null, null));
    }

    @Test
    public void testCreateAclsNullability() {
        assertNpe(() -> ADMIN.createAcls(null));
        assertNpe(() -> ADMIN.createAcls(singletonList(null)));
        assertNpe(() -> ADMIN.createAcls(emptyList(), null));
    }

    @Test
    public void testDeleteAclsNullability() {
        assertNpe(() -> ADMIN.deleteAcls(null));
        assertNpe(() -> ADMIN.deleteAcls(singletonList(null)));
        assertNpe(() -> ADMIN.deleteAcls(emptyList(), null));
    }

    @Test
    public void testDescribeConfigsNullability() {
        assertNpe(() -> ADMIN.describeConfigs(null));
        assertNpe(() -> ADMIN.describeConfigs(singletonList(null)));
        assertNpe(() -> ADMIN.describeConfigs(singletonList(new ConfigResource(BROKER, "42")), null));
    }

    @Test
    public void testIncrementalAlterConfigsNullability() {
        assertNpe(() -> ADMIN.incrementalAlterConfigs(null));
        assertNpe(() -> ADMIN.incrementalAlterConfigs(mkMap(mkEntry(null, null))));

        /**
         * The following calls are going to lead to an NPE but we cannot test it
         * because it happens only if we perform the call. However, we cannot
         * perform the call here since it runs into a timeout as there is no
         * backend to call. Have a look at
         * {@link KafkaAdminClient#toIncrementalAlterConfigsRequestData(Collection, Map, boolean)}
         * where we unconditionally loop over the collection and unconditionally
         * access the members of the elements in the collection.
         */
        //assertNpe(() -> admin.incrementalAlterConfigs(mkMap(mkEntry(new ConfigResource(BROKER, "42"), null))));
        //assertNpe(() -> admin.incrementalAlterConfigs(mkMap(mkEntry(new ConfigResource(BROKER, "42"), singletonList(null)))));

        assertNpe(() -> ADMIN.incrementalAlterConfigs(
            mkMap(
                mkEntry(
                    new ConfigResource(BROKER, "42"),
                    singletonList(
                        new AlterConfigOp(
                            new ConfigEntry("key", "value"),
                            AlterConfigOp.OpType.SET
                        )
                    )
                )
            ),
            null
        ));
    }

    @Test
    public void testAlterReplicaLogDirsNullability() {
        assertNpe(() -> ADMIN.alterReplicaLogDirs(null));
        assertNpe(() -> ADMIN.alterReplicaLogDirs(mkMap(mkEntry(null, null))));
        assertNpe(() -> ADMIN.alterReplicaLogDirs(
            mkMap(
                mkEntry(
                    new TopicPartitionReplica("topic", 1, 1),
                    "/some/path"
                )
            ),
            null
        ));

        /**
         * This does not seem to lead to an NPE but it also does not make sense
         * to allow null here because it will be assigned to
         * {@link AlterReplicaLogDirsRequestData.AlterReplicaLogDir#path} and
         * it is not clear what a path of null should represent.
         */
        //assertNpe(() -> admin.alterReplicaLogDirs(mkMap(mkEntry(new TopicPartitionReplica("topic", 1, 1), null))));
    }

    @Test
    public void testDescribeLogDirsNullability() {
        assertNpe(() -> ADMIN.describeLogDirs(null));
        assertNpe(() -> ADMIN.describeLogDirs(singletonList(null)));
        assertNpe(() -> ADMIN.describeLogDirs(singletonList(42), null));
    }

    @Test
    public void testDescribeReplicaLogDirsNullability() {
        assertNpe(() -> ADMIN.describeReplicaLogDirs(null));
        assertNpe(() -> ADMIN.describeReplicaLogDirs(singletonList(null)));
        assertNpe(() -> ADMIN.describeReplicaLogDirs(singletonList(new TopicPartitionReplica("topic", 1, 1)), null));
    }

    @Test
    public void testCreatePartitionsNullability() {
        assertNpe(() -> ADMIN.createPartitions(null));
        assertNpe(() -> ADMIN.createPartitions(mkMap(mkEntry(null, null))));
        assertNpe(() -> ADMIN.createPartitions(mkMap(mkEntry("topic", null))));
        assertNpe(() -> ADMIN.createPartitions(mkMap(mkEntry("topic", NewPartitions.increaseTo(42))), null));

        /**
         * Does not lead to an NPE but makes no sense either.
         */
        //assertNpe(() -> admin.createPartitions(mkMap(mkEntry(null, NewPartitions.increaseTo(42)))));
    }

    @Test
    public void testDeleteRecordsNullability() {
        assertNpe(() -> ADMIN.deleteRecords(null));
        assertNpe(() -> ADMIN.deleteRecords(mkMap(mkEntry(null, null))));
        assertNpe(() -> ADMIN.deleteRecords(mkMap(mkEntry(null, RecordsToDelete.beforeOffset(42)))));
        assertNpe(() -> ADMIN.deleteRecords(mkMap(mkEntry(new TopicPartition("topic", 42), RecordsToDelete.beforeOffset(42))), null));

        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.deleteRecords(mkMap(mkEntry(new TopicPartition("topic", 42), null))));
    }

    @Test
    public void testCreateDelegationTokenNullability() {
        assertNpe(() -> ADMIN.createDelegationToken(null));
    }

    @Test
    public void testRenewDelegationTokenNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.renewDelegationToken(null));

        assertNpe(() -> ADMIN.renewDelegationToken(new byte[0], null));
    }

    @Test
    public void testExpireDelegationTokenNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.expireDelegationToken(null));

        assertNpe(() -> ADMIN.expireDelegationToken(new byte[0], null));
    }

    @Test
    public void testDescribeDelegationTokenNullability() {
        assertNpe(() -> ADMIN.describeDelegationToken(null));
    }

    @Test
    public void testDescribeConsumerGroupsNullability() {
        assertNpe(() -> ADMIN.describeConsumerGroups(null));
        assertNpe(() -> ADMIN.describeConsumerGroups(singletonList("group"), null));

        try {
            ADMIN.describeConsumerGroups(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidGroupIdException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testListConsumerGroupsNullability() {
        assertNpe(() -> ADMIN.listConsumerGroups(null));
    }

    @Test
    public void testListConsumerGroupOffsetsNullability() {
        assertNpe(() -> ADMIN.listConsumerGroupOffsets("group", null));

        /**
         * Does not lead to an NPE but is assigned to
         * {@link ConsumerGroupOperationContext#groupId} from where it is used
         * in multiple places. In any event, null as groupId makes no sense.
         */
        //assertNpe(() -> admin.listConsumerGroupOffsets(null));
    }

    @Test
    public void testDeleteConsumerGroupsNullability() {
        assertNpe(() -> ADMIN.deleteConsumerGroups(null));
        assertNpe(() -> ADMIN.deleteConsumerGroups(singletonList("group"), null));

        try {
            ADMIN.deleteConsumerGroups(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidGroupIdException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNullability() {
        try {
            ADMIN.deleteConsumerGroupOffsets(null, null).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidGroupIdException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }

        /**
         * NPEs here are thrown much later because it is passed to
         * {@link DeleteConsumerGroupOffsetsResult#partitions} and depending on
         * which method is called a different exception will be thrown. This
         * call leads to an NPE because contains is called unconditionally.
         */
        assertNpe(() -> ADMIN.deleteConsumerGroupOffsets("group", null).partitionResult(new TopicPartition("topic", 1)));

        /**
         * Does not lead to an NPE but will lead to other exceptions, we cannot
         * test that here because we would need to set up a server and its
         * responses for this to work.
         */
        //assertNpe(() -> admin.deleteConsumerGroupOffsets("group", singleton(null)));

        assertNpe(() -> ADMIN.deleteConsumerGroupOffsets("group", singleton(new TopicPartition("topic", 1)), null));
    }

    @Test
    public void testElectLeadersNullability() {
        assertNpe(() -> ADMIN.electLeaders(ElectionType.PREFERRED, singleton(new TopicPartition("topic", 1)), null));
    }

    @Test
    public void testAlterPartitionReassignmentsNullability() {
        assertNpe(() -> ADMIN.alterPartitionReassignments(null));
        assertNpe(() -> ADMIN.alterPartitionReassignments(mkMap(mkEntry(null, null))));

        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.alterPartitionReassignments(mkMap(mkEntry(new TopicPartition("topic", 1), null))));

        assertNpe(() -> ADMIN.alterPartitionReassignments(
            mkMap(mkEntry(new TopicPartition("topic", 1), Optional.empty())),
            null
        ));
    }

    @SuppressWarnings("OptionalAssignedToNull")
    @Test
    public void testListPartitionReassignmentsNullability() {
        assertNpe(() -> ADMIN.listPartitionReassignments((Set<TopicPartition>) null));
        assertNpe(() -> ADMIN.listPartitionReassignments(singleton(null)));
        assertNpe(() -> ADMIN.listPartitionReassignments(singleton(new TopicPartition("topic", 1)), null));
        assertNpe(() -> ADMIN.listPartitionReassignments((ListPartitionReassignmentsOptions) null));
        assertNpe(() -> ADMIN.listPartitionReassignments((Optional<Set<TopicPartition>>) null, null));
        assertNpe(() -> ADMIN.listPartitionReassignments((Optional<Set<TopicPartition>>) null, new ListPartitionReassignmentsOptions()));
        assertNpe(() -> ADMIN.listPartitionReassignments(Optional.empty(), null));
        assertNpe(() -> ADMIN.listPartitionReassignments(Optional.of(singleton(null)), null));
        assertNpe(() -> ADMIN.listPartitionReassignments(Optional.of(singleton(null)), new ListPartitionReassignmentsOptions()));
        assertNpe(() -> ADMIN.listPartitionReassignments(Optional.of(singleton(new TopicPartition("topic", 1))), null));
    }

    @Test
    public void testRemoveMembersFromConsumerGroupNullability() {
        assertNpe(() -> ADMIN.removeMembersFromConsumerGroup(null, null));
        assertNpe(() -> ADMIN.removeMembersFromConsumerGroup("group", null));

        try {
            ADMIN.removeMembersFromConsumerGroup(null, new RemoveMembersFromConsumerGroupOptions()).all().get();
        } catch (Exception e) {
            assertTrue(e instanceof KafkaException);
            assertTrue(e.getMessage().endsWith(" null"));
        }
    }

    @Test
    public void testAlterConsumerGroupOffsetsNullability() {
        /**
         * groupId and offsets only lead to exceptions if executed.
         */

        assertNpe(() -> ADMIN.alterConsumerGroupOffsets("group", emptyMap(), null));
    }

    @Test
    public void testListOffsetsNullability() {
        assertNpe(() -> ADMIN.listOffsets(null));
        assertNpe(() -> ADMIN.listOffsets(mkMap(mkEntry(null, null))));

        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.listOffsets(mkMap(mkEntry(new TopicPartition("topic", 1), null))));

        assertNpe(() -> ADMIN.listOffsets(mkMap(mkEntry(new TopicPartition("topic", 1), OffsetSpec.earliest())), null));
    }

    @Test
    public void testDescribeClientQuotasNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.describeClientQuotas(null));

        assertNpe(() -> ADMIN.describeClientQuotas(ClientQuotaFilter.all(), null));
    }

    @Test
    public void testAlterClientQuotasNullability() {
        assertNpe(() -> ADMIN.alterClientQuotas(null));
        assertNpe(() -> ADMIN.alterClientQuotas(singletonList(null)));
        assertNpe(() -> ADMIN.alterClientQuotas(singletonList(new ClientQuotaAlteration(new ClientQuotaEntity(emptyMap()), emptyList())), null));
    }

    @Test
    public void testDescribeUserScramCredentialsNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.describeUserScramCredentials(null));
        //assertNpe(() -> admin.describeUserScramCredentials(singletonList(null)));

        assertNpe(() -> ADMIN.describeUserScramCredentials(emptyList(), null));
    }

    @Test
    public void testAlterUserScramCredentialsNullability() {
        assertNpe(() -> ADMIN.alterUserScramCredentials(null));
        assertNpe(() -> ADMIN.alterUserScramCredentials(singletonList(null)));
        assertNpe(() -> ADMIN.alterUserScramCredentials(singletonList(new UserScramCredentialDeletion("user", ScramMechanism.SCRAM_SHA_256)), null));
    }
}
