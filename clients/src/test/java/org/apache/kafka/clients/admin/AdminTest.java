package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.admin.internals.ConsumerGroupOperationContext;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.utils.Utils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.*;
import static org.apache.kafka.common.config.ConfigResource.Type.*;
import static org.apache.kafka.common.utils.Utils.*;
import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings({"ConstantConditions", "DanglingJavadoc"})
public class AdminTest {
    private static final Admin admin = Admin.create(mkMap(mkEntry("bootstrap.servers", "127.0.0.1:0")));

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
        assertNpe(() -> admin.close(null));
    }

    @Test
    public void testCreateTopicsNullability() {
        assertNpe(() -> admin.createTopics(null));
        assertNpe(() -> admin.createTopics(singletonList(null)));
        assertNpe(() -> admin.createTopics(singletonList(new NewTopic("valid.topic.name", 1, (short) 1)), null));
    }

    @Test
    public void testDeleteTopicsNullability() {
        assertNpe(() -> admin.deleteTopics(null));
        assertNpe(() -> admin.deleteTopics(singletonList("valid.topic.name"), null));

        try {
            admin.deleteTopics(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidTopicException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testListTopicsNullability() {
        assertNpe(() -> admin.listTopics(null));
    }

    @Test
    public void testDescribeTopicsNullability() {
        assertNpe(() -> admin.describeTopics(null));
        assertNpe(() -> admin.describeTopics(emptyList(), null));

        try {
            admin.describeTopics(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidTopicException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testDescribeClusterNullability() {
        assertNpe(() -> admin.describeCluster(null));
    }

    @Test
    public void testDescribeAclsNullability() {
        assertNpe(() -> admin.describeAcls(null));
        assertNpe(() -> admin.describeAcls(null, null));
    }

    @Test
    public void testCreateAclsNullability() {
        assertNpe(() -> admin.createAcls(null));
        assertNpe(() -> admin.createAcls(singletonList(null)));
        assertNpe(() -> admin.createAcls(emptyList(), null));
    }

    @Test
    public void testDeleteAclsNullability() {
        assertNpe(() -> admin.deleteAcls(null));
        assertNpe(() -> admin.deleteAcls(singletonList(null)));
        assertNpe(() -> admin.deleteAcls(emptyList(), null));
    }

    @Test
    public void testDescribeConfigsNullability() {
        assertNpe(() -> admin.describeConfigs(null));
        assertNpe(() -> admin.describeConfigs(singletonList(null)));
        assertNpe(() -> admin.describeConfigs(singletonList(new ConfigResource(BROKER, "42")), null));
    }

    @Test
    public void testIncrementalAlterConfigsNullability() {
        assertNpe(() -> admin.incrementalAlterConfigs(null));
        assertNpe(() -> admin.incrementalAlterConfigs(mkMap(mkEntry(null, null))));

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

        assertNpe(() -> admin.incrementalAlterConfigs(
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
        assertNpe(() -> admin.alterReplicaLogDirs(null));
        assertNpe(() -> admin.alterReplicaLogDirs(mkMap(mkEntry(null, null))));
        assertNpe(() -> admin.alterReplicaLogDirs(
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
        assertNpe(() -> admin.describeLogDirs(null));
        assertNpe(() -> admin.describeLogDirs(singletonList(null)));
        assertNpe(() -> admin.describeLogDirs(singletonList(42), null));
    }

    @Test
    public void testDescribeReplicaLogDirsNullability() {
        assertNpe(() -> admin.describeReplicaLogDirs(null));
        assertNpe(() -> admin.describeReplicaLogDirs(singletonList(null)));
        assertNpe(() -> admin.describeReplicaLogDirs(singletonList(new TopicPartitionReplica("topic", 1, 1)), null));
    }

    @Test
    public void testCreatePartitionsNullability() {
        assertNpe(() -> admin.createPartitions(null));
        assertNpe(() -> admin.createPartitions(mkMap(mkEntry(null, null))));
        assertNpe(() -> admin.createPartitions(mkMap(mkEntry("topic", null))));
        assertNpe(() -> admin.createPartitions(mkMap(mkEntry("topic", NewPartitions.increaseTo(42))), null));

        /**
         * Does not lead to an NPE but makes no sense either.
         */
        //assertNpe(() -> admin.createPartitions(mkMap(mkEntry(null, NewPartitions.increaseTo(42)))));
    }

    @Test
    public void testDeleteRecordsNullability() {
        assertNpe(() -> admin.deleteRecords(null));
        assertNpe(() -> admin.deleteRecords(mkMap(mkEntry(null, null))));
        assertNpe(() -> admin.deleteRecords(mkMap(mkEntry(null, RecordsToDelete.beforeOffset(42)))));
        assertNpe(() -> admin.deleteRecords(mkMap(mkEntry(new TopicPartition("topic", 42), RecordsToDelete.beforeOffset(42))), null));

        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.deleteRecords(mkMap(mkEntry(new TopicPartition("topic", 42), null))));
    }

    @Test
    public void testCreateDelegationTokenNullability() {
        assertNpe(() -> admin.createDelegationToken(null));
    }

    @Test
    public void testRenewDelegationTokenNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.renewDelegationToken(null));

        assertNpe(() -> admin.renewDelegationToken(new byte[0], null));
    }

    @Test
    public void testExpireDelegationTokenNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.expireDelegationToken(null));

        assertNpe(() -> admin.expireDelegationToken(new byte[0], null));
    }

    @Test
    public void testDescribeDelegationTokenNullability() {
        assertNpe(() -> admin.describeDelegationToken(null));
    }

    @Test
    public void testDescribeConsumerGroupsNullability() {
        assertNpe(() -> admin.describeConsumerGroups(null));
        assertNpe(() -> admin.describeConsumerGroups(singletonList("group"), null));

        try {
            admin.describeConsumerGroups(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidGroupIdException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testListConsumerGroupsNullability() {
        assertNpe(() -> admin.listConsumerGroups(null));
    }

    @Test
    public void testListConsumerGroupOffsetsNullability() {
        assertNpe(() -> admin.listConsumerGroupOffsets("group", null));

        /**
         * Does not lead to an NPE but is assigned to
         * {@link ConsumerGroupOperationContext#groupId} from where it is used
         * in multiple places. In any event, null as groupId makes no sense.
         */
        //assertNpe(() -> admin.listConsumerGroupOffsets(null));
    }

    @Test
    public void testDeleteConsumerGroupsNullability() {
        assertNpe(() -> admin.deleteConsumerGroups(null));
        assertNpe(() -> admin.deleteConsumerGroups(singletonList("group"), null));

        try {
            admin.deleteConsumerGroups(singletonList(null)).all().get();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof InvalidGroupIdException);
            assertTrue(e.getCause().getMessage().contains("'null'"));
        }
    }

    @Test
    public void testDeleteConsumerGroupOffsetsNullability() {
        try {
            admin.deleteConsumerGroupOffsets(null, null).all().get();
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
        assertNpe(() -> admin.deleteConsumerGroupOffsets("group", null).partitionResult(new TopicPartition("topic", 1)));

        /**
         * Does not lead to an NPE but will lead to other exceptions, we cannot
         * test that here because we would need to set up a server and its
         * responses for this to work.
         */
        //assertNpe(() -> admin.deleteConsumerGroupOffsets("group", singleton(null)));

        assertNpe(() -> admin.deleteConsumerGroupOffsets("group", singleton(new TopicPartition("topic", 1)), null));
    }

    @Test
    public void testElectLeadersNullability() {
        //
        //assertNpe(() -> admin.electLeaders(null, null));

        assertNpe(() -> admin.electLeaders(ElectionType.PREFERRED, singleton(new TopicPartition("topic", 1)), null));
    }

    @Test
    public void testAlterPartitionReassignmentsNullability() {
        assertNpe(() -> admin.alterPartitionReassignments(null));
        assertNpe(() -> admin.alterPartitionReassignments(mkMap(mkEntry(null, null))));

        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.alterPartitionReassignments(mkMap(mkEntry(new TopicPartition("topic", 1), null))));

        assertNpe(() -> admin.alterPartitionReassignments(
            mkMap(mkEntry(new TopicPartition("topic", 1), Optional.empty())),
            null
        ));
    }

    @SuppressWarnings("OptionalAssignedToNull")
    @Test
    public void testListPartitionReassignmentsNullability() {
        assertNpe(() -> admin.listPartitionReassignments((Set<TopicPartition>) null));
        assertNpe(() -> admin.listPartitionReassignments(singleton(null)));
        assertNpe(() -> admin.listPartitionReassignments(singleton(new TopicPartition("topic", 1)), null));
        assertNpe(() -> admin.listPartitionReassignments((ListPartitionReassignmentsOptions) null));
        assertNpe(() -> admin.listPartitionReassignments((Optional<Set<TopicPartition>>) null, null));
        assertNpe(() -> admin.listPartitionReassignments((Optional<Set<TopicPartition>>) null, new ListPartitionReassignmentsOptions()));
        assertNpe(() -> admin.listPartitionReassignments(Optional.empty(), null));
        assertNpe(() -> admin.listPartitionReassignments(Optional.of(singleton(null)), null));
        assertNpe(() -> admin.listPartitionReassignments(Optional.of(singleton(null)), new ListPartitionReassignmentsOptions()));
        assertNpe(() -> admin.listPartitionReassignments(Optional.of(singleton(new TopicPartition("topic", 1))), null));
    }

    @Test
    public void testRemoveMembersFromConsumerGroupNullability() {
        assertNpe(() -> admin.removeMembersFromConsumerGroup(null, null));
        assertNpe(() -> admin.removeMembersFromConsumerGroup("group", null));

        try {
            admin.removeMembersFromConsumerGroup(null, new RemoveMembersFromConsumerGroupOptions()).all().get();
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

        assertNpe(() -> admin.alterConsumerGroupOffsets("group", emptyMap(), null));
    }

    @Test
    public void testListOffsetsNullability() {
        assertNpe(() -> admin.listOffsets(null));
        assertNpe(() -> admin.listOffsets(mkMap(mkEntry(null, null))));

        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.listOffsets(mkMap(mkEntry(new TopicPartition("topic", 1), null))));

        assertNpe(() -> admin.listOffsets(mkMap(mkEntry(new TopicPartition("topic", 1), OffsetSpec.earliest())), null));
    }

    @Test
    public void testDescribeClientQuotasNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.describeClientQuotas(null));

        assertNpe(() -> admin.describeClientQuotas(ClientQuotaFilter.all(), null));
    }

    @Test
    public void testAlterClientQuotasNullability() {
        assertNpe(() -> admin.alterClientQuotas(null));
        assertNpe(() -> admin.alterClientQuotas(singletonList(null)));
        assertNpe(() -> admin.alterClientQuotas(singletonList(new ClientQuotaAlteration(new ClientQuotaEntity(emptyMap()), emptyList())), null));
    }

    @Test
    public void testDescribeUserScramCredentialsNullability() {
        /**
         * Only leads to exceptions if executed.
         */
        //assertNpe(() -> admin.describeUserScramCredentials(null));
        //assertNpe(() -> admin.describeUserScramCredentials(singletonList(null)));

        assertNpe(() -> admin.describeUserScramCredentials(emptyList(), null));
    }

    @Test
    public void testAlterUserScramCredentialsNullability() {
        assertNpe(() -> admin.alterUserScramCredentials(null));
        assertNpe(() -> admin.alterUserScramCredentials(singletonList(null)));
        assertNpe(() -> admin.alterUserScramCredentials(singletonList(new UserScramCredentialDeletion("user", ScramMechanism.SCRAM_SHA_256)), null));
    }
}
