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
package org.apache.kafka.metadata.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Methods for interacting with ZooKeeper during a KIP-866 migration. The migration leadership state is stored under
 * a ZNode /migration. All write operations to ZK during the migration are performed as a multi-op transaction which
 * also updates the state of /migration.
 */
public interface MigrationClient {

    /**
     * Read or initialize the ZK migration leader state in ZK. If the ZNode is absent, the given {@code initialState}
     * will be written and subsequently returned with the zkVersion of the node. If the ZNode is present, it will be
     * read and returned.
     * @param initialState  An initial, emtpy, state to write to ZooKeeper for the migration state.
     * @return  The existing migration state, or the initial state given.
     */
    ZkMigrationLeadershipState getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState initialState);

    /**
     * Overwrite the migration state in ZK. This is done as a conditional update using
     * {@link ZkMigrationLeadershipState#migrationZkVersion()}. If the conditional update fails, an exception is thrown.
     * @param state The migration state to persist
     * @return  The persisted migration state or an exception.
     */
    ZkMigrationLeadershipState setMigrationRecoveryState(ZkMigrationLeadershipState state);

    /**
     * Attempt to claim controller leadership of the cluster in ZooKeeper. This involves overwriting the /controller
     * and /controller_epoch ZNodes. The epoch given by {@code state} must be greater than the current epoch in ZooKeeper.
     *
     *
     * @param state The current migration leadership state
     * @return      An updated migration leadership state including the version of /controller_epoch ZNode, if the
     *              leadership claim was successful. Otherwise, return the previous state unmodified.
     */
    ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state);

    /**
     * Release an existing claim on the cluster leadership in ZooKeeper. This involves deleting the /controller ZNode
     * so that another controller can claim leadership.
     *
     * @param state The current migration leadership state.
     * @return      An updated migration leadership state with controllerZkVersion = 1, or raise an exception if ZooKeeper
     *
     *
     */
    ZkMigrationLeadershipState releaseControllerLeadership(ZkMigrationLeadershipState state);

    ZkMigrationLeadershipState createTopic(
        String topicName,
        Uuid topicId,
        Map<Integer, PartitionRegistration> topicPartitions,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState updateTopicPartitions(
        Map<String, Map<Integer, PartitionRegistration>> topicPartitions,
        ZkMigrationLeadershipState state
    );

    void readAllMetadata(Consumer<List<ApiMessageAndVersion>> batchConsumer, Consumer<Integer> brokerIdConsumer);

    Set<Integer> readBrokerIds();

    Set<Integer> readBrokerIdsFromTopicAssignments();
}
