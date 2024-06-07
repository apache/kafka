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

import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.ProducerIdsBlock;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class CapturingMigrationClient implements MigrationClient {

    static final MigrationBatchSupplier EMPTY_BATCH_SUPPLIER = new MigrationBatchSupplier() {

    };

    interface MigrationBatchSupplier {
        default List<List<ApiMessageAndVersion>> recordBatches() {
            return Collections.emptyList();
        }

        default List<Integer> brokerIds() {
            return Collections.emptyList();
        }
    }

    static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        Set<Integer> brokersInZk = Collections.emptySet();
        TopicMigrationClient topicMigrationClient = new CapturingTopicMigrationClient();
        ConfigMigrationClient configMigrationClient = new CapturingConfigMigrationClient();
        AclMigrationClient aclMigrationClient = new CapturingAclMigrationClient();
        DelegationTokenMigrationClient delegationTokenMigrationClient = new CapturingDelegationTokenMigrationClient();
        MigrationBatchSupplier batchSupplier = EMPTY_BATCH_SUPPLIER;


        public Builder setBrokersInZk(int... brokerIds) {
            brokersInZk = IntStream.of(brokerIds).boxed().collect(Collectors.toSet());
            return this;
        }

        public Builder setTopicMigrationClient(TopicMigrationClient topicMigrationClient) {
            this.topicMigrationClient = topicMigrationClient;
            return this;
        }

        public Builder setConfigMigrationClient(ConfigMigrationClient configMigrationClient) {
            this.configMigrationClient = configMigrationClient;
            return this;
        }

        public Builder setAclMigrationClient(AclMigrationClient aclMigrationClient) {
            this.aclMigrationClient = aclMigrationClient;
            return this;
        }

        public Builder setDelegationTokenMigrationClient(DelegationTokenMigrationClient delegationTokenMigrationClient) {
            this.delegationTokenMigrationClient = delegationTokenMigrationClient;
            return this;
        }

        public Builder setBatchSupplier(MigrationBatchSupplier batchSupplier) {
            this.batchSupplier = batchSupplier;
            return this;
        }

        public CapturingMigrationClient build() {
            return new CapturingMigrationClient(
                brokersInZk,
                topicMigrationClient,
                configMigrationClient,
                aclMigrationClient,
                delegationTokenMigrationClient,
                batchSupplier
            );
        }
    }

    private final Set<Integer> brokerIds;
    private final TopicMigrationClient topicMigrationClient;
    private final ConfigMigrationClient configMigrationClient;
    private final AclMigrationClient aclMigrationClient;
    private final DelegationTokenMigrationClient delegationTokenMigrationClient;
    private final MigrationBatchSupplier batchSupplier;
    private ZkMigrationLeadershipState state = null;

    CapturingMigrationClient(
        Set<Integer> brokerIdsInZk,
        TopicMigrationClient topicMigrationClient,
        ConfigMigrationClient configMigrationClient,
        AclMigrationClient aclMigrationClient,
        DelegationTokenMigrationClient delegationTokenMigrationClient,
        MigrationBatchSupplier batchSupplier
    ) {
        this.brokerIds = brokerIdsInZk;
        this.topicMigrationClient = topicMigrationClient;
        this.configMigrationClient = configMigrationClient;
        this.aclMigrationClient = aclMigrationClient;
        this.delegationTokenMigrationClient = delegationTokenMigrationClient;
        this.batchSupplier = batchSupplier;
    }

    @Override
    public ZkMigrationLeadershipState getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState initialState) {
        if (this.state == null) {
            this.state = initialState;
        }
        return this.state;
    }

    @Override
    public ZkMigrationLeadershipState setMigrationRecoveryState(ZkMigrationLeadershipState state) {
        this.state = state;
        return state;
    }

    @Override
    public ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state) {
        if (state.zkControllerEpochZkVersion() == ZkMigrationLeadershipState.UNKNOWN_ZK_VERSION) {
            this.state = state.withZkController(0, 0);
        } else {
            this.state = state.withZkController(state.zkControllerEpoch() + 1, state.zkControllerEpochZkVersion() + 1);
        }
        return this.state;
    }

    @Override
    public ZkMigrationLeadershipState releaseControllerLeadership(ZkMigrationLeadershipState state) {
        this.state = state.withUnknownZkController();
        return this.state;
    }


    @Override
    public TopicMigrationClient topicClient() {
        return topicMigrationClient;
    }

    @Override
    public ConfigMigrationClient configClient() {
        return configMigrationClient;
    }

    @Override
    public AclMigrationClient aclClient() {
        return aclMigrationClient;
    }

    @Override
    public DelegationTokenMigrationClient delegationTokenClient() {
        return delegationTokenMigrationClient;
    }

    @Override
    public Optional<ProducerIdsBlock> readProducerId() {
        return Optional.empty();
    }

    @Override
    public ZkMigrationLeadershipState writeProducerId(
        long nextProducerId,
        ZkMigrationLeadershipState state
    ) {
        this.state = state;
        return state;
    }

    @Override
    public void readAllMetadata(
        Consumer<List<ApiMessageAndVersion>> batchConsumer,
        Consumer<Integer> brokerIdConsumer
    ) {
        batchSupplier.recordBatches().forEach(batchConsumer);
        batchSupplier.brokerIds().forEach(brokerIdConsumer);
    }

    @Override
    public Set<Integer> readBrokerIds() {
        return brokerIds;
    }
}
