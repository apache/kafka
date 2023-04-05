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

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

class CapturingMigrationClient implements MigrationClient {

    private final Set<Integer> brokerIds;
    private final TopicMigrationClient topicMigrationClient;

    public final Map<ConfigResource, Map<String, String>> capturedConfigs = new HashMap<>();

    public CapturingMigrationClient(
        Set<Integer> brokerIdsInZk,
        TopicMigrationClient topicMigrationClient
    ) {
        this.brokerIds = brokerIdsInZk;
        this.topicMigrationClient = topicMigrationClient;
    }

    @Override
    public ZkMigrationLeadershipState getOrCreateMigrationRecoveryState(ZkMigrationLeadershipState initialState) {
        return initialState;
    }

    @Override
    public ZkMigrationLeadershipState setMigrationRecoveryState(ZkMigrationLeadershipState state) {
        return state;
    }

    @Override
    public ZkMigrationLeadershipState claimControllerLeadership(ZkMigrationLeadershipState state) {
        return state;
    }

    @Override
    public ZkMigrationLeadershipState releaseControllerLeadership(ZkMigrationLeadershipState state) {
        return state;
    }


    @Override
    public TopicMigrationClient topicClient() {
        return topicMigrationClient;
    }

    @Override
    public ConfigMigrationClient configClient() {
        return null;
    }

    @Override
    public AclMigrationClient aclClient() {
        return null;
    }

    @Override
    public ZkMigrationLeadershipState writeProducerId(
            long nextProducerId,
            ZkMigrationLeadershipState state
    ) {
        return state;
    }

    @Override
    public void readAllMetadata(
            Consumer<List<ApiMessageAndVersion>> batchConsumer,
            Consumer<Integer> brokerIdConsumer
    ) {

    }

    @Override
    public Set<Integer> readBrokerIds() {
        return brokerIds;
    }
}
