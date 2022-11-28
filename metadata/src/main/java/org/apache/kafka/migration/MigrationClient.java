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
package org.apache.kafka.migration;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.AbstractControlRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public interface MigrationClient {
    ZkControllerState claimControllerLeadership(int kraftControllerId, int kraftControllerEpoch);

    void readAllMetadata(Consumer<List<ApiMessageAndVersion>> batchConsumer, Consumer<Integer> brokerIdConsumer);

    void addZkBroker(int brokerId);

    void removeZkBroker(int brokerId);

    Set<Integer> readBrokerIds();

    Set<Integer> readBrokerIdsFromTopicAssignments();

    MigrationRecoveryState getOrCreateMigrationRecoveryState(MigrationRecoveryState initialState);

    MigrationRecoveryState setMigrationRecoveryState(MigrationRecoveryState initialState);

    MigrationRecoveryState createTopic(String topicName, Uuid topicId, Map<Integer, PartitionRegistration> topicPartitions, MigrationRecoveryState state);

    MigrationRecoveryState updateTopicPartitions(Map<String, Map<Integer, PartitionRegistration>> topicPartitions, MigrationRecoveryState state);

    void sendRequestToBroker(int brokerId,
                             AbstractControlRequest.Builder<? extends AbstractControlRequest> request,
                             Consumer<AbstractResponse> callback);
}
