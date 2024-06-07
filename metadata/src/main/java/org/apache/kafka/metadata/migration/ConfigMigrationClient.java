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

import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.metadata.ClientQuotaRecord;
import org.apache.kafka.common.security.scram.ScramCredential;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface ConfigMigrationClient {

    interface ClientQuotaVisitor {
        void visitClientQuota(List<ClientQuotaRecord.EntityData> entityDataList, Map<String, Double> quotas);

        void visitScramCredential(String userName, ScramMechanism scramMechanism, ScramCredential scramCredential);
    }

    void iterateClientQuotas(ClientQuotaVisitor visitor);

    void iterateBrokerConfigs(BiConsumer<String, Map<String, String>> configConsumer);

    void iterateTopicConfigs(BiConsumer<String, Map<String, String>> configConsumer);

    void readTopicConfigs(String topicName, Consumer<Map<String, String>> configConsumer);

    ZkMigrationLeadershipState writeConfigs(
        ConfigResource configResource,
        Map<String, String> configMap,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState writeClientQuotas(
        Map<String, String> clientQuotaEntity,
        Map<String, Double> quotas,
        Map<String, String> scram,
        ZkMigrationLeadershipState state
    );

    ZkMigrationLeadershipState deleteConfigs(
        ConfigResource configResource,
        ZkMigrationLeadershipState state
    );
}
