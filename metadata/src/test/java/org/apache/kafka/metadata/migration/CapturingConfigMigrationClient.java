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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class CapturingConfigMigrationClient implements ConfigMigrationClient {
    public List<ConfigResource> deletedResources = new ArrayList<>();
    public LinkedHashMap<ConfigResource, Map<String, String>> writtenConfigs = new LinkedHashMap<>();

    public void reset() {
        deletedResources.clear();
        writtenConfigs.clear();
    }

    @Override
    public void iterateClientQuotas(ClientQuotaVisitor visitor) {

    }

    @Override
    public void iterateBrokerConfigs(BiConsumer<String, Map<String, String>> configConsumer) {

    }

    @Override
    public void iterateTopicConfigs(BiConsumer<String, Map<String, String>> configConsumer) {

    }

    @Override
    public void readTopicConfigs(String topicName, Consumer<Map<String, String>> configConsumer) {

    }

    @Override
    public ZkMigrationLeadershipState writeConfigs(ConfigResource configResource, Map<String, String> configMap, ZkMigrationLeadershipState state) {
        writtenConfigs.put(configResource, configMap);
        return state;
    }

    @Override
    public ZkMigrationLeadershipState writeClientQuotas(Map<String, String> clientQuotaEntity, Map<String, Double> quotas, Map<String, String> scram, ZkMigrationLeadershipState state) {
        return null;
    }


    @Override
    public ZkMigrationLeadershipState deleteConfigs(ConfigResource configResource, ZkMigrationLeadershipState state) {
        deletedResources.add(configResource);
        return state;
    }

}
