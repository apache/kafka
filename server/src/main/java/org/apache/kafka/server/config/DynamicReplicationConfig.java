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
package org.apache.kafka.server.config;

import java.util.Set;

public class DynamicReplicationConfig implements BrokerReconfigurable {

    public static final Set<String> RECONFIGURABLE_CONFIGS = Set.of(
            ReplicationConfigs.FOLLOWER_FETCH_LAST_TIERED_OFFSET_ENABLE_CONFIG);

    @Override
    public Set<String> reconfigurableConfigs() {
        return RECONFIGURABLE_CONFIGS;
    }

    @Override
    public void validateReconfiguration(AbstractKafkaConfig newConfig) {
        // Currently it is a noop for reconfiguring the dynamic config follower.fetch.last.tiered.offset.enable
    }

    @Override
    public void reconfigure(AbstractKafkaConfig oldConfig, AbstractKafkaConfig newConfig) {
        // Currently it is a noop for reconfiguring the dynamic config follower.fetch.last.tiered.offset.enable
    }

    @Override
    public String toString() {
        return "DynamicReplicationConfig";
    }
}
