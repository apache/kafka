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

package org.apache.kafka.coordinator.group.streams;

import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_ASSIGNOR_NAME_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupConfig.STREAMS_NUM_WARMUP_CONFIG;

import org.apache.kafka.coordinator.group.ConfigUpdateEvent;
import org.apache.kafka.coordinator.group.GroupConfigListener;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class StreamsRebalanceConfigListener implements GroupConfigListener {

    private static final Set<String> PROPERTIES_TO_TRIGGER_REBALANCE = Set.of(
            STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            STREAMS_ASSIGNOR_NAME_CONFIG,
            STREAMS_NUM_WARMUP_CONFIG,
            STREAMS_ACCEPTABLE_RECOVERY_LAG_CONFIG
    );
    private final AtomicBoolean triggerRebalance = new AtomicBoolean(false);

    @Override
    public void onConfigUpdated(String groupId, Properties newGroupConfig) {
        newGroupConfig.keySet().forEach(property -> {
            if (PROPERTIES_TO_TRIGGER_REBALANCE.contains(property)) {
                triggerRebalance.set(true);
            }
        });
    }

    @Override
    public ConfigUpdateEvent getUpdateEvent() {
        if (triggerRebalance.getAndSet(false)) {
            return ConfigUpdateEvent.REBALANCE_REQUIRED;
        }
        return ConfigUpdateEvent.UNCHANGED;
    }
}
