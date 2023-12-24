/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group.consumer;

import org.apache.kafka.common.errors.InvalidRequestException;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The consumer group config manager is responsible for config modification and cleaning.
 */
public class ConsumerGroupConfigManager {

    private final ConsumerGroupConfig defaultConfig;

    private final Map<String, ConsumerGroupConfig> configMap;

    public ConsumerGroupConfigManager(Map<?, ?>  defaultConfig) {
        this.configMap = new ConcurrentHashMap<>();
        this.defaultConfig = new ConsumerGroupConfig(defaultConfig);
    }

    /**
     * Update the configuration of the provided group.
     *
     * @param groupId                   The group id.
     * @param newConsumerGroupConfig    The new consumer group config.
     */
    public void updateConsumerGroupConfig(String groupId, Properties newConsumerGroupConfig) {
        if (null == groupId || groupId.isEmpty()) {
            throw new InvalidRequestException("Consumer group name can't be empty.");
        }

        // Validate the configuration
        ConsumerGroupConfig.validate(newConsumerGroupConfig);

        final ConsumerGroupConfig newConfig = ConsumerGroupConfig.fromProps(defaultConfig.originals(),
            newConsumerGroupConfig);
        configMap.put(groupId, newConfig);
    }

    /**
     * Get the consumer group config if it exists, otherwise return None.
     *
     * @param groupId  The group id.
     * @return The consumer group config.
     */
    public Optional<ConsumerGroupConfig> getConsumerGroupConfig(String groupId) {
        if (configMap.containsKey(groupId))
            return Optional.of(configMap.get(groupId));

        return Optional.empty();
    }

    /**
     * Remove all consumer group configs.
     */
    public void close() {
        configMap.clear();
    }
}
