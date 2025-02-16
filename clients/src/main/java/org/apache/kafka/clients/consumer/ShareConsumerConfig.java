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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class ShareConsumerConfig extends ConsumerConfig {
    /**
     * A list of configuration keys not supported for SHARE protocol.
     */
    private static final List<String> SHARE_PROTOCOL_UNSUPPORTED_CONFIGS = List.of(
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
            ConsumerConfig.ISOLATION_LEVEL_CONFIG,
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
            ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
            ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG
    );

    public ShareConsumerConfig(Properties props) {
        super(props);
    }

    public ShareConsumerConfig(Map<String, Object> props) {
        super(props);
    }

    protected ShareConsumerConfig(Map<?, ?> props, boolean doLog) {
        super(props, doLog);
    }


    @Override
    protected void checkUnsupportedConfigs() {
        List<String> invalidConfigs = new ArrayList<>();
        SHARE_PROTOCOL_UNSUPPORTED_CONFIGS.forEach(configName -> {
            Object config = originals().get(configName);
            if (config != null && !Utils.isBlank(config.toString())) {
                invalidConfigs.add(configName);
            }
        });
        if (!invalidConfigs.isEmpty()) {
            throw new ConfigException(String.join(", ", invalidConfigs) +
                    " cannot be set when prototype when using shareConsumer.");
        }
    }

}
