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
package org.apache.kafka.clients;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Optional;

/**
 * Class to extract group rebalance related configs.
 */
public class GroupRebalanceConfig {

    public enum ProtocolType {
        Consumer("consumer"),
        Connect("connect");

        private String type;

        ProtocolType(String type) {
            this.type = type;
        }

        public String get() {
            return type;
        }
    }

    public final int sessionTimeoutMs;
    public final int rebalanceTimeoutMs;
    public final int heartbeatIntervalMs;
    public final String groupId;
    public final Optional<String> groupInstanceId;
    public final long retryBackoffMs;
    public final boolean leaveGroupOnClose;

    public GroupRebalanceConfig(AbstractConfig config, ProtocolType protocolType) {
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);
        if (protocolType == ProtocolType.Consumer) {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        } else {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG);
        }
        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);

        // Static membership is only introduced in consumer API.
        if (protocolType == ProtocolType.Consumer) {
            String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
                this.groupInstanceId = Optional.of(groupInstanceId);
            } else {
                this.groupInstanceId = Optional.empty();
            }
        } else {
            this.groupInstanceId = Optional.empty();
        }

        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);
        if (protocolType == ProtocolType.Consumer) {
            this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
        } else {
            this.leaveGroupOnClose = true;
        }
    }

    // For testing purpose.
    public GroupRebalanceConfig(final int sessionTimeoutMs,
                                final int rebalanceTimeoutMs,
                                final int heartbeatIntervalMs,
                                String groupId,
                                Optional<String> groupInstanceId,
                                long retryBackoffMs,
                                boolean leaveGroupOnClose) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.retryBackoffMs = retryBackoffMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
    }
}
