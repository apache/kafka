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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.distributed.NotLeaderException;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.mirror.MirrorMaker.CONNECTOR_CLASSES;

public class MirrorHerder extends DistributedHerder {

    private static final Logger log = LoggerFactory.getLogger(MirrorHerder.class);

    private final MirrorMakerConfig config;
    private final SourceAndTarget sourceAndTarget;
    private boolean wasLeader;

    public MirrorHerder(MirrorMakerConfig mirrorConfig, SourceAndTarget sourceAndTarget, DistributedConfig config, Time time, Worker worker, String kafkaClusterId, StatusBackingStore statusBackingStore, ConfigBackingStore configBackingStore, String restUrl, RestClient restClient, ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy, List<String> restNamespace, AutoCloseable... uponShutdown) {
        super(config, time, worker, kafkaClusterId, statusBackingStore, configBackingStore, restUrl, restClient, connectorClientConfigOverridePolicy, restNamespace, uponShutdown);
        this.config = mirrorConfig;
        this.sourceAndTarget = sourceAndTarget;
    }

    @Override
    protected void rebalanceSuccess() {
        if (isLeader()) {
            if (!wasLeader) {
                log.info("This node {} is now a leader for {}. Configuring connectors...", this, sourceAndTarget);
                configureConnectors();
            }
            wasLeader = true;
        } else {
            wasLeader = false;
        }
    }

    private void configureConnectors() {
        CONNECTOR_CLASSES.forEach(this::maybeConfigureConnector);
    }

    private void maybeConfigureConnector(Class<?> connectorClass) {
        Map<String, String> desiredConfig = config.connectorBaseConfig(sourceAndTarget, connectorClass);
        Map<String, String> actualConfig = configState.connectorConfig(connectorClass.getSimpleName());
        if (actualConfig == null || !actualConfig.equals(desiredConfig)) {
            configureConnector(connectorClass.getSimpleName(), desiredConfig);
        } else {
            log.info("This node is a leader for {} and configuration for {} is already up to date.", sourceAndTarget, connectorClass.getSimpleName());
        }
    }

    private void configureConnector(String connectorName, Map<String, String> connectorProps) {
        putConnectorConfig(connectorName, connectorProps, true, (e, x) -> {
            if (e == null) {
                log.info("{} connector configured for {}.", connectorName, sourceAndTarget);
            } else if (e instanceof NotLeaderException) {
                // No way to determine if the herder is a leader or not beforehand.
                log.info("This node lost leadership for {} while trying to update the connector configuration for {}. Using existing connector configuration.", connectorName, sourceAndTarget);
            } else {
                log.error("Failed to configure {} connector for {}", connectorName, sourceAndTarget, e);
            }
        });
    }

}
