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

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.List;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorHeartbeatConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorHeartbeatConnector.class);

    private String connectorName;
    private MirrorConnectorConfig config;
    private SourceAndTarget sourceAndTarget;
    private boolean enabled;

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorConnectorConfig(props);
        connectorName = config.connectorName();
        sourceAndTarget = new SourceAndTarget(config.sourceClusterAlias(), config.targetClusterAlias());
        enabled = config.enabled();
        if (!enabled) {
            log.info("{} for {} is disabled.", connectorName, sourceAndTarget);
            return;
        }
        log.info("Starting {}.", connectorName);
    }

    @Override
    public void stop() {
        if (enabled) {
            log.info("Stopping {}.", connectorName);
        }
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorHeartbeatTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (!enabled) {
            return Collections.emptyList();
        } else {
            // just need a single task
            return Collections.singletonList(config.originalsStrings());
        }
    }

    @Override
    public ConfigDef config() {
        return MirrorConnectorConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return "WIP";
    }
}
