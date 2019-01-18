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
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;
import java.util.List;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorMonitorConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(MirrorMonitorConnector.class);

    private MirrorConnectorConfig config;

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorConnectorConfig(props);
        log.info("Starting {} for {} -> {}.", config.connectorName(), config.sourceClusterAlias(),
            config.targetClusterAlias());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorMonitorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.singletonList(config.originalsStrings());
    }

    @Override
    public void stop() {
        log.info("Stopped {} for {} -> {}.", config.connectorName(), config.sourceClusterAlias(),
            config.targetClusterAlias());
 
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
