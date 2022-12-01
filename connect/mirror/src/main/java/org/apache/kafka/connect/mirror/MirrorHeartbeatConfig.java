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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.runtime.ConnectorConfig;

import java.time.Duration;
import java.util.Map;

public class MirrorHeartbeatConfig extends MirrorConnectorConfig {

    protected static final String EMIT_HEARTBEATS = "emit.heartbeats";

    public static final String HEARTBEATS_TOPIC_REPLICATION_FACTOR = "heartbeats.topic.replication.factor";
    public static final String HEARTBEATS_TOPIC_REPLICATION_FACTOR_DOC = "Replication factor for heartbeats topic.";
    public static final short HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT = 3;

    public static final String EMIT_HEARTBEATS_ENABLED = EMIT_HEARTBEATS + ENABLED_SUFFIX;
    private static final String EMIT_HEARTBEATS_ENABLED_DOC = "Whether to emit heartbeats to target cluster.";
    public static final boolean EMIT_HEARTBEATS_ENABLED_DEFAULT = true;
    public static final String EMIT_HEARTBEATS_INTERVAL_SECONDS = EMIT_HEARTBEATS + INTERVAL_SECONDS_SUFFIX;
    private static final String EMIT_HEARTBEATS_INTERVAL_SECONDS_DOC = "Frequency of heartbeats.";
    public static final long EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT = 1;

    public MirrorHeartbeatConfig(Map<String, String> props) {
        super(CONNECTOR_CONFIG_DEF, props);
    }

    String connectorName() {
        return getString(ConnectorConfig.NAME_CONFIG);
    }

    String heartbeatsTopic() {
        return replicationPolicy().heartbeatsTopic();
    }

    Duration emitHeartbeatsInterval() {
        if (getBoolean(EMIT_HEARTBEATS_ENABLED)) {
            return Duration.ofSeconds(getLong(EMIT_HEARTBEATS_INTERVAL_SECONDS));
        } else {
            // negative interval to disable
            return Duration.ofMillis(-1);
        }
    }

    short heartbeatsTopicReplicationFactor() {
        return getShort(HEARTBEATS_TOPIC_REPLICATION_FACTOR);
    }

    protected static final ConfigDef CONNECTOR_CONFIG_DEF = new ConfigDef(BASE_CONNECTOR_CONFIG_DEF)
            .define(
                    EMIT_HEARTBEATS_ENABLED,
                    ConfigDef.Type.BOOLEAN,
                    EMIT_HEARTBEATS_ENABLED_DEFAULT,
                    ConfigDef.Importance.LOW,
                    EMIT_HEARTBEATS_ENABLED_DOC)
            .define(
                    EMIT_HEARTBEATS_INTERVAL_SECONDS,
                    ConfigDef.Type.LONG,
                    EMIT_HEARTBEATS_INTERVAL_SECONDS_DEFAULT,
                    ConfigDef.Importance.LOW,
                    EMIT_HEARTBEATS_INTERVAL_SECONDS_DOC)
            .define(
                    HEARTBEATS_TOPIC_REPLICATION_FACTOR,
                    ConfigDef.Type.SHORT,
                    HEARTBEATS_TOPIC_REPLICATION_FACTOR_DEFAULT,
                    ConfigDef.Importance.LOW,
                    HEARTBEATS_TOPIC_REPLICATION_FACTOR_DOC);

    public static void main(String[] args) {
        System.out.println(CONNECTOR_CONFIG_DEF.toHtml(4, config -> "mirror_heartbeat_" + config));
    }
}
