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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.List;
import java.util.Collections;

import static org.apache.kafka.connect.mirror.Heartbeat.SOURCE_CLUSTER_ALIAS_KEY;
import static org.apache.kafka.connect.mirror.Heartbeat.TARGET_CLUSTER_ALIAS_KEY;

/** Emits heartbeats to Kafka.
 *
 *  @see MirrorHeartbeatConfig for supported config properties.
 */
public class MirrorHeartbeatConnector extends SourceConnector {
    private MirrorHeartbeatConfig config;
    private Scheduler scheduler;
    private Admin targetAdminClient;

    public MirrorHeartbeatConnector() {
        // nop
    }

    // visible for testing
    MirrorHeartbeatConnector(MirrorHeartbeatConfig config) {
        this.config = config;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new MirrorHeartbeatConfig(props);
        targetAdminClient = config.forwardingAdmin(config.targetAdminConfig("heartbeats-target-admin"));
        scheduler = new Scheduler(getClass(), config.entityLabel(), config.adminTimeout());
        scheduler.execute(this::createInternalTopics, "creating internal topics");
    }

    @Override
    public void stop() {
        Utils.closeQuietly(scheduler, "scheduler");
        Utils.closeQuietly(targetAdminClient, "target admin client");
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MirrorHeartbeatTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // if the heartbeats emission is disabled by setting `emit.heartbeats.enabled` to `false`,
        // the interval heartbeat emission will be negative and no `MirrorHeartbeatTask` will be created
        if (config.emitHeartbeatsInterval().isNegative()) {
            return Collections.emptyList();
        }
        // just need a single task
        return Collections.singletonList(config.originalsStrings());
    }

    @Override
    public ConfigDef config() {
        return MirrorHeartbeatConfig.CONNECTOR_CONFIG_DEF;
    }

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public boolean alterOffsets(Map<String, String> config, Map<Map<String, ?>, Map<String, ?>> offsets) {
        for (Map.Entry<Map<String, ?>, Map<String, ?>> offsetEntry : offsets.entrySet()) {
            Map<String, ?> sourceOffset = offsetEntry.getValue();
            if (sourceOffset == null) {
                // We allow tombstones for anything; if there's garbage in the offsets for the connector, we don't
                // want to prevent users from being able to clean it up using the REST API
                continue;
            }

            Map<String, ?> sourcePartition = offsetEntry.getKey();
            if (sourcePartition == null) {
                throw new ConnectException("Source partitions may not be null");
            }

            MirrorUtils.validateSourcePartitionString(sourcePartition, SOURCE_CLUSTER_ALIAS_KEY);
            MirrorUtils.validateSourcePartitionString(sourcePartition, TARGET_CLUSTER_ALIAS_KEY);

            MirrorUtils.validateSourceOffset(sourcePartition, sourceOffset, true);
        }

        // We don't actually use these offsets in the task class, so no additional effort is required beyond just validating
        // the format of the user-supplied offsets
        return true;
    }

    private void createInternalTopics() {
        MirrorUtils.createSinglePartitionCompactedTopic(
                config.heartbeatsTopic(),
                config.heartbeatsTopicReplicationFactor(),
                targetAdminClient
        );
    }
}
