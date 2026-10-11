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
package org.apache.kafka.server.quota;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.publisher.QuotaConfigChangeListener;
import org.apache.kafka.server.config.AbstractKafkaConfig;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig;

import java.util.Optional;


public class QuotaFactory {

    public static final ReplicaQuota UNBOUNDED_QUOTA = new ReplicaQuota() {
        @Override
        public boolean isThrottled(TopicPartition topicPartition) {
            return false;
        }

        @Override
        public boolean isQuotaExceeded() {
            return false;
        }

        @Override
        public void record(long value) {
            // No-op
        }
    };

    public record QuotaManagers(ClientQuotaManager fetch,
                                ClientQuotaManager produce,
                                ClientRequestQuotaManager request,
                                ControllerMutationQuotaManager controllerMutation,
                                ReplicationQuotaManager leader,
                                ReplicationQuotaManager follower,
                                ReplicationQuotaManager alterLogDirs,
                                Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin) {

        public void shutdown() {
            fetch.shutdown();
            produce.shutdown();
            request.shutdown();
            controllerMutation.shutdown();
            clientQuotaCallbackPlugin.ifPresent(plugin -> Utils.closeQuietly(plugin, "client quota callback plugin"));
        }

        public QuotaConfigChangeListener quotaConfigChangeListener() {
            return () -> {
                fetch.updateQuotaMetricConfigs();
                produce.updateQuotaMetricConfigs();
                request.updateQuotaMetricConfigs();
                controllerMutation.updateQuotaMetricConfigs();
            };
        }
    }

    public static QuotaManagers instantiate(
        AbstractKafkaConfig cfg,
        Metrics metrics,
        Time time,
        String threadNamePrefix,
        String role
    ) {
        Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin = createClientQuotaCallback(cfg, metrics, role);
        var quotaConfig = new QuotaConfig(cfg);

        return new QuotaManagers(
            new ClientQuotaManager(clientConfig(quotaConfig), metrics, QuotaType.FETCH, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ClientQuotaManager(clientConfig(quotaConfig), metrics, QuotaType.PRODUCE, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ClientRequestQuotaManager(clientConfig(quotaConfig), metrics, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ControllerMutationQuotaManager(clientControllerMutationConfig(quotaConfig), metrics, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ReplicationQuotaManager(replicationConfig(quotaConfig), metrics, QuotaType.LEADER_REPLICATION, time),
            new ReplicationQuotaManager(replicationConfig(quotaConfig), metrics, QuotaType.FOLLOWER_REPLICATION, time),
            new ReplicationQuotaManager(alterLogDirsReplicationConfig(quotaConfig), metrics, QuotaType.ALTER_LOG_DIRS_REPLICATION, time),
            clientQuotaCallbackPlugin
        );
    }

    private static Optional<Plugin<ClientQuotaCallback>> createClientQuotaCallback(
        AbstractKafkaConfig cfg,
        Metrics metrics,
        String role
    ) {
        ClientQuotaCallback clientQuotaCallback = cfg.getConfiguredInstance(
            QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG, ClientQuotaCallback.class);
        return clientQuotaCallback == null ? Optional.empty() : Optional.of(Plugin.wrapInstance(
            clientQuotaCallback,
            metrics,
            QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG,
            "role", role
        ));
    }

    private static ClientQuotaManagerConfig clientConfig(QuotaConfig quotaConfig) {
        return new ClientQuotaManagerConfig(
            quotaConfig.numQuotaSamples(),
            quotaConfig.quotaWindowSizeSeconds()
        );
    }

    private static ClientQuotaManagerConfig clientControllerMutationConfig(QuotaConfig quotaConfig) {
        return new ClientQuotaManagerConfig(
            quotaConfig.numControllerQuotaSamples(),
            quotaConfig.controllerQuotaWindowSizeSeconds()
        );
    }

    private static ReplicationQuotaManagerConfig replicationConfig(QuotaConfig quotaConfig) {
        return new ReplicationQuotaManagerConfig(
            quotaConfig.numReplicationQuotaSamples(),
            quotaConfig.replicationQuotaWindowSizeSeconds()
        );
    }

    private static ReplicationQuotaManagerConfig alterLogDirsReplicationConfig(QuotaConfig quotaConfig) {
        return new ReplicationQuotaManagerConfig(
            quotaConfig.numAlterLogDirsReplicationQuotaSamples(),
            quotaConfig.alterLogDirsReplicationQuotaWindowSizeSeconds()
        );
    }
}
