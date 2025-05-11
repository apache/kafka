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
package kafka.server;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ClientQuotaManagerConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ReplicationQuotaManagerConfig;
import org.apache.kafka.server.quota.ClientQuotaCallback;
import org.apache.kafka.server.quota.QuotaType;

import java.util.Optional;

import scala.Option;
import scala.jdk.javaapi.OptionConverters;

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

    public static class QuotaManagers {
        private final ClientQuotaManager fetch;
        private final ClientQuotaManager produce;
        private final ClientRequestQuotaManager request;
        private final ControllerMutationQuotaManager controllerMutation;
        private final ReplicationQuotaManager leader;
        private final ReplicationQuotaManager follower;
        private final ReplicationQuotaManager alterLogDirs;
        private final Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin;

        public QuotaManagers(ClientQuotaManager fetch, ClientQuotaManager produce, ClientRequestQuotaManager request,
                             ControllerMutationQuotaManager controllerMutation, ReplicationQuotaManager leader,
                             ReplicationQuotaManager follower, ReplicationQuotaManager alterLogDirs,
                             Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin) {
            this.fetch = fetch;
            this.produce = produce;
            this.request = request;
            this.controllerMutation = controllerMutation;
            this.leader = leader;
            this.follower = follower;
            this.alterLogDirs = alterLogDirs;
            this.clientQuotaCallbackPlugin = clientQuotaCallbackPlugin;
        }

        public ClientQuotaManager fetch() {
            return fetch;
        }

        public ClientQuotaManager produce() {
            return produce;
        }

        public ClientRequestQuotaManager request() {
            return request;
        }

        public ControllerMutationQuotaManager controllerMutation() {
            return controllerMutation;
        }

        public ReplicationQuotaManager leader() {
            return leader;
        }

        public ReplicationQuotaManager follower() {
            return follower;
        }

        public ReplicationQuotaManager alterLogDirs() {
            return alterLogDirs;
        }

        public Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin() {
            return clientQuotaCallbackPlugin;
        }

        public void shutdown() {
            fetch.shutdown();
            produce.shutdown();
            request.shutdown();
            controllerMutation.shutdown();
            clientQuotaCallbackPlugin.ifPresent(plugin -> Utils.closeQuietly(plugin, "client quota callback plugin"));
        }
    }

    public static QuotaManagers instantiate(
        KafkaConfig cfg,
        Metrics metrics,
        Time time,
        String threadNamePrefix,
        String role
    ) {
        Optional<Plugin<ClientQuotaCallback>> clientQuotaCallbackPlugin = createClientQuotaCallback(cfg, metrics, role);
        Option<Plugin<ClientQuotaCallback>> clientQuotaCallbackPluginOption = OptionConverters.toScala(clientQuotaCallbackPlugin);

        return new QuotaManagers(
            new ClientQuotaManager(clientConfig(cfg), metrics, QuotaType.FETCH, time, threadNamePrefix, clientQuotaCallbackPluginOption),
            new ClientQuotaManager(clientConfig(cfg), metrics, QuotaType.PRODUCE, time, threadNamePrefix, clientQuotaCallbackPluginOption),
            new ClientRequestQuotaManager(clientConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallbackPlugin),
            new ControllerMutationQuotaManager(clientControllerMutationConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallbackPluginOption),
            new ReplicationQuotaManager(replicationConfig(cfg), metrics, QuotaType.LEADER_REPLICATION, time),
            new ReplicationQuotaManager(replicationConfig(cfg), metrics, QuotaType.FOLLOWER_REPLICATION, time),
            new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, QuotaType.ALTER_LOG_DIRS_REPLICATION, time),
            clientQuotaCallbackPlugin
        );
    }

    private static Optional<Plugin<ClientQuotaCallback>> createClientQuotaCallback(
        KafkaConfig cfg, 
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

    private static ClientQuotaManagerConfig clientConfig(KafkaConfig cfg) {
        return new ClientQuotaManagerConfig(
            cfg.quotaConfig().numQuotaSamples(),
            cfg.quotaConfig().quotaWindowSizeSeconds()
        );
    }

    private static ClientQuotaManagerConfig clientControllerMutationConfig(KafkaConfig cfg) {
        return new ClientQuotaManagerConfig(
            cfg.quotaConfig().numControllerQuotaSamples(),
            cfg.quotaConfig().controllerQuotaWindowSizeSeconds()
        );
    }

    private static ReplicationQuotaManagerConfig replicationConfig(KafkaConfig cfg) {
        return new ReplicationQuotaManagerConfig(
            cfg.quotaConfig().numReplicationQuotaSamples(),
            cfg.quotaConfig().replicationQuotaWindowSizeSeconds()
        );
    }

    private static ReplicationQuotaManagerConfig alterLogDirsReplicationConfig(KafkaConfig cfg) {
        return new ReplicationQuotaManagerConfig(
            cfg.quotaConfig().numAlterLogDirsReplicationQuotaSamples(),
            cfg.quotaConfig().alterLogDirsReplicationQuotaWindowSizeSeconds()
        );
    }
}
