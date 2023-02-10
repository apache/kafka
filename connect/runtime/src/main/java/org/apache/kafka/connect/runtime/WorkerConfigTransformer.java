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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigTransformerResult;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.Herder.ConfigReloadAction;
import org.apache.kafka.connect.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A wrapper class to perform configuration transformations and schedule reloads for any
 * retrieved TTL values.
 */
public class WorkerConfigTransformer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(WorkerConfigTransformer.class);

    private final Worker worker;
    private final ConfigTransformer configTransformer;
    private final ConcurrentMap<String, Map<String, HerderRequest>> requests = new ConcurrentHashMap<>();
    private final Map<String, ConfigProvider> configProviders;

    public WorkerConfigTransformer(Worker worker, Map<String, ConfigProvider> configProviders) {
        this.worker = worker;
        this.configProviders = configProviders;
        this.configTransformer = new ConfigTransformer(configProviders);
    }

    public Map<String, String> transform(Map<String, String> configs) {
        return transform(null, configs);
    }

    public Map<String, String> transform(String connectorName, Map<String, String> configs) {
        if (configs == null) return null;
        ConfigTransformerResult result = configTransformer.transform(configs);
        if (connectorName != null) {
            String key = ConnectorConfig.CONFIG_RELOAD_ACTION_CONFIG;
            String action = (String) ConfigDef.parseType(key, configs.get(key), ConfigDef.Type.STRING);
            if (action == null) {
                // The default action is "restart".
                action = ConnectorConfig.CONFIG_RELOAD_ACTION_RESTART;
            }
            ConfigReloadAction reloadAction = ConfigReloadAction.valueOf(action.toUpperCase(Locale.ROOT));
            if (reloadAction == ConfigReloadAction.RESTART) {
                scheduleReload(connectorName, result.ttls());
            }
        }
        return result.data();
    }

    private void scheduleReload(String connectorName, Map<String, Long> ttls) {
        for (Map.Entry<String, Long> entry : ttls.entrySet()) {
            scheduleReload(connectorName, entry.getKey(), entry.getValue());
        }
    }

    private void scheduleReload(String connectorName, String path, long ttl) {
        Map<String, HerderRequest> connectorRequests = requests.computeIfAbsent(connectorName, s -> new ConcurrentHashMap<>());
        connectorRequests.compute(path, (s, previousRequest) -> {
            if (previousRequest != null) {
                // Delete previous request for ttl which is now stale
                previousRequest.cancel();
            }
            log.info("Scheduling a restart of connector {} in {} ms", connectorName, ttl);
            Callback<Void> cb = (error, result) -> {
                if (error != null) {
                    log.error("Unexpected error during connector restart: ", error);
                }
            };
            return worker.herder().restartConnector(ttl, connectorName, cb);
        });
    }

    @Override
    public void close() {
        configProviders.values().forEach(x -> Utils.closeQuietly(x, "config provider"));
    }
}
