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

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.ConfigTransformer;
import org.apache.kafka.common.config.ConfigTransformerResult;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A wrapper class to perform configuration transformations and schedule reloads for any
 * retrieved TTL values.
 */
public class WorkerConfigTransformer {
    private final Worker worker;
    private final ConfigTransformer configTransformer;
    private final ConcurrentMap<String, Map<String, HerderRequest>> requests = new ConcurrentHashMap<>();

    public WorkerConfigTransformer(Worker worker, Map<String, ConfigProvider> configProviders) {
        this.worker = worker;
        this.configTransformer = new ConfigTransformer(configProviders);
    }

    public Map<String, String> transform(String connectorName, Map<String, String> configs) {
        if (configs == null) return null;
        ConfigTransformerResult result = configTransformer.transform(configs);
        scheduleReload(connectorName, result.ttls());
        return result.data();
    }

    private void scheduleReload(String connectorName, Map<String, Long> ttls) {
        for (Map.Entry<String, Long> entry : ttls.entrySet()) {
            scheduleReload(connectorName, entry.getKey(), entry.getValue());
        }
    }

    private void scheduleReload(String connectorName, String path, long ttl) {
        Herder herder = worker.herder();
        if (herder.connectorConfigReloadAction(connectorName) == Herder.ConfigReloadAction.RESTART) {
            Map<String, HerderRequest> connectorRequests = requests.get(connectorName);
            if (connectorRequests == null) {
                connectorRequests = new ConcurrentHashMap<>();
                requests.put(connectorName, connectorRequests);
            } else {
                HerderRequest previousRequest = connectorRequests.get(path);
                if (previousRequest != null) {
                    // Delete previous request for ttl which is now stale
                    previousRequest.cancel();
                }
            }
            HerderRequest request = herder.restartConnector(ttl, connectorName, null);
            connectorRequests.put(path, request);
        }
    }
}
