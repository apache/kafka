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
package org.apache.kafka.connect.cli;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <p>
 * Command line utility that runs Kafka Connect as a standalone process. In this mode, work (connectors and tasks) is not
 * distributed. Instead, all the normal Connect machinery works within a single process. This is useful for for development
 * and testing Kafka Connect on a local machine.
 * </p>
 */
public class ConnectStandalone extends AbstractConnectCli<StandaloneConfig> {
    private static final Logger log = LoggerFactory.getLogger(ConnectStandalone.class);

    protected ConnectStandalone(String... args) {
        super(args);
    }

    @Override
    protected String usage() {
        return "ConnectStandalone worker.properties [connector1.properties connector2.properties ...]";
    }

    @Override
    protected void processExtraArgs(Herder herder, String[] extraArgs) throws Throwable {
        for (final String connectorPropsFile : extraArgs) {
            Map<String, String> connectorProps = Utils.propsToStringMap(Utils.loadProps(connectorPropsFile));
            FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
                if (error != null)
                    log.error("Failed to create connector for {}", connectorPropsFile);
                else
                    log.info("Created connector {}", info.result().name());
            });
            herder.putConnectorConfig(
                    connectorProps.get(ConnectorConfig.NAME_CONFIG),
                    connectorProps, false, cb);
            cb.get();
        }
    }

    @Override
    protected Herder createHerder(StandaloneConfig config, String workerId, Plugins plugins,
                                  ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                  RestServer restServer, RestClient restClient) {

        OffsetBackingStore offsetBackingStore = new FileOffsetBackingStore();
        offsetBackingStore.configure(config);

        Worker worker = new Worker(workerId, Time.SYSTEM, plugins, config, offsetBackingStore,
                connectorClientConfigOverridePolicy);

        return new StandaloneHerder(worker, config.kafkaClusterId(), connectorClientConfigOverridePolicy);
    }

    @Override
    protected StandaloneConfig createConfig(Map<String, String> workerProps) {
        return new StandaloneConfig(workerProps);
    }

    public static void main(String[] args) {
        ConnectStandalone connectStandalone = new ConnectStandalone(args);
        connectStandalone.run();
    }
}
