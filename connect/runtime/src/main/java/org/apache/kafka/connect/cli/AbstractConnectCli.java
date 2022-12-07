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

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;

/**
 * Common initialization logic for Kafka Connect, intended for use by command line utilities
 *
 * @param <T> the type of {@link WorkerConfig} to be used
 */
public abstract class AbstractConnectCli<T extends WorkerConfig> {

    private static final Logger log = LoggerFactory.getLogger(AbstractConnectCli.class);
    private final Time time = Time.SYSTEM;

    protected abstract Herder createHerder(T config, String workerId, Plugins plugins,
                                           ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                           RestServer restServer, RestClient restClient);

    protected abstract T createConfig(Map<String, String> workerProps);

    /**
     * @param workerProps the worker properties map
     * @param connectorPropsFiles zero or more connector property files for connectors that are to be created after
     *                            Connect is successfully started
     * @return a started instance of {@link Connect}
     */
    public Connect startConnect(Map<String, String> workerProps, String... connectorPropsFiles) {
        log.info("Kafka Connect worker initializing ...");
        long initStart = time.hiResClockMs();

        WorkerInfo initInfo = new WorkerInfo();
        initInfo.logAll();

        log.info("Scanning for plugin classes. This might take a moment ...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();
        T config = createConfig(workerProps);
        log.debug("Kafka cluster ID: {}", config.kafkaClusterId());

        RestClient restClient = new RestClient(config);

        RestServer restServer = new RestServer(config, restClient);
        restServer.initializeServer();

        URI advertisedUrl = restServer.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy = plugins.newPlugin(
                config.getString(WorkerConfig.CONNECTOR_CLIENT_POLICY_CLASS_CONFIG),
                config, ConnectorClientConfigOverridePolicy.class);

        Herder herder = createHerder(config, workerId, plugins, connectorClientConfigOverridePolicy, restServer, restClient);

        final Connect connect = new Connect(herder, restServer);
        log.info("Kafka Connect worker initialization took {}ms", time.hiResClockMs() - initStart);
        try {
            connect.start();
        } catch (Exception e) {
            log.error("Failed to start Connect", e);
            connect.stop();
            Exit.exit(3);
        }

        try {
            for (final String connectorPropsFile : connectorPropsFiles) {
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
        } catch (Throwable t) {
            log.error("Stopping after connector error", t);
            connect.stop();
            Exit.exit(3);
        }

        return connect;
    }
}
