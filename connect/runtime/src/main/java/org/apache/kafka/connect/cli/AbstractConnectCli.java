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
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.ConnectRestServer;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Common initialization logic for Kafka Connect, intended for use by command line utilities
 *
 * @param <T> the type of {@link WorkerConfig} to be used
 */
public abstract class AbstractConnectCli<T extends WorkerConfig> {

    private static final Logger log = LoggerFactory.getLogger(AbstractConnectCli.class);
    private final String[] args;
    private final Time time = Time.SYSTEM;

    /**
     *
     * @param args the CLI arguments to be processed. Note that if one or more arguments are passed, the first argument is
     *             assumed to be the Connect worker properties file and is processed in {@link #run()}. The remaining arguments
     *             can be handled in {@link #processExtraArgs(Herder, Connect, String[])}
     */
    protected AbstractConnectCli(String... args) {
        this.args = args;
    }

    protected abstract String usage();

    /**
     * The first CLI argument is assumed to be the Connect worker properties file and is processed by default. This method
     * can be overridden if there are more arguments that need to be processed.
     *
     * @param herder the {@link Herder} instance that can be used to perform operations on the Connect cluster
     * @param connect the {@link Connect} instance that can be stopped (via {@link Connect#stop()}) if there's an error
     *                encountered while processing the additional CLI arguments.
     * @param extraArgs the extra CLI arguments that need to be processed
     */
    protected void processExtraArgs(Herder herder, Connect connect, String[] extraArgs) {
    }

    protected abstract Herder createHerder(T config, String workerId, Plugins plugins,
                                           ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                           RestServer restServer, RestClient restClient);

    protected abstract T createConfig(Map<String, String> workerProps);

    /**
     *  Validate {@link #args}, process worker properties from the first CLI argument, and start {@link Connect}
     */
    public void run() {
        if (args.length < 1 || Arrays.asList(args).contains("--help")) {
            log.info("Usage: {}", usage());
            Exit.exit(1);
        }

        try {
            String workerPropsFile = args[0];
            Map<String, String> workerProps = !workerPropsFile.isEmpty() ?
                    Utils.propsToStringMap(Utils.loadProps(workerPropsFile)) : Collections.emptyMap();
            String[] extraArgs = Arrays.copyOfRange(args, 1, args.length);
            Connect connect = startConnect(workerProps, extraArgs);

            // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
            connect.awaitStop();

        } catch (Throwable t) {
            log.error("Stopping due to error", t);
            Exit.exit(2);
        }
    }

    /**
     * Initialize and start an instance of {@link Connect}
     *
     * @param workerProps the worker properties map used to initialize the {@link WorkerConfig}
     * @param extraArgs any additional CLI arguments that may need to be processed via
     *                  {@link #processExtraArgs(Herder, Connect, String[])}
     * @return a started instance of {@link Connect}
     */
    public Connect startConnect(Map<String, String> workerProps, String... extraArgs) {
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

        ConnectRestServer restServer = new ConnectRestServer(config.rebalanceTimeout(), restClient, config.originals());
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

        processExtraArgs(herder, connect, extraArgs);

        return connect;
    }
}
