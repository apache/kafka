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
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerInfo;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * <p>
 * Command line utility that runs Kafka Connect as a standalone process. In this mode, work is not
 * distributed. Instead, all the normal Connect machinery works within a single process. This is
 * useful for ad hoc, small, or experimental jobs.
 * </p>
 * <p>
 * By default, no job configs or offset data is persistent. You can make jobs persistent and
 * fault tolerant by overriding the settings to use file storage for both.
 * </p>
 */
public class ConnectStandalone {
    private static final Logger log = LoggerFactory.getLogger(ConnectStandalone.class);

    public static void main(String[] args) {

        if (args.length < 2 || Arrays.asList(args).contains("--help")) {
            log.info("Usage: ConnectStandalone worker.properties connector1.properties [connector2.properties ...]");
            Exit.exit(1);
        }

        try {
            Time time = Time.SYSTEM;
            log.info("Kafka Connect standalone worker initializing ...");
            long initStart = time.hiResClockMs();
            WorkerInfo initInfo = new WorkerInfo();
            initInfo.logAll();

            String workerPropsFile = args[0];
            Map<String, String> workerProps = !workerPropsFile.isEmpty() ?
                    Utils.propsToStringMap(Utils.loadProps(workerPropsFile)) : Collections.<String, String>emptyMap();

            log.info("Scanning for plugin classes. This might take a moment ...");
            Plugins plugins = new Plugins(workerProps);
            plugins.compareAndSwapWithDelegatingLoader();
            StandaloneConfig config = new StandaloneConfig(workerProps);

            String kafkaClusterId = ConnectUtils.lookupKafkaClusterId(config);
            log.debug("Kafka cluster ID: {}", kafkaClusterId);

            RestServer rest = new RestServer(config);
            rest.initializeServer();

            URI advertisedUrl = rest.advertisedUrl();
            String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

            Worker worker = new Worker(workerId, time, plugins, config, new FileOffsetBackingStore());

            Herder herder = new StandaloneHerder(worker, kafkaClusterId);
            final Connect connect = new Connect(herder, rest);
            log.info("Kafka Connect standalone worker initialization took {}ms", time.hiResClockMs() - initStart);

            try {
                connect.start();
                for (final String connectorPropsFile : Arrays.copyOfRange(args, 1, args.length)) {
                    Map<String, String> connectorProps = Utils.propsToStringMap(Utils.loadProps(connectorPropsFile));
                    FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>(new Callback<Herder.Created<ConnectorInfo>>() {
                        @Override
                        public void onCompletion(Throwable error, Herder.Created<ConnectorInfo> info) {
                            if (error != null)
                                log.error("Failed to create job for {}", connectorPropsFile);
                            else
                                log.info("Created connector {}", info.result().name());
                        }
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

            // Shutdown will be triggered by Ctrl-C or via HTTP shutdown request
            connect.awaitStop();

        } catch (Throwable t) {
            log.error("Stopping due to error", t);
            Exit.exit(2);
        }
    }
}
