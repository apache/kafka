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
package org.apache.kafka.connect.util.clusters;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.FutureCallback;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EmbeddedConnectCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectCluster.class);

    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG = new Properties();

    private final EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    private final Map<String, String> workerProps;

    private File offsetsDirectory;
    private Connect connect;
    private URI advertisedUrl;
    private Herder herder;

    public EmbeddedConnectCluster() {
        // this empty map will be populated with defaults before starting Connect.
        this(new HashMap<>());
    }

    public EmbeddedConnectCluster(Map<String, String> workerProps) {
        this.workerProps = workerProps;
    }

    @Override
    protected void before() throws IOException {
        kafkaCluster.before();
        start();
        log.info("Started connect at {} with kafka cluster at {}", restUrl(), kafka().bootstrapServers());
    }

    @Override
    protected void after() {
        try {
            log.info("Cleaning up connect offset dir at {}", offsetsDirectory);
            Utils.delete(offsetsDirectory);
        } catch (IOException e) {
            log.error("Could not delete directory at {}", offsetsDirectory, e);
        }

        connect.stop();
        kafkaCluster.after();
    }

    public void start() throws IOException {
        log.info("Starting standalone connect cluster..");
        workerProps.put("bootstrap.servers", kafka().bootstrapServers());
        workerProps.put("rest.host.name", "localhost");

        putIfAbsent(workerProps, "key.converter", "org.apache.kafka.connect.json.JsonConverter");
        putIfAbsent(workerProps, "value.converter", "org.apache.kafka.connect.json.JsonConverter");
        putIfAbsent(workerProps, "key.converter.schemas.enable", "false");
        putIfAbsent(workerProps, "value.converter.schemas.enable", "false");

        offsetsDirectory = createTmpDir();
        putIfAbsent(workerProps, "offset.storage.file.filename", new File(offsetsDirectory, "connect.integration.offsets").getAbsolutePath());

        log.info("Scanning for plugins...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();

        StandaloneConfig config = new StandaloneConfig(workerProps);

        RestServer rest = new RestServer(config);
        advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        Worker worker = new Worker(workerId, kafkaCluster.time(), plugins, config, new FileOffsetBackingStore());

        herder = new StandaloneHerder(worker, ConnectUtils.lookupKafkaClusterId(config));
        connect = new Connect(herder, rest);
        connect.start();
    }

    public void startConnector(String connName, Map<String, String> connConfig) {
        connConfig.put("name", connName);
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.putConnectorConfig(connName, connConfig, true, cb);
        try {
            cb.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void deleteConnector(String connName) {
        FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>();
        herder.deleteConnectorConfig(connName, cb);
        try {
            cb.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void putIfAbsent(Map<String, String> props, String propertyKey, String propertyValue) {
        if (!props.containsKey(propertyKey)) {
            props.put(propertyKey, propertyValue);
        }
    }

    private File createTmpDir() throws IOException {
        TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        return tmpFolder.newFolder();
    }

    public URI restUrl() {
        return advertisedUrl;
    }

    public EmbeddedKafkaCluster kafka() {
        return kafkaCluster;
    }
}