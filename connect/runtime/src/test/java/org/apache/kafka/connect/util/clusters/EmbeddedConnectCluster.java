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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfigTransformer;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedHerder;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.storage.ConfigBackingStore;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.KafkaConfigBackingStore;
import org.apache.kafka.connect.storage.KafkaOffsetBackingStore;
import org.apache.kafka.connect.storage.KafkaStatusBackingStore;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectUtils;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class EmbeddedConnectCluster extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectCluster.class);

    private static final int NUM_BROKERS = 1;
    private static final Properties BROKER_CONFIG = new Properties();

    private final EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(NUM_BROKERS, BROKER_CONFIG);

    private final Map<String, String> workerProps;
    private final String clusterName;

    private Connect connect;
    private URI advertisedUrl;

    public EmbeddedConnectCluster(Class<?> klass) {
        this(klass.getName());
    }

    public EmbeddedConnectCluster(String name) {
        // this empty map will be populated with defaults before starting Connect.
        this(name, new HashMap<>());
    }

    public EmbeddedConnectCluster(String name, Map<String, String> workerProps) {
        this.workerProps = workerProps;
        this.clusterName = name;
    }

    public void start() throws IOException {
        before();
    }

    /**
     * Stop the connect cluster and the embedded Kafka and
     */
    public void stop() {
        after();
    }

    @Override
    protected void before() throws IOException {
        kafkaCluster.before();
        startConnect();
        log.info("Started connect at {} with kafka cluster at {}", restUrl(), kafka().bootstrapServers());
    }

    @Override
    protected void after() {
        try {
            connect.stop();
        } catch (Exception e) {
            log.error("Could not stop connect", e);
        }

        try {
            kafkaCluster.after();
        } catch (Exception e) {
            log.error("Could not stop kafka", e);
        }
    }

    public void startConnect() {
        log.info("Starting standalone connect cluster..");
        workerProps.put("bootstrap.servers", kafka().bootstrapServers());
        workerProps.put("rest.host.name", "localhost");

        putIfAbsent(workerProps, "group.id", "connect-integration-test-" + clusterName);
        putIfAbsent(workerProps, "offset.storage.topic", "connect-offset-topic-" + clusterName);
        putIfAbsent(workerProps, "offset.storage.replication.factor", "1");
        putIfAbsent(workerProps, "config.storage.topic", "connect-config-topic-" + clusterName);
        putIfAbsent(workerProps, "config.storage.replication.factor", "1");
        putIfAbsent(workerProps, "status.storage.topic", "connect-storage-topic-" + clusterName);
        putIfAbsent(workerProps, "status.storage.replication.factor", "1");
        putIfAbsent(workerProps, "key.converter", "org.apache.kafka.connect.json.JsonConverter");
        putIfAbsent(workerProps, "value.converter", "org.apache.kafka.connect.json.JsonConverter");
        putIfAbsent(workerProps, "key.converter.schemas.enable", "false");
        putIfAbsent(workerProps, "value.converter.schemas.enable", "false");

        log.info("Scanning for plugins...");
        Plugins plugins = new Plugins(workerProps);
        plugins.compareAndSwapWithDelegatingLoader();

        DistributedConfig config = new DistributedConfig(workerProps);

        RestServer rest = new RestServer(config);
        advertisedUrl = rest.advertisedUrl();
        String workerId = advertisedUrl.getHost() + ":" + advertisedUrl.getPort();

        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore();
        offsetBackingStore.configure(config);

        Worker worker = new Worker(workerId, kafkaCluster.time(), plugins, config, offsetBackingStore);

        WorkerConfigTransformer configTransformer = worker.configTransformer();
        Converter internalValueConverter = worker.getInternalValueConverter();

        StatusBackingStore statusBackingStore = new KafkaStatusBackingStore(kafkaCluster.time(), internalValueConverter);
        statusBackingStore.configure(config);

        ConfigBackingStore configBackingStore = new KafkaConfigBackingStore(
                internalValueConverter,
                config,
                configTransformer);

        Herder herder = new DistributedHerder(config, kafkaCluster.time(), worker,
                ConnectUtils.lookupKafkaClusterId(config), statusBackingStore, configBackingStore,
                advertisedUrl.toString());
        connect = new Connect(herder, rest);
        connect.start();
    }

    public void startConnector(String connName, Map<String, String> connConfig) throws IOException {
        String url = String.format("http://localhost:8083/connectors/%s/config", connName);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String content = mapper.writeValueAsString(connConfig);
            int status = executePut(url , content);
            if (status >= 400) {
                throw new IOException("Could not execute PUT request. status=" + status);
            }
        } catch (JsonProcessingException e) {
            log.error("Could not serialize config", e);
            throw new RuntimeException(e);
        }
    }

    public void deleteConnector(String connName) throws IOException {
        int status = executeDelete(String.format("http://localhost:8083/connectors/%s", connName));
        if (status >= 400) {
            throw new IOException("Could not execute DELETE request. status=" + status);
        }
    }

    private void putIfAbsent(Map<String, String> props, String propertyKey, String propertyValue) {
        if (!props.containsKey(propertyKey)) {
            props.put(propertyKey, propertyValue);
        }
    }

    public URI restUrl() {
        return advertisedUrl;
    }

    public EmbeddedKafkaCluster kafka() {
        return kafkaCluster;
    }

    public int executePut(String url, String body) throws IOException {
        log.debug("Executing PUT request to URL={}. Payload={}", url, body);
        HttpURLConnection httpCon = (HttpURLConnection) new URL(url).openConnection();
        httpCon.setDoOutput(true);
        httpCon.setRequestProperty("Content-Type", "application/json");
        httpCon.setRequestMethod("PUT");
        OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream());
        out.write(body);
        out.close();
        try (InputStream is = httpCon.getInputStream()) {
            int c;
            StringBuilder response = new StringBuilder();
            while ((c = is.read()) != -1) {
                response.append((char) c);
            }
            log.info("Put response for URL={} is {}", url, response);
        }
        return httpCon.getResponseCode();
    }

    public int executeDelete(String url) throws IOException {
        log.debug("Executing DELETE request to URL={}", url);
        HttpURLConnection httpCon = (HttpURLConnection) new URL(url).openConnection();
        httpCon.setDoOutput(true);
        httpCon.setRequestMethod("DELETE");
        httpCon.connect();
        return httpCon.getResponseCode();
    }

}