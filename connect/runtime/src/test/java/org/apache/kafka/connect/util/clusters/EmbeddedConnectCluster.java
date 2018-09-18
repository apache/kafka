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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.WorkerConfig;
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

import javax.servlet.http.HttpServletResponse;
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

    private static final int DEFAULT_NUM_BROKERS = 1;
    private static final Properties DEFAULT_BROKER_CONFIG = new Properties();
    private static final String REST_HOST_NAME= "localhost";

    private final EmbeddedKafkaCluster kafkaCluster;

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
        this(name, workerProps, DEFAULT_NUM_BROKERS, DEFAULT_BROKER_CONFIG);
    }

    public EmbeddedConnectCluster(String name, Map<String, String> workerProps, int numBrokers, Properties brokerProps) {
        this.workerProps = workerProps;
        this.clusterName = name;
        kafkaCluster = new EmbeddedKafkaCluster(numBrokers, brokerProps);
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
        log.info("Started Connect at {} with Kafka cluster at {}", restUrl(), kafka().bootstrapServers());
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
        log.info("Starting Connect cluster with one worker.");

        workerProps.put(WorkerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka().bootstrapServers());
        workerProps.put(WorkerConfig.REST_HOST_NAME_CONFIG, REST_HOST_NAME);

        putIfAbsent(workerProps, ConsumerConfig.GROUP_ID_CONFIG, "connect-integration-test-" + clusterName);
        putIfAbsent(workerProps, DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, "connect-offset-topic-" + clusterName);
        putIfAbsent(workerProps, DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        putIfAbsent(workerProps, DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-config-topic-" + clusterName);
        putIfAbsent(workerProps, DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        putIfAbsent(workerProps, DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "connect-storage-topic-" + clusterName);
        putIfAbsent(workerProps, DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        putIfAbsent(workerProps, DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        putIfAbsent(workerProps, DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");

        connect = new ConnectDistributed().startConnect(workerProps);
    }

    public void startConnector(String connName, Map<String, String> connConfig) throws IOException {
        String url = String.format("http://%s:%s/connectors/%s/config", REST_HOST_NAME, WorkerConfig.REST_PORT_DEFAULT, connName);
        ObjectMapper mapper = new ObjectMapper();
        try {
            String content = mapper.writeValueAsString(connConfig);
            int status = executePut(url, content);
            if (status >= HttpServletResponse.SC_BAD_REQUEST) {
                throw new IOException("Could not execute PUT request. status=" + status);
            }
        } catch (IOException e) {
            log.error("Could not serialize config", e);
            throw new IOException(e);
        }
    }

    public void deleteConnector(String connName) throws IOException {
        int status = executeDelete(String.format("http://%s:%s/connectors/%s", REST_HOST_NAME, WorkerConfig.REST_PORT_DEFAULT, connName));
        if (status >= HttpServletResponse.SC_BAD_REQUEST) {
            throw new IOException("Could not execute DELETE request. status=" + status);
        }
    }

    private static void putIfAbsent(Map<String, String> props, String propertyKey, String propertyValue) {
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
        try (OutputStreamWriter out = new OutputStreamWriter(httpCon.getOutputStream())) {
            out.write(body);
        }
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
