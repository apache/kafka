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
import org.apache.kafka.connect.cli.ConnectDistributed;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.connect.runtime.WorkerConfig.REST_HOST_NAME_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.CONFIG_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG;

/**
 * Start an embedded connect worker. Internally, this class will spin up a Kafka and Zk cluster, setup any tmp
 * directories and clean up them on them.
 */
public class EmbeddedConnectCluster {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnectCluster.class);

    private static final int DEFAULT_NUM_BROKERS = 1;
    private static final Properties DEFAULT_BROKER_CONFIG = new Properties();
    private static final String REST_HOST_NAME = "localhost";

    private final EmbeddedKafkaCluster kafkaCluster;

    private final Map<String, String> workerProps;
    private final String clusterName;

    private Connect connect;

    public EmbeddedConnectCluster() {
        this(UUID.randomUUID().toString());
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

    /**
     * Start the connect cluster and the embedded Kafka and Zookeeper cluster.
     */
    public void start() throws IOException {
        kafkaCluster.before();
        startConnect();
    }

    /**
     * Stop the connect cluster and the embedded Kafka and Zookeeper cluster.
     * Clean up any temp directories created locally.
     */
    public void stop() {
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
        log.info("Starting Connect cluster with one worker. clusterName=" + clusterName);

        workerProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka().bootstrapServers());
        workerProps.put(REST_HOST_NAME_CONFIG, REST_HOST_NAME);

        putIfAbsent(workerProps, GROUP_ID_CONFIG, "connect-integration-test-" + clusterName);
        putIfAbsent(workerProps, OFFSET_STORAGE_TOPIC_CONFIG, "connect-offset-topic-" + clusterName);
        putIfAbsent(workerProps, OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        putIfAbsent(workerProps, CONFIG_TOPIC_CONFIG, "connect-config-topic-" + clusterName);
        putIfAbsent(workerProps, CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        putIfAbsent(workerProps, STATUS_STORAGE_TOPIC_CONFIG, "connect-storage-topic-" + clusterName);
        putIfAbsent(workerProps, STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, "1");
        putIfAbsent(workerProps, KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        putIfAbsent(workerProps, VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");

        connect = new ConnectDistributed().startConnect(workerProps);
    }

    /**
     * Configure a connector. If the connector does not already exist, a new one will be created and
     * the given configuration will be applied to it.
     *
     * @param connName   the name of the connector
     * @param connConfig the intended configuration
     * @throws IOException          if call to the REST api fails.
     * @throws ConnectRestException if REST api returns error status
     */
    public void configureConnector(String connName, Map<String, String> connConfig) throws IOException {
        String url = String.format("http://%s:%s/connectors/%s/config", REST_HOST_NAME, WorkerConfig.REST_PORT_DEFAULT, connName);
        ObjectMapper mapper = new ObjectMapper();
        int status;
        try {
            String content = mapper.writeValueAsString(connConfig);
            status = executePut(url, content);
        } catch (IOException e) {
            log.error("Could not serialize config", e);
            throw e;
        }
        if (status >= HttpServletResponse.SC_BAD_REQUEST) {
            throw new ConnectRestException(status, "Could not execute PUT request");
        }
    }

    /**
     * Delete an existing connector.
     *
     * @param connName name of the connector to be deleted
     * @throws IOException if call to the REST api fails.
     */
    public void deleteConnector(String connName) throws IOException {
        int status = executeDelete(String.format("http://%s:%s/connectors/%s", REST_HOST_NAME, WorkerConfig.REST_PORT_DEFAULT, connName));
        if (status >= HttpServletResponse.SC_BAD_REQUEST) {
            throw new ConnectRestException(status, "Could not execute DELETE request.");
        }
    }

    private static void putIfAbsent(Map<String, String> props, String propertyKey, String propertyValue) {
        if (!props.containsKey(propertyKey)) {
            props.put(propertyKey, propertyValue);
        }
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
