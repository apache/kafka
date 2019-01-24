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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
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
import static org.apache.kafka.connect.runtime.WorkerConfig.REST_PORT_CONFIG;
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
    private static final int DEFAULT_NUM_WORKERS = 1;
    private static final Properties DEFAULT_BROKER_CONFIG = new Properties();
    private static final String REST_HOST_NAME = "localhost";

    private final Connect[] connectCluster;
    private final EmbeddedKafkaCluster kafkaCluster;
    private final Map<String, String> workerProps;
    private final String connectClusterName;
    private final int numBrokers;

    private EmbeddedConnectCluster(String name, Map<String, String> workerProps, int numWorkers, int numBrokers, Properties brokerProps) {
        this.workerProps = workerProps;
        this.connectClusterName = name;
        this.numBrokers = numBrokers;
        this.kafkaCluster = new EmbeddedKafkaCluster(numBrokers, brokerProps);
        this.connectCluster = new Connect[numWorkers];
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
        for (Connect worker : this.connectCluster) {
            try {
                worker.stop();
            } catch (Exception e) {
                log.error("Could not stop connect", e);
                throw new RuntimeException("Could not stop worker", e);
            }
        }

        try {
            kafkaCluster.after();
        } catch (Exception e) {
            log.error("Could not stop kafka", e);
            throw new RuntimeException("Could not stop brokers", e);
        }
    }

    @SuppressWarnings("deprecation")
    public void startConnect() {
        log.info("Starting Connect cluster with {} workers. clusterName {}", connectCluster.length, connectClusterName);

        workerProps.put(BOOTSTRAP_SERVERS_CONFIG, kafka().bootstrapServers());
        workerProps.put(REST_HOST_NAME_CONFIG, REST_HOST_NAME);
        workerProps.put(REST_PORT_CONFIG, "0"); // use a random available port

        String internalTopicsReplFactor = String.valueOf(numBrokers);
        putIfAbsent(workerProps, GROUP_ID_CONFIG, "connect-integration-test-" + connectClusterName);
        putIfAbsent(workerProps, OFFSET_STORAGE_TOPIC_CONFIG, "connect-offset-topic-" + connectClusterName);
        putIfAbsent(workerProps, OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, internalTopicsReplFactor);
        putIfAbsent(workerProps, CONFIG_TOPIC_CONFIG, "connect-config-topic-" + connectClusterName);
        putIfAbsent(workerProps, CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, internalTopicsReplFactor);
        putIfAbsent(workerProps, STATUS_STORAGE_TOPIC_CONFIG, "connect-storage-topic-" + connectClusterName);
        putIfAbsent(workerProps, STATUS_STORAGE_REPLICATION_FACTOR_CONFIG, internalTopicsReplFactor);
        putIfAbsent(workerProps, KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        putIfAbsent(workerProps, VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");

        for (int i = 0; i < connectCluster.length; i++) {
            connectCluster[i] = new ConnectDistributed().startConnect(workerProps);
        }
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
        String url = endpointForResource(String.format("connectors/%s/config", connName));
        ObjectMapper mapper = new ObjectMapper();
        int status;
        try {
            String content = mapper.writeValueAsString(connConfig);
            status = executePut(url, content);
        } catch (IOException e) {
            log.error("Could not execute PUT request to " + url, e);
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
        String url = endpointForResource(String.format("connectors/%s", connName));
        int status = executeDelete(url);
        if (status >= HttpServletResponse.SC_BAD_REQUEST) {
            throw new ConnectRestException(status, "Could not execute DELETE request.");
        }
    }

    /**
     * Get the status for a connector running in this cluster.
     *
     * @param connectorName name of the connector
     * @return an instance of {@link ConnectorStateInfo} populated with state informaton of the connector and it's tasks.
     * @throws ConnectRestException if the HTTP request to the REST API failed with a valid status code.
     * @throws ConnectException for any other error.
     */
    public ConnectorStateInfo connectorStatus(String connectorName) {
        ObjectMapper mapper = new ObjectMapper();
        String url = endpointForResource(String.format("connectors/%s/status", connectorName));
        try {
            return mapper.readerFor(ConnectorStateInfo.class).readValue(executeGet(url));
        } catch (IOException e) {
            log.error("Could not read connector state", e);
            throw new ConnectException("Could not read connector state", e);
        }
    }

    private String endpointForResource(String resource) {
        String url = String.valueOf(connectCluster[0].restUrl());
        return url + resource;
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

    /**
     * Execute a GET request on the given URL.
     *
     * @param url the HTTP endpoint
     * @return response body encoded as a String
     * @throws ConnectRestException if the HTTP request fails with a valid status code
     * @throws IOException for any other I/O error.
     */
    public String executeGet(String url) throws IOException {
        log.debug("Executing GET request to URL={}.", url);
        HttpURLConnection httpCon = (HttpURLConnection) new URL(url).openConnection();
        httpCon.setDoOutput(true);
        httpCon.setRequestMethod("GET");
        try (InputStream is = httpCon.getInputStream()) {
            int c;
            StringBuilder response = new StringBuilder();
            while ((c = is.read()) != -1) {
                response.append((char) c);
            }
            log.debug("Get response for URL={} is {}", url, response);
            return response.toString();
        } catch (IOException e) {
            Response.Status status = Response.Status.fromStatusCode(httpCon.getResponseCode());
            if (status != null) {
                throw new ConnectRestException(status, "Invalid endpoint: " + url, e);
            }
            // invalid response code, re-throw the IOException.
            throw e;
        }
    }

    public int executeDelete(String url) throws IOException {
        log.debug("Executing DELETE request to URL={}", url);
        HttpURLConnection httpCon = (HttpURLConnection) new URL(url).openConnection();
        httpCon.setDoOutput(true);
        httpCon.setRequestMethod("DELETE");
        httpCon.connect();
        return httpCon.getResponseCode();
    }

    public static class Builder {
        private String name = UUID.randomUUID().toString();
        private Map<String, String> workerProps = new HashMap<>();
        private int numWorkers = DEFAULT_NUM_WORKERS;
        private int numBrokers = DEFAULT_NUM_BROKERS;
        private Properties brokerProps = DEFAULT_BROKER_CONFIG;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder workerProps(Map<String, String> workerProps) {
            this.workerProps = workerProps;
            return this;
        }

        public Builder numWorkers(int numWorkers) {
            this.numWorkers = numWorkers;
            return this;
        }

        public Builder numBrokers(int numBrokers) {
            this.numBrokers = numBrokers;
            return this;
        }

        public Builder brokerProps(Properties brokerProps) {
            this.brokerProps = brokerProps;
            return this;
        }

        public EmbeddedConnectCluster build() {
            return new EmbeddedConnectCluster(name, workerProps, numWorkers, numBrokers, brokerProps);
        }
    }

}
