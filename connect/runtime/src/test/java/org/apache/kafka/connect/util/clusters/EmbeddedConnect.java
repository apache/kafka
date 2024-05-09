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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ActiveTopicsInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConfigInfos;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffset;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorOffsets;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorStateInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.rest.entities.LoggerLevel;
import org.apache.kafka.connect.runtime.rest.entities.TaskInfo;
import org.apache.kafka.connect.runtime.rest.errors.ConnectRestException;
import org.apache.kafka.connect.util.SinkUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

abstract class EmbeddedConnect {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedConnect.class);

    public static final int DEFAULT_NUM_BROKERS = 1;

    protected final int numBrokers;

    private final EmbeddedKafkaCluster kafkaCluster;
    private final boolean maskExitProcedures;
    private final HttpClient httpClient;
    private final ConnectAssertions assertions;
    private final ClassLoader originalClassLoader;

    protected EmbeddedConnect(
            int numBrokers,
            Properties brokerProps,
            boolean maskExitProcedures,
            Map<String, String> clientProps
    ) {
        this.numBrokers = numBrokers;
        this.kafkaCluster = new EmbeddedKafkaCluster(numBrokers, brokerProps, clientProps);
        this.maskExitProcedures = maskExitProcedures;
        this.httpClient = new HttpClient();
        this.assertions = new ConnectAssertions(this);
        // we should keep the original class loader and set it back after connector stopped since the connector will change the class loader,
        // and then, the Mockito will use the unexpected class loader to generate the wrong proxy instance, which makes mock failed
        this.originalClassLoader = Thread.currentThread().getContextClassLoader();
    }

    /**
     * @return the set of all {@link WorkerHandle workers}, running or stopped, in the cluster;
     * may be empty, but never null
     */
    protected abstract Set<WorkerHandle> workers();

    /**
     * Start (or restart) the {@link WorkerHandle workers} in the cluster.
     */
    public abstract void startConnect();

    /**
     * A more graceful way to handle abnormal exit of services in integration tests.
     */
    public Exit.Procedure exitProcedure = (code, message) -> {
        if (code != 0) {
            String exitMessage = "Abrupt service exit with code " + code + " and message " + message;
            log.warn(exitMessage);
            throw new UngracefulShutdownException(exitMessage);
        }
    };

    /**
     * A more graceful way to handle abnormal halt of services in integration tests.
     */
    public Exit.Procedure haltProcedure = (code, message) -> {
        if (code != 0) {
            String haltMessage = "Abrupt service halt with code " + code + " and message " + message;
            log.warn(haltMessage);
            throw new UngracefulShutdownException(haltMessage);
        }
    };

    /**
     * Start the connect cluster and the embedded Kafka and Zookeeper cluster.
     */
    public void start() {
        if (maskExitProcedures) {
            Exit.setExitProcedure(exitProcedure);
            Exit.setHaltProcedure(haltProcedure);
        }
        kafkaCluster.start();
        startConnect();
        try {
            httpClient.start();
        } catch (Exception e) {
            throw new ConnectException("Failed to start HTTP client", e);
        }
    }

    /**
     * Stop the connect cluster and the embedded Kafka and Zookeeper cluster.
     * Clean up any temp directories created locally.
     *
     * @throws RuntimeException if Kafka brokers fail to stop
     */
    public void stop() {
        Utils.closeQuietly(httpClient::stop, "HTTP client for embedded Connect cluster");
        workers().forEach(this::stopWorker);
        try {
            kafkaCluster.stop();
        } catch (UngracefulShutdownException e) {
            log.warn("Kafka did not shutdown gracefully");
        } catch (Exception e) {
            log.error("Could not stop kafka", e);
            throw new RuntimeException("Could not stop brokers", e);
        } finally {
            if (maskExitProcedures) {
                Exit.resetExitProcedure();
                Exit.resetHaltProcedure();
            }
            Plugins.compareAndSwapLoaders(originalClassLoader);
        }
    }

    protected void stopWorker(WorkerHandle worker) {
        try {
            log.info("Stopping worker {}", worker);
            worker.stop();
        } catch (UngracefulShutdownException e) {
            log.warn("Worker {} did not shutdown gracefully", worker);
        } catch (Exception e) {
            log.error("Could not stop connect", e);
            throw new RuntimeException("Could not stop worker", e);
        }
    }

    /**
     * Set a new timeout for REST requests to each worker in the cluster. Useful if a request
     * is expected to block, since the time spent awaiting that request can be reduced
     * and test runtime bloat can be avoided.
     * @param requestTimeoutMs the new timeout in milliseconds; must be positive
     */
    public void requestTimeout(long requestTimeoutMs) {
        workers().forEach(worker -> worker.requestTimeout(requestTimeoutMs));
    }

    /**
     * Configure a connector. If the connector does not already exist, a new one will be created and
     * the given configuration will be applied to it.
     *
     * @param connName   the name of the connector
     * @param connConfig the intended configuration
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException if the configuration fails to be serialized or if the request could not be sent
     */
    public String configureConnector(String connName, Map<String, String> connConfig) {
        String url = endpointForResource(String.format("connectors/%s/config", connName));
        return putConnectorConfig(url, connConfig);
    }

    /**
     * Configure a new connector using the <strong><em>POST /connectors</em></strong> endpoint. If the connector already exists, a
     * {@link ConnectRestException} will be thrown.
     *
     * @param createConnectorRequest the connector creation request
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException if the request could not be sent
     */
    public String configureConnector(CreateConnectorRequest createConnectorRequest) {
        String url = endpointForResource("connectors");
        ObjectMapper objectMapper = new ObjectMapper();

        String requestBody;
        try {
            requestBody = objectMapper.writeValueAsString(createConnectorRequest);
        } catch (IOException e) {
            throw new ConnectException("Failed to serialize connector creation request: " + createConnectorRequest);
        }

        Response response = requestPost(url, requestBody, Collections.emptyMap());
        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            return responseToString(response);
        } else {
            throw new ConnectRestException(
                response.getStatus(),
                "Could not execute 'POST /connectors' request. Error response: " + responseToString(response)
            );
        }
    }

    /**
     * Validate a given connector configuration. If the configuration validates or
     * has a configuration error, an instance of {@link ConfigInfos} is returned. If the validation fails
     * an exception is thrown.
     *
     * @param connClassName the name of the connector class
     * @param connConfig    the intended configuration
     * @throws ConnectRestException if the REST api returns error status
     * @throws ConnectException if the configuration fails to serialize/deserialize or if the request failed to send
     */
    public ConfigInfos validateConnectorConfig(String connClassName, Map<String, String> connConfig) {
        String url = endpointForResource(String.format("connector-plugins/%s/config/validate", connClassName));
        String response = putConnectorConfig(url, connConfig);
        ConfigInfos configInfos;
        try {
            configInfos = new ObjectMapper().readValue(response, ConfigInfos.class);
        } catch (IOException e) {
            throw new ConnectException("Unable deserialize response into a ConfigInfos object");
        }
        return configInfos;
    }

    /**
     * Execute a PUT request with the given connector configuration on the given URL endpoint.
     *
     * @param url        the full URL of the endpoint that corresponds to the given REST resource
     * @param connConfig the intended configuration
     * @throws ConnectRestException if the REST api returns error status
     * @throws ConnectException if the configuration fails to be serialized or if the request could not be sent
     */
    protected String putConnectorConfig(String url, Map<String, String> connConfig) {
        ObjectMapper mapper = new ObjectMapper();
        String content;
        try {
            content = mapper.writeValueAsString(connConfig);
        } catch (IOException e) {
            throw new ConnectException("Could not serialize connector configuration and execute PUT request");
        }
        Response response = requestPut(url, content);
        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            return responseToString(response);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not execute PUT request. Error response: " + responseToString(response));
    }

    /**
     * Patch the config of a connector.
     *
     * @param connName   the name of the connector
     * @param connConfigPatch the configuration patch
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException if the configuration fails to be serialized or if the request could not be sent
     */
    public String patchConnectorConfig(String connName, Map<String, String> connConfigPatch) {
        String url = endpointForResource(String.format("connectors/%s/config", connName));
        return doPatchConnectorConfig(url, connConfigPatch);
    }

    /**
     * Execute a PATCH request with the given connector configuration on the given URL endpoint.
     *
     * @param url        the full URL of the endpoint that corresponds to the given REST resource
     * @param connConfigPatch the configuration patch
     * @throws ConnectRestException if the REST api returns error status
     * @throws ConnectException if the configuration fails to be serialized or if the request could not be sent
     */
    protected String doPatchConnectorConfig(String url, Map<String, String> connConfigPatch) {
        ObjectMapper mapper = new ObjectMapper();
        String content;
        try {
            content = mapper.writeValueAsString(connConfigPatch);
        } catch (IOException e) {
            throw new ConnectException("Could not serialize connector configuration and execute PUT request");
        }
        Response response = requestPatch(url, content);
        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            return responseToString(response);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not execute PATCH request. Error response: " + responseToString(response));
    }

    /**
     * Delete an existing connector.
     *
     * @param connName name of the connector to be deleted
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public void deleteConnector(String connName) {
        String url = endpointForResource(String.format("connectors/%s", connName));
        Response response = requestDelete(url);
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Could not execute DELETE request. Error response: " + responseToString(response));
        }
    }

    /**
     * Stop an existing connector.
     *
     * @param connName name of the connector to be paused
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public void stopConnector(String connName) {
        String url = endpointForResource(String.format("connectors/%s/stop", connName));
        Response response = requestPut(url, "");
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Could not execute PUT request. Error response: " + responseToString(response));
        }
    }

    /**
     * Pause an existing connector.
     *
     * @param connName name of the connector to be paused
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public void pauseConnector(String connName) {
        String url = endpointForResource(String.format("connectors/%s/pause", connName));
        Response response = requestPut(url, "");
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Could not execute PUT request. Error response: " + responseToString(response));
        }
    }

    /**
     * Resume an existing connector.
     *
     * @param connName name of the connector to be resumed
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public void resumeConnector(String connName) {
        String url = endpointForResource(String.format("connectors/%s/resume", connName));
        Response response = requestPut(url, "");
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Could not execute PUT request. Error response: " + responseToString(response));
        }
    }

    /**
     * Restart an existing connector.
     *
     * @param connName name of the connector to be restarted
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public void restartConnector(String connName) {
        String url = endpointForResource(String.format("connectors/%s/restart", connName));
        Response response = requestPost(url, "", Collections.emptyMap());
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Could not execute POST request. Error response: " + responseToString(response));
        }
    }

    /**
     * Restart an existing task.
     *
     * @param connName name of the connector
     * @param taskNum ID of the task (starting from 0)
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public void restartTask(String connName, int taskNum) {
        String url = endpointForResource(String.format("connectors/%s/tasks/%d/restart", connName, taskNum));
        Response response = requestPost(url, "", Collections.emptyMap());
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Could not execute POST request. Error response: " + responseToString(response));
        }
    }

    /**
     * Restart an existing connector and its tasks.
     *
     * @param connName  name of the connector to be restarted
     * @param onlyFailed    true if only failed instances should be restarted
     * @param includeTasks  true if tasks should be restarted, or false if only the connector should be restarted
     * @param onlyCallOnEmptyWorker true if the REST API call should be called on a worker not running this connector or its tasks
     * @throws ConnectRestException if the REST API returns error status
     * @throws ConnectException for any other error.
     */
    public ConnectorStateInfo restartConnectorAndTasks(String connName, boolean onlyFailed, boolean includeTasks, boolean onlyCallOnEmptyWorker) {
        ObjectMapper mapper = new ObjectMapper();
        String restartPath = String.format("connectors/%s/restart?onlyFailed=" + onlyFailed + "&includeTasks=" + includeTasks, connName);
        String restartEndpoint;
        if (onlyCallOnEmptyWorker) {
            restartEndpoint = endpointForResourceNotRunningConnector(restartPath, connName);
        } else {
            restartEndpoint = endpointForResource(restartPath);
        }
        Response response = requestPost(restartEndpoint, "", Collections.emptyMap());
        try {
            if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
                //only the 202 stauts returns a body
                if (response.getStatus() == Response.Status.ACCEPTED.getStatusCode()) {
                    return mapper.readerFor(ConnectorStateInfo.class)
                            .readValue(responseToString(response));
                }
            }
            return null;
        } catch (IOException e) {
            log.error("Could not read connector state from response: {}",
                    responseToString(response), e);
            throw new ConnectException("Could not not parse connector state", e);
        }
    }
    /**
     * Get the connector names of the connectors currently running on this cluster.
     *
     * @return the list of connector names
     * @throws ConnectRestException if the HTTP request to the REST API failed with a valid status code.
     * @throws ConnectException for any other error.
     */
    public Collection<String> connectors() {
        ObjectMapper mapper = new ObjectMapper();
        String url = endpointForResource("connectors");
        Response response = requestGet(url);
        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            try {
                return mapper.readerFor(Collection.class).readValue(responseToString(response));
            } catch (IOException e) {
                log.error("Could not parse connector list from response: {}",
                        responseToString(response), e
                );
                throw new ConnectException("Could not not parse connector list", e);
            }
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not read connector list. Error response: " + responseToString(response));
    }

    /**
     * Get the status for a connector running in this cluster.
     *
     * @param connectorName name of the connector
     * @return an instance of {@link ConnectorStateInfo} populated with state information of the connector and its tasks.
     * @throws ConnectRestException if the HTTP request to the REST API failed with a valid status code.
     * @throws ConnectException for any other error.
     */
    public ConnectorStateInfo connectorStatus(String connectorName) {
        ObjectMapper mapper = new ObjectMapper();
        String url = endpointForResource(String.format("connectors/%s/status", connectorName));
        Response response = requestGet(url);
        try {
            if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
                return mapper.readerFor(ConnectorStateInfo.class)
                        .readValue(responseToString(response));
            }
        } catch (IOException e) {
            log.error("Could not read connector state from response: {}",
                    responseToString(response), e);
            throw new ConnectException("Could not not parse connector state", e);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not read connector state. Error response: " + responseToString(response));
    }

    /**
     * Get the active topics of a connector running in this cluster.
     *
     * @param connectorName name of the connector
     * @return an instance of {@link ConnectorStateInfo} populated with state information of the connector and its tasks.
     * @throws ConnectRestException if the HTTP request to the REST API failed with a valid status code.
     * @throws ConnectException for any other error.
     */
    public ActiveTopicsInfo connectorTopics(String connectorName) {
        ObjectMapper mapper = new ObjectMapper();
        String url = endpointForResource(String.format("connectors/%s/topics", connectorName));
        Response response = requestGet(url);
        try {
            if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
                Map<String, Map<String, List<String>>> activeTopics = mapper
                        .readerFor(new TypeReference<Map<String, Map<String, List<String>>>>() { })
                        .readValue(responseToString(response));
                return new ActiveTopicsInfo(connectorName,
                        activeTopics.get(connectorName).getOrDefault("topics", Collections.emptyList()));
            }
        } catch (IOException e) {
            log.error("Could not read connector state from response: {}",
                    responseToString(response), e);
            throw new ConnectException("Could not not parse connector state", e);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not read connector state. Error response: " + responseToString(response));
    }

    /**
     * Get the info of a connector running in this cluster (retrieved via the <code>GET /connectors/{connector}</code> endpoint).

     * @param connectorName name of the connector
     * @return an instance of {@link ConnectorInfo} populated with state information of the connector and its tasks.
     */
    public ConnectorInfo connectorInfo(String connectorName) {
        ObjectMapper mapper = new ObjectMapper();
        String url = endpointForResource(String.format("connectors/%s", connectorName));
        Response response = requestGet(url);
        try {
            if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
                return mapper.readValue(responseToString(response), ConnectorInfo.class);
            }
        } catch (IOException e) {
            log.error("Could not read connector info from response: {}",
                    responseToString(response), e);
            throw new ConnectException("Could not not parse connector info", e);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not read connector info. Error response: " + responseToString(response));
    }

    /**
     * Get the task configs of a connector running in this cluster.
     *
     * @param connectorName name of the connector
     * @return a list of task configurations for the connector
     */
    public List<TaskInfo> taskConfigs(String connectorName) {
        ObjectMapper mapper = new ObjectMapper();
        String url = endpointForResource(String.format("connectors/%s/tasks", connectorName));
        Response response = requestGet(url);
        try {
            if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
                // We use String instead of ConnectorTaskId as the key here since the latter can't be automatically
                // deserialized by Jackson when used as a JSON object key (i.e., when it's serialized as a JSON string)
                return mapper.readValue(responseToString(response), new TypeReference<List<TaskInfo>>() { });
            }
        } catch (IOException e) {
            log.error("Could not read task configs from response: {}",
                    responseToString(response), e);
            throw new ConnectException("Could not not parse task configs", e);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not read task configs. Error response: " + responseToString(response));
    }

    /**
     * Reset the set of active topics of a connector running in this cluster.
     *
     * @param connectorName name of the connector
     * @throws ConnectRestException if the HTTP request to the REST API failed with a valid status code.
     * @throws ConnectException for any other error.
     */
    public void resetConnectorTopics(String connectorName) {
        String url = endpointForResource(String.format("connectors/%s/topics/reset", connectorName));
        Response response = requestPut(url, null);
        if (response.getStatus() >= Response.Status.BAD_REQUEST.getStatusCode()) {
            throw new ConnectRestException(response.getStatus(),
                    "Resetting active topics for connector " + connectorName + " failed. "
                            + "Error response: " + responseToString(response));
        }
    }

    /**
     * Get the offsets for a connector via the <strong><em>GET /connectors/{connector}/offsets</em></strong> endpoint
     *
     * @param connectorName name of the connector whose offsets are to be retrieved
     * @return the connector's offsets
     */
    public ConnectorOffsets connectorOffsets(String connectorName) {
        String url = endpointForResource(String.format("connectors/%s/offsets", connectorName));
        Response response = requestGet(url);
        ObjectMapper mapper = new ObjectMapper();

        try {
            if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
                return mapper.readerFor(ConnectorOffsets.class).readValue(responseToString(response));
            }
        } catch (IOException e) {
            throw new ConnectException("Could not not parse connector offsets", e);
        }
        throw new ConnectRestException(response.getStatus(),
                "Could not fetch connector offsets. Error response: " + responseToString(response));
    }

    /**
     * Alter the offset for a source connector's partition via the <strong><em>PATCH /connectors/{connector}/offsets</em></strong>
     * endpoint
     *
     * @param connectorName name of the source connector whose offset is to be altered
     * @param partition the source partition for which the offset is to be altered
     * @param offset the source offset to be written
     *
     * @return the API response as a {@link java.lang.String}
     */
    public String alterSourceConnectorOffset(String connectorName, Map<String, ?> partition, Map<String, ?> offset) {
        return alterConnectorOffsets(
                connectorName,
                new ConnectorOffsets(Collections.singletonList(new ConnectorOffset(partition, offset)))
        );
    }

    /**
     * Alter the offset for a sink connector's topic partition via the <strong><em>PATCH /connectors/{connector}/offsets</em></strong>
     * endpoint
     *
     * @param connectorName name of the sink connector whose offset is to be altered
     * @param topicPartition the topic partition for which the offset is to be altered
     * @param offset the offset to be written
     *
     * @return the API response as a {@link java.lang.String}
     */
    public String alterSinkConnectorOffset(String connectorName, TopicPartition topicPartition, Long offset) {
        return alterConnectorOffsets(
                connectorName,
                SinkUtils.consumerGroupOffsetsToConnectorOffsets(Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset)))
        );
    }

    /**
     * Alter a connector's offsets via the <strong><em>PATCH /connectors/{connector}/offsets</em></strong> endpoint
     *
     * @param connectorName name of the connector whose offsets are to be altered
     * @param offsets offsets to alter
     *
     * @return the API response as a {@link java.lang.String}
     */
    public String alterConnectorOffsets(String connectorName, ConnectorOffsets offsets) {
        String url = endpointForResource(String.format("connectors/%s/offsets", connectorName));
        ObjectMapper mapper = new ObjectMapper();
        String content;
        try {
            content = mapper.writeValueAsString(offsets);
        } catch (IOException e) {
            throw new ConnectException("Could not serialize connector offsets and execute PATCH request");
        }

        Response response = requestPatch(url, content);
        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            return responseToString(response);
        } else {
            throw new ConnectRestException(response.getStatus(),
                    "Could not alter connector offsets. Error response: " + responseToString(response));
        }
    }

    /**
     * Reset a connector's offsets via the <strong><em>DELETE /connectors/{connector}/offsets</em></strong> endpoint
     *
     * @param connectorName name of the connector whose offsets are to be reset
     */
    public String resetConnectorOffsets(String connectorName) {
        String url = endpointForResource(String.format("connectors/%s/offsets", connectorName));
        Response response = requestDelete(url);
        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            return responseToString(response);
        } else {
            throw new ConnectRestException(response.getStatus(),
                    "Could not reset connector offsets. Error response: " + responseToString(response));
        }
    }

    /**
     * Get the {@link LoggerLevel level} for a specific logger
     * @param logger the name of the logger
     * @return the level for the logger, as reported by the Connect REST API
     */
    public LoggerLevel getLogLevel(String logger) {
        String resource = "admin/loggers/" + logger;
        String url = adminEndpoint(resource);
        Response response = requestGet(url);

        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper.readerFor(LoggerLevel.class).readValue(responseToString(response));
            } catch (IOException e) {
                log.error("Could not read logger level from response: {}",
                        responseToString(response), e);
                throw new ConnectException("Could not not parse logger level", e);
            }
        } else {
            throw new ConnectRestException(
                    response.getStatus(),
                    "Could not read log level. Error response: " + responseToString(response)
            );
        }
    }

    /**
     * Get the {@link LoggerLevel levels} for all known loggers
     * @return the levels of all known loggers, as reported by the Connect REST API
     */
    public Map<String, LoggerLevel> allLogLevels() {
        String resource = "admin/loggers";
        String url = adminEndpoint(resource);
        Response response = requestGet(url);

        if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper
                        .readerFor(new TypeReference<Map<String, LoggerLevel>>() { })
                        .readValue(responseToString(response));
            } catch (IOException e) {
                log.error("Could not read logger levels from response: {}",
                        responseToString(response), e);
                throw new ConnectException("Could not not parse logger levels", e);
            }
        } else {
            throw new ConnectRestException(
                    response.getStatus(),
                    "Could not read log levels. Error response: " + responseToString(response)
            );
        }
    }

    /**
     * Adjust the level of a logging namespace.
     * @param namespace the namespace to adjust; may not be null
     * @param level the level to set the namespace to; may not be null
     * @param scope the scope of the operation; may be null
     * @return the list of affected loggers, as reported by the Connect REST API;
     * may be null if no body was included in the response
     */
    public List<String> setLogLevel(String namespace, String level, String scope) {
        String resource = "admin/loggers/" + namespace;
        if (scope != null)
            resource += "?scope=" + scope;
        String url = adminEndpoint(resource);
        String body = "{\"level\": \"" + level + "\"}";
        Response response = requestPut(url, body);

        if (response.getStatus() == Response.Status.NO_CONTENT.getStatusCode()) {
            if (response.getEntity() != null && !response.getEntity().equals("")) {
                // Don't use JUnit assertNull here because this library is used by both
                // Connect runtime tests and MirrorMaker 2 tests, which use different
                // versions of JUnit
                throw new AssertionError(
                        "Response with 204 status contained non-null entity: '"
                                + response.getEntity() + "'"
                );
            }
            return null;
        } else if (response.getStatus() < Response.Status.BAD_REQUEST.getStatusCode()) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper
                        .readerFor(new TypeReference<List<String>>() { })
                        .readValue(responseToString(response));
            } catch (IOException e) {
                log.error("Could not read loggers from response: {}",
                        responseToString(response), e);
                throw new ConnectException("Could not not parse loggers", e);
            }
        } else {
            throw new ConnectRestException(
                    response.getStatus(),
                    "Could not set log level. Error response: " + responseToString(response)
            );
        }
    }

    /**
     * Get the full URL of the admin endpoint that corresponds to the given REST resource
     *
     * @param resource the resource under the worker's admin endpoint
     * @return the admin endpoint URL
     * @throws ConnectException if no admin REST endpoint is available
     */
    public String adminEndpoint(String resource) {
        String url = workers().stream()
                .map(WorkerHandle::adminUrl)
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new ConnectException("Admin endpoint is disabled."))
                .toString();
        return url + resource;
    }

    /**
     * Get the full URL of the endpoint that corresponds to the given REST resource
     *
     * @param resource the resource under the worker's admin endpoint
     * @return the admin endpoint URL
     * @throws ConnectException if no REST endpoint is available
     */
    public String endpointForResource(String resource) {
        String url = workers().stream()
                .map(WorkerHandle::url)
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() -> new ConnectException("Connect workers have not been provisioned"))
                .toString();
        return url + resource;
    }

    /**
     * Get the full URL of the endpoint that corresponds to the given REST resource using a worker
     * that is not running any tasks or connector instance for the connectorName provided in the arguments
     *
     * @param resource the resource under the worker's admin endpoint
     * @param connectorName the name of the connector
     * @return the admin endpoint URL
     * @throws ConnectException if no REST endpoint is available
     */
    public String endpointForResourceNotRunningConnector(String resource, String connectorName) {
        ConnectorStateInfo info = connectorStatus(connectorName);
        Set<String> activeWorkerUrls = new HashSet<>();
        activeWorkerUrls.add(String.format("http://%s/", info.connector().workerId()));
        info.tasks().forEach(t -> activeWorkerUrls.add(String.format("http://%s/", t.workerId())));
        String url = workers().stream()
                .map(WorkerHandle::url)
                .filter(Objects::nonNull)
                .filter(workerUrl -> !activeWorkerUrls.contains(workerUrl.toString()))
                .findFirst()
                .orElseThrow(() -> new ConnectException(
                        String.format("Connect workers have not been provisioned or no free worker found that is not running this connector(%s) or its tasks", connectorName)))
                .toString();
        return url + resource;
    }

    /**
     * Return the handle to the Kafka cluster this Connect cluster connects to.
     *
     * @return the Kafka cluster handle
     */
    public EmbeddedKafkaCluster kafka() {
        return kafkaCluster;
    }

    /**
     * Execute a GET request on the given URL.
     *
     * @param url the HTTP endpoint
     * @return the response to the GET request
     * @throws ConnectException if execution of the GET request fails
     * @deprecated Use {@link #requestGet(String)} instead.
     */
    @Deprecated
    public String executeGet(String url) {
        return responseToString(requestGet(url));
    }

    /**
     * Execute a GET request on the given URL.
     *
     * @param url the HTTP endpoint
     * @return the response to the GET request
     * @throws ConnectException if execution of the GET request fails
     */
    public Response requestGet(String url) {
        return requestHttpMethod(url, null, Collections.emptyMap(), "GET");
    }

    /**
     * Execute a PUT request on the given URL.
     *
     * @param url the HTTP endpoint
     * @param body the payload of the PUT request
     * @return the response to the PUT request
     * @throws ConnectException if execution of the PUT request fails
     * @deprecated Use {@link #requestPut(String, String)} instead.
     */
    @Deprecated
    public int executePut(String url, String body) {
        return requestPut(url, body).getStatus();
    }

    /**
     * Execute a PUT request on the given URL.
     *
     * @param url the HTTP endpoint
     * @param body the payload of the PUT request
     * @return the response to the PUT request
     * @throws ConnectException if execution of the PUT request fails
     */
    public Response requestPut(String url, String body) {
        return requestHttpMethod(url, body, Collections.emptyMap(), "PUT");
    }

    /**
     * Execute a POST request on the given URL.
     *
     * @param url the HTTP endpoint
     * @param body the payload of the POST request
     * @param headers a map that stores the POST request headers
     * @return the response to the POST request
     * @throws ConnectException if execution of the POST request fails
     * @deprecated Use {@link #requestPost(String, String, java.util.Map)} instead.
     */
    @Deprecated
    public int executePost(String url, String body, Map<String, String> headers) {
        return requestPost(url, body, headers).getStatus();
    }

    /**
     * Execute a POST request on the given URL.
     *
     * @param url the HTTP endpoint
     * @param body the payload of the POST request
     * @param headers a map that stores the POST request headers
     * @return the response to the POST request
     * @throws ConnectException if execution of the POST request fails
     */
    public Response requestPost(String url, String body, Map<String, String> headers) {
        return requestHttpMethod(url, body, headers, "POST");
    }

    /**
     * Execute a PATCH request on the given URL.
     *
     * @param url the HTTP endpoint
     * @param body the payload of the PATCH request
     * @return the response to the PATCH request
     * @throws ConnectException if execution of the PATCH request fails
     */
    public Response requestPatch(String url, String body) {
        return requestHttpMethod(url, body, Collections.emptyMap(), "PATCH");
    }

    /**
     * Execute a DELETE request on the given URL.
     *
     * @param url the HTTP endpoint
     * @return the response to the DELETE request
     * @throws ConnectException if execution of the DELETE request fails
     * @deprecated Use {@link #requestDelete(String)} instead.
     */
    @Deprecated
    public int executeDelete(String url) {
        return requestDelete(url).getStatus();
    }

    /**
     * Execute a DELETE request on the given URL.
     *
     * @param url the HTTP endpoint
     * @return the response to the DELETE request
     * @throws ConnectException if execution of the DELETE request fails
     */
    public Response requestDelete(String url) {
        return requestHttpMethod(url, null, Collections.emptyMap(), "DELETE");
    }

    /**
     * A general method that executes an HTTP request on a given URL.
     *
     * @param url the HTTP endpoint
     * @param body the payload of the request; null if there isn't one
     * @param headers a map that stores the request headers; empty if there are no headers
     * @param httpMethod the name of the HTTP method to execute
     * @return the response to the HTTP request
     * @throws ConnectException if execution of the HTTP method fails
     */
    protected Response requestHttpMethod(String url, String body, Map<String, String> headers,
                                         String httpMethod) {
        log.debug("Executing {} request to URL={}." + (body != null ? " Payload={}" : ""),
                httpMethod, url, body);

        try {
            Request req = httpClient.newRequest(url);
            req.method(httpMethod);
            if (body != null) {
                headers.forEach(req::header);
                req.content(new StringContentProvider(body), "application/json");
            }

            ContentResponse res = req.send();
            log.info("{} response for URL={} is {}",
                    httpMethod, url, res.getContentAsString().isEmpty() ? "empty" : res.getContentAsString());
            return Response.status(Response.Status.fromStatusCode(res.getStatus()))
                    .entity(res.getContentAsString())
                    .build();
        } catch (Exception e) {
            log.error("Could not execute " + httpMethod + " request to " + url, e);
            throw new ConnectException(e);
        }
    }

    private String responseToString(Response response) {
        return response == null ? "empty" : (String) response.getEntity();
    }

    /**
     * Get the workers that are up and running.
     *
     * @return the list of handles of the online workers
     */
    public Set<WorkerHandle> activeWorkers() {
        return workers().stream()
                .filter(w -> {
                    try {
                        String endpoint = w.url().resolve("/connectors/liveness-check").toString();
                        Response response = requestGet(endpoint);
                        boolean live = response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()
                                || response.getStatus() == Response.Status.OK.getStatusCode();
                        if (live) {
                            return true;
                        } else {
                            log.warn("Worker failed liveness probe. Response: {}", response);
                            return false;
                        }
                    } catch (Exception e) {
                        // Worker failed to respond. Consider it's offline
                        log.warn("Failed to contact worker during liveness check", e);
                        return false;
                    }
                })
                .collect(Collectors.toSet());
    }


    /**
     * Return the available assertions for this Connect cluster
     *
     * @return the assertions object
     */
    public ConnectAssertions assertions() {
        return assertions;
    }

}
