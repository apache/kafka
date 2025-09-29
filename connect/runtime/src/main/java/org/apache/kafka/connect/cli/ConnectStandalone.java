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
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.Connect;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.RestClient;
import org.apache.kafka.connect.runtime.rest.RestServer;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.FutureCallback;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;

/**
 * <p>
 * Command line utility that runs Kafka Connect as a standalone process. In this mode, work (connectors and tasks) is not
 * distributed. Instead, all the normal Connect machinery works within a single process. This is useful for ad hoc,
 * small, or experimental jobs.
 * </p>
 * <p>
 * Connector and task configs are stored in memory and are not persistent. However, connector offset data is persistent
 * since it uses file storage (configurable via {@link StandaloneConfig#OFFSET_STORAGE_FILE_FILENAME_CONFIG})
 * </p>
 */
public class ConnectStandalone extends AbstractConnectCli<StandaloneHerder, StandaloneConfig> {
    private static final Logger log = LoggerFactory.getLogger(ConnectStandalone.class);

    public ConnectStandalone(String... args) {
        super(args);
    }

    @Override
    protected String usage() {
        return "ConnectStandalone worker.properties [connector1.properties connector2.json ...]";
    }

    @Override
    public void processExtraArgs(Connect<StandaloneHerder> connect, String[] extraArgs) {
        try {
            for (final String connectorConfigFile : extraArgs) {
                CreateConnectorRequest createConnectorRequest = parseConnectorConfigurationFile(connectorConfigFile);
                FutureCallback<Herder.Created<ConnectorInfo>> cb = new FutureCallback<>((error, info) -> {
                    if (error != null)
                        log.error("Failed to create connector for {}", connectorConfigFile);
                    else
                        log.info("Created connector {}", info.result().name());
                });
                connect.herder().putConnectorConfig(
                    createConnectorRequest.name(), createConnectorRequest.config(),
                    createConnectorRequest.initialTargetState(),
                    false, cb);
                cb.get();
            }
            connect.herder().ready();
        } catch (Throwable t) {
            log.error("Stopping after connector error", t);
            connect.stop();
            Exit.exit(3);
        }
    }

    /**
     * Parse a connector configuration file into a {@link CreateConnectorRequest}. The file can have any one of the following formats (note that
     * we attempt to parse the file in this order):
     * <ol>
     *     <li>A JSON file containing an Object with only String keys and values that represent the connector configuration.</li>
     *     <li>A JSON file containing an Object that can be parsed directly into a {@link CreateConnectorRequest}</li>
     *     <li>A valid Java Properties file (i.e. containing String key/value pairs representing the connector configuration)</li>
     * </ol>
     * <p>
     * Visible for testing.
     *
     * @param filePath the path of the connector configuration file
     * @return the parsed connector configuration in the form of a {@link CreateConnectorRequest}
     */
    CreateConnectorRequest parseConnectorConfigurationFile(String filePath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        File connectorConfigurationFile = Paths.get(filePath).toFile();
        try {
            Map<String, String> connectorConfigs = objectMapper.readValue(connectorConfigurationFile, new TypeReference<>() { });

            if (!connectorConfigs.containsKey(NAME_CONFIG)) {
                throw new ConnectException("Connector configuration at '" + filePath + "' is missing the mandatory '" + NAME_CONFIG + "' "
                    + "configuration");
            }
            return new CreateConnectorRequest(connectorConfigs.get(NAME_CONFIG), connectorConfigs, null);
        } catch (StreamReadException | DatabindException e) {
            log.debug("Could not parse connector configuration file '{}' into a Map with String keys and values", filePath);
        }

        try {
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            CreateConnectorRequest createConnectorRequest = objectMapper.readValue(connectorConfigurationFile, new TypeReference<>() { });
            if (createConnectorRequest.config().containsKey(NAME_CONFIG)) {
                if (!createConnectorRequest.config().get(NAME_CONFIG).equals(createConnectorRequest.name())) {
                    throw new ConnectException("Connector name configuration in 'config' doesn't match the one specified in 'name' at '" + filePath
                        + "'");
                }
            } else {
                createConnectorRequest.config().put(NAME_CONFIG, createConnectorRequest.name());
            }
            return createConnectorRequest;
        } catch (StreamReadException | DatabindException e) {
            log.debug("Could not parse connector configuration file '{}' into an object of type {}",
                filePath, CreateConnectorRequest.class.getSimpleName());
        }

        Map<String, String> connectorConfigs = Utils.propsToStringMap(Utils.loadProps(filePath));
        if (!connectorConfigs.containsKey(NAME_CONFIG)) {
            throw new ConnectException("Connector configuration at '" + filePath + "' is missing the mandatory '" + NAME_CONFIG + "' "
                + "configuration");
        }
        return new CreateConnectorRequest(connectorConfigs.get(NAME_CONFIG), connectorConfigs, null);
    }

    @Override
    protected StandaloneHerder createHerder(StandaloneConfig config, String workerId, Plugins plugins,
                                  ConnectorClientConfigOverridePolicy connectorClientConfigOverridePolicy,
                                  RestServer restServer, RestClient restClient) {

        OffsetBackingStore offsetBackingStore = new FileOffsetBackingStore(plugins.newInternalConverter(
                true, JsonConverter.class.getName(), Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false")));
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
