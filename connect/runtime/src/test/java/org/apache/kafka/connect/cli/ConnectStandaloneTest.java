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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.runtime.rest.entities.CreateConnectorRequest;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.connect.runtime.ConnectorConfig.NAME_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ConnectStandaloneTest {

    private static final String CONNECTOR_NAME = "test-connector";
    private static final Map<String, String> CONNECTOR_CONFIG = new HashMap<>();
    static {
        CONNECTOR_CONFIG.put(NAME_CONFIG, CONNECTOR_NAME);
        CONNECTOR_CONFIG.put("key1", "val1");
        CONNECTOR_CONFIG.put("key2", "val2");
    }

    private final ConnectStandalone connectStandalone = new ConnectStandalone();
    private File connectorConfigurationFile;

    @Before
    public void setUp() throws IOException {
        connectorConfigurationFile = TestUtils.tempFile();
    }

    @Test
    public void testParseJavaPropertiesFile() throws Exception {
        Properties properties = new Properties();
        CONNECTOR_CONFIG.forEach(properties::setProperty);

        try (FileWriter writer = new FileWriter(connectorConfigurationFile)) {
            properties.store(writer, null);
        }

        CreateConnectorRequest request = connectStandalone.parseConnectorConfigurationFile(connectorConfigurationFile.getAbsolutePath());
        assertEquals(CONNECTOR_NAME, request.name());
        assertEquals(CONNECTOR_CONFIG, request.config());
        assertNull(request.initialState());
    }

    @Test
    public void testParseJsonFileWithConnectorConfiguration() throws Exception {
        try (FileWriter writer = new FileWriter(connectorConfigurationFile)) {
            writer.write(new ObjectMapper().writeValueAsString(CONNECTOR_CONFIG));
        }

        CreateConnectorRequest request = connectStandalone.parseConnectorConfigurationFile(connectorConfigurationFile.getAbsolutePath());
        assertEquals(CONNECTOR_NAME, request.name());
        assertEquals(CONNECTOR_CONFIG, request.config());
        assertNull(request.initialState());
    }

    @Test
    public void testParseJsonFileWithCreateConnectorRequest() throws Exception {
        CreateConnectorRequest requestToWrite = new CreateConnectorRequest(
            CONNECTOR_NAME,
            CONNECTOR_CONFIG,
            CreateConnectorRequest.InitialState.STOPPED
        );

        try (FileWriter writer = new FileWriter(connectorConfigurationFile)) {
            writer.write(new ObjectMapper().writeValueAsString(requestToWrite));
        }

        CreateConnectorRequest parsedRequest = connectStandalone.parseConnectorConfigurationFile(connectorConfigurationFile.getAbsolutePath());
        assertEquals(requestToWrite, parsedRequest);
    }

    @Test
    public void testParseJsonFileWithCreateConnectorRequestWithoutInitialState() throws Exception {
        Map<String, Object> requestToWrite = new HashMap<>();
        requestToWrite.put("name", CONNECTOR_NAME);
        requestToWrite.put("config", CONNECTOR_CONFIG);

        try (FileWriter writer = new FileWriter(connectorConfigurationFile)) {
            writer.write(new ObjectMapper().writeValueAsString(requestToWrite));
        }

        CreateConnectorRequest parsedRequest = connectStandalone.parseConnectorConfigurationFile(connectorConfigurationFile.getAbsolutePath());
        CreateConnectorRequest expectedRequest = new CreateConnectorRequest(CONNECTOR_NAME, CONNECTOR_CONFIG, null);
        assertEquals(expectedRequest, parsedRequest);
    }

    @Test
    public void testParseJsonFileWithCreateConnectorRequestWithUnknownField() throws Exception {
        Map<String, Object> requestToWrite = new HashMap<>();
        requestToWrite.put("name", CONNECTOR_NAME);
        requestToWrite.put("config", CONNECTOR_CONFIG);
        requestToWrite.put("unknown-field", "random-value");

        try (FileWriter writer = new FileWriter(connectorConfigurationFile)) {
            writer.write(new ObjectMapper().writeValueAsString(requestToWrite));
        }

        CreateConnectorRequest parsedRequest = connectStandalone.parseConnectorConfigurationFile(connectorConfigurationFile.getAbsolutePath());
        CreateConnectorRequest expectedRequest = new CreateConnectorRequest(CONNECTOR_NAME, CONNECTOR_CONFIG, null);
        assertEquals(expectedRequest, parsedRequest);
    }
}
