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
package org.apache.kafka.connect.file;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileStreamSourceConnectorTest {

    private static final String SINGLE_TOPIC = "test";
    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String FILENAME = "/somefilename";

    private FileStreamSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @BeforeEach
    public void setup() {
        connector = new FileStreamSourceConnector();
        ctx = mock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, SINGLE_TOPIC);
        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG, FILENAME);
    }

    @Test
    public void testConnectorConfigValidation() {
        List<ConfigValue> configValues = connector.config().validate(sourceProperties);
        for (ConfigValue val : configValues) {
            assertEquals(0, val.errorMessages().size(), "Config property errors: " + val.errorMessages());
        }
    }

    @Test
    public void testSourceTasks() {
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));

        // Should be able to return fewer than requested #
        taskConfigs = connector.taskConfigs(2);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME,
                taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
        assertEquals(SINGLE_TOPIC,
                taskConfigs.get(0).get(FileStreamSourceConnector.TOPIC_CONFIG));
    }

    @Test
    public void testSourceTasksStdin() {
        sourceProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));
    }

    @Test
    public void testMultipleSourcesInvalid() {
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, MULTIPLE_TOPICS);
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }

    @Test
    public void testTaskClass() {
        connector.start(sourceProperties);
        assertEquals(FileStreamSourceTask.class, connector.taskClass());
    }

    @Test
    public void testMissingTopic() {
        sourceProperties.remove(FileStreamSourceConnector.TOPIC_CONFIG);
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }

    @Test
    public void testBlankTopic() {
        // Because of trimming this tests is same as testing for empty string.
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, "     ");
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }

    @Test
    public void testInvalidBatchSize() {
        sourceProperties.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, "abcd");
        assertThrows(ConfigException.class, () -> connector.start(sourceProperties));
    }
}
