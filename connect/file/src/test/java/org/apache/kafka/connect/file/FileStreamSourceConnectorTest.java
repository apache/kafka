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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.kafka.connect.file.FileStreamSourceTask.FILENAME_FIELD;
import static org.apache.kafka.connect.file.FileStreamSourceTask.POSITION_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class FileStreamSourceConnectorTest {

    private static final String SINGLE_TOPIC = "test";
    private static final String FILENAME = "/somefilename";

    private FileStreamSourceConnector connector;
    private Map<String, String> sourceProperties;

    @BeforeEach
    public void setup() {
        connector = new FileStreamSourceConnector();
        ConnectorContext ctx = mock(ConnectorContext.class);
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
    public void testExactlyOnceSupport() {
        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG, FILENAME);
        assertEquals(ExactlyOnceSupport.SUPPORTED, connector.exactlyOnceSupport(sourceProperties));

        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG, " ");
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(sourceProperties));

        sourceProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(sourceProperties));
    }

    @Test
    public void testTransactionBoundaryDefinition() {
        assertEquals(ConnectorTransactionBoundaries.UNSUPPORTED, connector.canDefineTransactionBoundaries(sourceProperties));
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
    public void testTaskClass() {
        connector.start(sourceProperties);
        assertEquals(FileStreamSourceTask.class, connector.taskClass());
    }

    @Test
    public void testConnectorConfigsPropagateToTaskConfigs() {
        // This is required so that updates in transforms/converters/clients configs get reflected
        // in tasks without manual restarts of the tasks (see https://issues.apache.org/jira/browse/KAFKA-13809)
        sourceProperties.put("transforms", "insert");
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals("insert", taskConfigs.get(0).get("transforms"));
    }

    @Test
    public void testValidConfigsAndDefaults() {
        AbstractConfig config = new AbstractConfig(FileStreamSourceConnector.CONFIG_DEF, sourceProperties);
        assertEquals(SINGLE_TOPIC, config.getString(FileStreamSourceConnector.TOPIC_CONFIG));
        assertEquals(FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE, config.getInt(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG));
    }

    @Test
    public void testMissingTopic() {
        sourceProperties.remove(FileStreamSourceConnector.TOPIC_CONFIG);
        assertThrows(ConfigException.class, () -> new AbstractConfig(FileStreamSourceConnector.CONFIG_DEF, sourceProperties));
    }

    @Test
    public void testBlankTopic() {
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, "   ");
        assertThrows(ConfigException.class, () -> new AbstractConfig(FileStreamSourceConnector.CONFIG_DEF, sourceProperties));
    }

    @Test
    public void testInvalidBatchSize() {
        sourceProperties.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, "abcd");
        assertThrows(ConfigException.class, () -> new AbstractConfig(FileStreamSourceConnector.CONFIG_DEF, sourceProperties));
    }

    @Test
    public void testAlterOffsetsStdin() {
        sourceProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                Collections.singletonMap(FILENAME_FIELD, FILENAME),
                Collections.singletonMap(POSITION_FIELD, 0L)
        );
        assertThrows(ConnectException.class, () -> connector.alterOffsets(sourceProperties, offsets));
    }

    @Test
    public void testAlterOffsetsIncorrectPartitionKey() {
        assertThrows(ConnectException.class, () -> connector.alterOffsets(sourceProperties, Collections.singletonMap(
                Collections.singletonMap("other_partition_key", FILENAME),
                Collections.singletonMap(POSITION_FIELD, 0L)
        )));

        // null partitions are invalid
        assertThrows(ConnectException.class, () -> connector.alterOffsets(sourceProperties, Collections.singletonMap(
                null,
                Collections.singletonMap(POSITION_FIELD, 0L)
        )));
    }

    @Test
    public void testAlterOffsetsMultiplePartitions() {
        Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
        offsets.put(Collections.singletonMap(FILENAME_FIELD, FILENAME), Collections.singletonMap(POSITION_FIELD, 0L));
        offsets.put(Collections.singletonMap(FILENAME_FIELD, "/someotherfilename"), null);
        assertTrue(connector.alterOffsets(sourceProperties, offsets));
    }

    @Test
    public void testAlterOffsetsIncorrectOffsetKey() {
        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                Collections.singletonMap(FILENAME_FIELD, FILENAME),
                Collections.singletonMap("other_offset_key", 0L)
        );
        assertThrows(ConnectException.class, () -> connector.alterOffsets(sourceProperties, offsets));
    }

    @Test
    public void testAlterOffsetsOffsetPositionValues() {
        Function<Object, Boolean> alterOffsets = offset -> connector.alterOffsets(sourceProperties, Collections.singletonMap(
                Collections.singletonMap(FILENAME_FIELD, FILENAME),
                Collections.singletonMap(POSITION_FIELD, offset)
        ));

        assertThrows(ConnectException.class, () -> alterOffsets.apply("nan"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(null));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(new Object()));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(3.14));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(-420));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("-420"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(10));
        assertThrows(ConnectException.class, () -> alterOffsets.apply("10"));
        assertThrows(ConnectException.class, () -> alterOffsets.apply(-10L));
        assertTrue(() -> alterOffsets.apply(10L));
    }

    @Test
    public void testSuccessfulAlterOffsets() {
        Map<Map<String, ?>, Map<String, ?>> offsets = Collections.singletonMap(
                Collections.singletonMap(FILENAME_FIELD, FILENAME),
                Collections.singletonMap(POSITION_FIELD, 0L)
        );

        // Expect no exception to be thrown when a valid offsets map is passed. An empty offsets map is treated as valid
        // since it could indicate that the offsets were reset previously or that no offsets have been committed yet
        // (for a reset operation)
        assertTrue(connector.alterOffsets(sourceProperties, offsets));
        assertTrue(connector.alterOffsets(sourceProperties, new HashMap<>()));
    }

    @Test
    public void testAlterOffsetsTombstones() {
        Function<Map<String, ?>, Boolean> alterOffsets = partition -> connector.alterOffsets(
            sourceProperties,
            Collections.singletonMap(partition, null)
        );

        assertTrue(alterOffsets.apply(null));
        assertTrue(alterOffsets.apply(Collections.emptyMap()));
        assertTrue(alterOffsets.apply(Collections.singletonMap(FILENAME_FIELD, FILENAME)));
        assertTrue(alterOffsets.apply(Collections.singletonMap(FILENAME_FIELD, "/someotherfilename")));
        assertTrue(alterOffsets.apply(Collections.singletonMap("garbage_partition_key", "garbage_partition_value")));
    }
}
