/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.file;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FileStreamSourceConnectorTest {

    private static final String SINGLE_TOPIC = "test";
    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String FILENAME = "/somefilename";

    private FileStreamSourceConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new FileStreamSourceConnector();
        ctx = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sourceProperties = new HashMap<>();
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, SINGLE_TOPIC);
        sourceProperties.put(FileStreamSourceConnector.FILE_CONFIG, FILENAME);
    }

    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();

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

        PowerMock.verifyAll();
    }

    @Test
    public void testSourceTasksStdin() {
        PowerMock.replayAll();

        sourceProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));

        PowerMock.verifyAll();
    }

    @Test(expected = ConnectException.class)
    public void testMultipleSourcesInvalid() {
        sourceProperties.put(FileStreamSourceConnector.TOPIC_CONFIG, MULTIPLE_TOPICS);
        connector.start(sourceProperties);
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();

        connector.start(sourceProperties);
        assertEquals(FileStreamSourceTask.class, connector.taskClass());

        PowerMock.verifyAll();
    }
}
