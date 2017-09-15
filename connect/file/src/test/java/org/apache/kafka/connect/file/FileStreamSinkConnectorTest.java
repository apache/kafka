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

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.sink.SinkConnector;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class FileStreamSinkConnectorTest extends EasyMockSupport {

    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String FILENAME = "/afilename";

    private FileStreamSinkConnector connector;
    private ConnectorContext ctx;
    private Map<String, String> sinkProperties;

    @Before
    public void setup() {
        connector = new FileStreamSinkConnector();
        ctx = createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sinkProperties = new HashMap<>();
        sinkProperties.put(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
        sinkProperties.put(FileStreamSinkConnector.FILE_CONFIG, FILENAME);
    }

    @Test
    public void testSinkTasks() {
        replayAll();

        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME, taskConfigs.get(0).get(FileStreamSinkConnector.FILE_CONFIG));

        taskConfigs = connector.taskConfigs(2);
        assertEquals(2, taskConfigs.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(FILENAME, taskConfigs.get(0).get(FileStreamSinkConnector.FILE_CONFIG));
        }

        verifyAll();
    }

    @Test
    public void testSinkTasksStdout() {
        replayAll();

        sinkProperties.remove(FileStreamSourceConnector.FILE_CONFIG);
        connector.start(sinkProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertNull(taskConfigs.get(0).get(FileStreamSourceConnector.FILE_CONFIG));

        verifyAll();
    }

    @Test
    public void testTaskClass() {
        replayAll();

        connector.start(sinkProperties);
        assertEquals(FileStreamSinkTask.class, connector.taskClass());

        verifyAll();
    }
}
