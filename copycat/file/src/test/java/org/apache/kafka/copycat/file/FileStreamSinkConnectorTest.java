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

package org.apache.kafka.copycat.file;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.copycat.connector.ConnectorContext;
import org.apache.kafka.copycat.sink.SinkConnector;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FileStreamSinkConnectorTest {

    private static final String MULTIPLE_TOPICS = "test1,test2";
    private static final String[] MULTIPLE_TOPICS_LIST
            = MULTIPLE_TOPICS.split(",");
    private static final List<TopicPartition> MULTIPLE_TOPICS_PARTITIONS = Arrays.asList(
            new TopicPartition("test1", 1), new TopicPartition("test2", 2)
    );
    private static final String FILENAME = "/afilename";

    private FileStreamSinkConnector connector;
    private ConnectorContext ctx;
    private Properties sinkProperties;

    @Before
    public void setup() {
        connector = new FileStreamSinkConnector();
        ctx = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(ctx);

        sinkProperties = new Properties();
        sinkProperties.setProperty(SinkConnector.TOPICS_CONFIG, MULTIPLE_TOPICS);
        sinkProperties.setProperty(FileStreamSinkConnector.FILE_CONFIG, FILENAME);
    }

    @Test
    public void testSinkTasks() {
        PowerMock.replayAll();

        connector.start(sinkProperties);
        List<Properties> taskConfigs = connector.getTaskConfigs(1);
        assertEquals(1, taskConfigs.size());
        assertEquals(FILENAME, taskConfigs.get(0).getProperty(FileStreamSinkConnector.FILE_CONFIG));

        taskConfigs = connector.getTaskConfigs(2);
        assertEquals(2, taskConfigs.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(FILENAME, taskConfigs.get(0).getProperty(FileStreamSinkConnector.FILE_CONFIG));
        }

        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();

        connector.start(sinkProperties);
        assertEquals(FileStreamSinkTask.class, connector.getTaskClass());

        PowerMock.verifyAll();
    }
}
