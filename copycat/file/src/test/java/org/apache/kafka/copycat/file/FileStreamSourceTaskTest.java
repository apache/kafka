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

import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.source.SourceRecord;
import org.apache.kafka.copycat.source.SourceTaskContext;
import org.apache.kafka.copycat.storage.OffsetStorageReader;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class FileStreamSourceTaskTest {

    private static final String TOPIC = "test";

    private File tempFile;
    private Properties config;
    private OffsetStorageReader offsetStorageReader;
    private FileStreamSourceTask task;

    private boolean verifyMocks = false;

    @Before
    public void setup() throws IOException {
        tempFile = File.createTempFile("file-stream-source-task-test", null);
        config = new Properties();
        config.setProperty(FileStreamSourceConnector.FILE_CONFIG, tempFile.getAbsolutePath());
        config.setProperty(FileStreamSourceConnector.TOPIC_CONFIG, TOPIC);
        task = new FileStreamSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        task.initialize(new SourceTaskContext(offsetStorageReader));
    }

    @After
    public void teardown() {
        tempFile.delete();

        if (verifyMocks)
            PowerMock.verifyAll();
    }

    private void replay() {
        PowerMock.replayAll();
        verifyMocks = true;
    }

    @Test
    public void testNormalLifecycle() throws InterruptedException, IOException {
        expectOffsetLookupReturnNone();
        replay();

        task.start(config);

        FileOutputStream os = new FileOutputStream(tempFile);
        assertEquals(null, task.poll());
        os.write("partial line".getBytes());
        os.flush();
        assertEquals(null, task.poll());
        os.write(" finished\n".getBytes());
        os.flush();
        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(TOPIC, records.get(0).getTopic());
        assertEquals("partial line finished", records.get(0).getValue());
        assertEquals(22L, records.get(0).getSourceOffset());
        assertEquals(null, task.poll());

        // Different line endings, and make sure the final \r doesn't result in a line until we can
        // read the subsequent byte.
        os.write("line1\rline2\r\nline3\nline4\n\r".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(4, records.size());
        assertEquals("line1", records.get(0).getValue());
        assertEquals(28L, records.get(0).getSourceOffset());
        assertEquals("line2", records.get(1).getValue());
        assertEquals(35L, records.get(1).getSourceOffset());
        assertEquals("line3", records.get(2).getValue());
        assertEquals(41L, records.get(2).getSourceOffset());
        assertEquals("line4", records.get(3).getValue());
        assertEquals(47L, records.get(3).getSourceOffset());

        os.write("subsequent text".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(1, records.size());
        assertEquals("", records.get(0).getValue());
        assertEquals(48L, records.get(0).getSourceOffset());

        task.stop();
    }

    @Test(expected = CopycatException.class)
    public void testMissingTopic() {
        expectOffsetLookupReturnNone();
        replay();

        config.remove(FileStreamSourceConnector.TOPIC_CONFIG);
        task.start(config);
    }

    @Test(expected = CopycatException.class)
    public void testInvalidFile() {
        config.setProperty(FileStreamSourceConnector.FILE_CONFIG, "bogusfilename");
        task.start(config);
    }


    private void expectOffsetLookupReturnNone() {
        EasyMock.expect(
                offsetStorageReader.getOffset(EasyMock.anyObject(Object.class)))
                .andReturn(null);
    }
}