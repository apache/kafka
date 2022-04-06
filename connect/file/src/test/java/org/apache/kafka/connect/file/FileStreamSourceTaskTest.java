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

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileStreamSourceTaskTest {

    private static final String TOPIC = "test";

    private File tempFile;
    private Map<String, String> config;
    private OffsetStorageReader offsetStorageReader;
    private SourceTaskContext context;
    private FileStreamSourceTask task;

    @BeforeEach
    public void setup() throws IOException {
        tempFile = File.createTempFile("file-stream-source-task-test", null);
        config = new HashMap<>();
        config.put(FileStreamSourceConnector.FILE_CONFIG, tempFile.getAbsolutePath());
        config.put(FileStreamSourceConnector.TOPIC_CONFIG, TOPIC);
        config.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, String.valueOf(FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE));
        task = new FileStreamSourceTask(2);
        offsetStorageReader = mock(OffsetStorageReader.class);
        context = mock(SourceTaskContext.class);
        task.initialize(context);
    }

    @AfterEach
    public void teardown() {
        tempFile.delete();
    }

    @Test
    public void testNormalLifecycle() throws InterruptedException, IOException {
        expectOffsetLookupReturnNone();

        task.start(config);

        OutputStream os = Files.newOutputStream(tempFile.toPath());
        assertNull(task.poll());
        os.write("partial line".getBytes());
        os.flush();
        assertNull(task.poll());
        os.write(" finished\n".getBytes());
        os.flush();
        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(TOPIC, records.get(0).topic());
        assertEquals("partial line finished", records.get(0).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 22L), records.get(0).sourceOffset());
        assertNull(task.poll());

        // Different line endings, and make sure the final \r doesn't result in a line until we can
        // read the subsequent byte.
        os.write("line1\rline2\r\nline3\nline4\n\r".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(4, records.size());
        assertEquals("line1", records.get(0).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 28L), records.get(0).sourceOffset());
        assertEquals("line2", records.get(1).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(1).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 35L), records.get(1).sourceOffset());
        assertEquals("line3", records.get(2).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(2).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 41L), records.get(2).sourceOffset());
        assertEquals("line4", records.get(3).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(3).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 47L), records.get(3).sourceOffset());

        os.write("subsequent text".getBytes());
        os.flush();
        records = task.poll();
        assertEquals(1, records.size());
        assertEquals("", records.get(0).value());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.FILENAME_FIELD, tempFile.getAbsolutePath()), records.get(0).sourcePartition());
        assertEquals(Collections.singletonMap(FileStreamSourceTask.POSITION_FIELD, 48L), records.get(0).sourceOffset());

        os.close();
        task.stop();

        verifyAll();
    }

    @Test
    public void testBatchSize() throws IOException, InterruptedException {
        expectOffsetLookupReturnNone();

        config.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, "5000");
        task.start(config);

        OutputStream os = Files.newOutputStream(tempFile.toPath());
        writeTimesAndFlush(os, 10_000,
                "Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...\n".getBytes()
        );

        assertEquals(2, task.bufferSize());
        List<SourceRecord> records = task.poll();
        assertEquals(5000, records.size());
        assertEquals(128, task.bufferSize());

        records = task.poll();
        assertEquals(5000, records.size());
        assertEquals(128, task.bufferSize());

        os.close();
        task.stop();
        verifyAll();
    }

    @Test
    public void testBufferResize() throws IOException, InterruptedException {
        int batchSize = 1000;
        expectOffsetLookupReturnNone();

        config.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, Integer.toString(batchSize));
        task.start(config);

        OutputStream os = Files.newOutputStream(tempFile.toPath());

        assertEquals(2, task.bufferSize());
        writeAndAssertBufferSize(batchSize, os, "1\n".getBytes(), 2);
        writeAndAssertBufferSize(batchSize, os, "3 \n".getBytes(), 4);
        writeAndAssertBufferSize(batchSize, os, "7     \n".getBytes(), 8);
        writeAndAssertBufferSize(batchSize, os, "8      \n".getBytes(), 8);
        writeAndAssertBufferSize(batchSize, os, "9       \n".getBytes(), 16);

        byte[] bytes = new byte[1025];
        Arrays.fill(bytes, (byte) '*');
        bytes[bytes.length - 1] = '\n';
        writeAndAssertBufferSize(batchSize, os, bytes, 2048);
        writeAndAssertBufferSize(batchSize, os, "9       \n".getBytes(), 2048);
        os.close();
        task.stop();

        verifyAll();
    }

    private void writeAndAssertBufferSize(int batchSize, OutputStream os, byte[] bytes, int expectBufferSize)
            throws IOException, InterruptedException {
        writeTimesAndFlush(os, batchSize, bytes);
        List<SourceRecord> records = task.poll();
        assertEquals(batchSize, records.size());
        String expectedLine = new String(bytes, 0, bytes.length - 1); // remove \n
        for (SourceRecord record : records) {
            assertEquals(expectedLine, record.value());
        }
        assertEquals(expectBufferSize, task.bufferSize());
    }

    private void writeTimesAndFlush(OutputStream os, int times, byte[] line) throws IOException {
        for (int i = 0; i < times; i++) {
            os.write(line);
        }
        os.flush();
    }

    @Test
    public void testUsingSystemInputSourceOnMissingFile() throws InterruptedException {
        String data = "line\n";
        System.setIn(new ByteArrayInputStream(data.getBytes()));

        config.remove(FileStreamSourceConnector.FILE_CONFIG);
        task.start(config);

        List<SourceRecord> records = task.poll();
        assertEquals(1, records.size());
        assertEquals(TOPIC, records.get(0).topic());
        assertEquals("line", records.get(0).value());

        task.stop();
    }

    @Test
    public void testInvalidFile() throws InterruptedException {
        config.put(FileStreamSourceConnector.FILE_CONFIG, "bogusfilename");
        task.start(config);
        // Currently the task retries indefinitely if the file isn't found, but shouldn't return any data.
        for (int i = 0; i < 3; i++)
            assertNull(task.poll());
    }

    private void expectOffsetLookupReturnNone() {
        when(context.offsetStorageReader()).thenReturn(offsetStorageReader);
        when(offsetStorageReader.offset(anyMap())).thenReturn(null);
    }

    private void verifyAll() {
        verify(context).offsetStorageReader();
        verify(offsetStorageReader).offset(anyMap());
    }
}
