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

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class FileStreamSinkTaskTest {

    private FileStreamSinkTask task;
    private ByteArrayOutputStream os;
    private PrintStream printStream;

    @Before
    public void setup() {
        os = new ByteArrayOutputStream();
        printStream = new PrintStream(os);
        task = new FileStreamSinkTask(printStream);
    }

    @Test
    public void testPutFlush() {
        HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        final String newLine = System.getProperty("line.separator"); 

        // We do not call task.start() since it would override the output stream

        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line1", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(1L));
        task.flush(offsets);
        assertEquals("line1" + newLine, os.toString());

        task.put(Arrays.asList(
                new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, "line2", 2),
                new SinkRecord("topic2", 0, null, null, Schema.STRING_SCHEMA, "line3", 1)
        ));
        offsets.put(new TopicPartition("topic1", 0), new OffsetAndMetadata(2L));
        offsets.put(new TopicPartition("topic2", 0), new OffsetAndMetadata(1L));
        task.flush(offsets);
        assertEquals("line1" + newLine + "line2" + newLine + "line3" + newLine, os.toString());
    }
}
