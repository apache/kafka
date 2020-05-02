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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);
    public static final String FILENAME_FIELD = "filename";
    public static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String filename;
    private String topic = null;
    private int batchSize = FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE;

    private final FileStreamBuffer fileStreamBuffer;

    public FileStreamSourceTask() {
        this.fileStreamBuffer = new FileStreamBuffer();
    }

    FileStreamSourceTask(FileStreamBuffer fileStreamBuffer) {
        this.fileStreamBuffer = fileStreamBuffer;
    }

    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
        fileStreamBuffer.setFilename(filename);
        // Missing topic or parsing error is not possible because we've parsed the config in the
        // Connector
        topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
        batchSize = Integer.parseInt(props.get(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (!fileStreamBuffer.ensureOpen(() -> context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename))))
            return null;

        // Unfortunately we can't just use readLine() because it blocks in an uninterruptible way.
        // Instead we have to manage splitting lines ourselves, using simple backoff when no new data
        // is available.
        try {

            ArrayList<SourceRecord> records = null;

            String line;
            do {
                line = fileStreamBuffer.extractLine();
                if (line != null) {
                    log.trace("Read a line from {}", logFilename());
                    if (records == null)
                        records = new ArrayList<>();
                    records.add(new SourceRecord(offsetKey(filename), offsetValue(fileStreamBuffer.streamOffset), topic, null,
                            null, null, VALUE_SCHEMA, line, System.currentTimeMillis()));

                    if (records.size() >= batchSize) {
                        return records;
                    }
                }
            } while (line != null);

            // wait for 1 second if reaching end of file and nothing been read
            if (fileStreamBuffer.nread <= 0 && records == null)
                synchronized (this) {
                    this.wait(1000);
                }

            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            fileStreamBuffer.close();
            this.notify();
        }
    }

    private Map<String, String> offsetKey(String filename) {
        return Collections.singletonMap(FILENAME_FIELD, filename);
    }

    private Map<String, Long> offsetValue(Long pos) {
        return Collections.singletonMap(POSITION_FIELD, pos);
    }

    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }

    interface OffsetStorageReader {
        Map<String, Object> getOffset();
    }

}
