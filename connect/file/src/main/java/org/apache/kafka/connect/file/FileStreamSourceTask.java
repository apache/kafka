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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
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
    public  static final String POSITION_FIELD = "position";
    private static final Schema VALUE_SCHEMA = Schema.STRING_SCHEMA;

    private String filename;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;
    private int batchSize = FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE;

    private Long streamOffset;
    private boolean needExtendBuffer;
    private int nread = 0;
    private static int maxBufferSize = Integer.MAX_VALUE;

    @Override
    public String version() {
        return new FileStreamSourceConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
        filename = props.get(FileStreamSourceConnector.FILE_CONFIG);
        if (filename == null || filename.isEmpty()) {
            stream = System.in;
            // Tracking offset for stdin doesn't make sense
            streamOffset = null;
            reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
        }
        // Missing topic or parsing error is not possible because we've parsed the config in the
        // Connector
        topic = props.get(FileStreamSourceConnector.TOPIC_CONFIG);
        batchSize = Integer.parseInt(props.get(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        if (stream == null) {
            try {
                stream = Files.newInputStream(Paths.get(filename));
                Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(FILENAME_FIELD, filename));
                if (offset != null) {
                    Object lastRecordedOffset = offset.get(POSITION_FIELD);
                    if (lastRecordedOffset != null && !(lastRecordedOffset instanceof Long))
                        throw new ConnectException("Offset position is the incorrect type");
                    if (lastRecordedOffset != null) {
                        log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                        long skipLeft = (Long) lastRecordedOffset;
                        while (skipLeft > 0) {
                            try {
                                long skipped = stream.skip(skipLeft);
                                skipLeft -= skipped;
                            } catch (IOException e) {
                                log.error("Error while trying to seek to previous offset in file {}: ", filename, e);
                                throw new ConnectException(e);
                            }
                        }
                        log.debug("Skipped to offset {}", lastRecordedOffset);
                    }
                    streamOffset = (lastRecordedOffset != null) ? (Long) lastRecordedOffset : 0L;
                } else {
                    streamOffset = 0L;
                }
                reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                log.debug("Opened {} for reading", logFilename());
            } catch (NoSuchFileException e) {
                log.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", logFilename());
                synchronized (this) {
                    this.wait(1000);
                }
                return null;
            } catch (IOException e) {
                log.error("Error while trying to open file {}: ", filename, e);
                throw new ConnectException(e);
            }
        }

        // Unfortunately we can't just use readLine() because it blocks in an uninterruptible way.
        // Instead we have to manage splitting lines ourselves, using simple backoff when no new data
        // is available.
        try {
            final BufferedReader readerCopy;
            synchronized (this) {
                readerCopy = reader;
            }
            if (readerCopy == null)
                return null;

            ArrayList<SourceRecord> records = null;

            String line;
            do {
                line = extractLine();
                if (line != null) {
                    log.trace("Read a line from {}", logFilename());
                    if (records == null)
                        records = new ArrayList<>();
                    records.add(new SourceRecord(offsetKey(filename), offsetValue(streamOffset), topic, null,
                            null, null, VALUE_SCHEMA, line, System.currentTimeMillis()));

                    if (records.size() >= batchSize) {
                        return records;
                    }
                }
            } while (line != null);

            if (nread <= 0)
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

    /**
     * Extract a line from the character buffer, looking for \n and \r\n for line separator.
     * If found, shift buffer leftward.
     * If not found and the buffer is full expand the buffer until next read.
     */
    String extractLine() throws IOException {
        int until = -1, newStart = -1;

        if (needExtendBuffer) {
            expandBuffer();
            needExtendBuffer = false;
        }

        nread = 0;
        if (offset < buffer.length && reader.ready()) {

            nread = reader.read(buffer, offset, buffer.length - offset);
            log.trace("Read {} bytes from {}", nread, logFilename());
            if (nread > 0) {
                offset += nread;
            }
        }

        log.trace("offset {}", offset);
        for (int i = 0; i < offset; i++) {
            if (buffer[i] == '\n') {
                until = i;
                newStart = i + 1;
                break;
            } else if (buffer[i] == '\r') {
                // We need to check for \r\n, so we must skip this if we can't check the next char
                if (i + 1 >= offset)
                    return null;

                until = i;
                newStart = (buffer[i + 1] == '\n') ? i + 2 : i + 1;
                break;
            }
        }

        if (until != -1) {
            String result = new String(buffer, 0, until);
            System.arraycopy(buffer, newStart, buffer, 0, buffer.length - newStart);
            offset = offset - newStart;
            if (streamOffset != null)
                streamOffset += newStart;
            return result;
        } else {
            if (offset == buffer.length &&  buffer.length  < maxBufferSize) {
                needExtendBuffer = true;
            }
            return null;
        }
    }

    /**
     * Expand the internal character buffer, double the size if less than maximum value, otherwise use the maximum value.
     */
    private void expandBuffer()  {
        int newSize = (buffer.length > maxBufferSize / 2) ? maxBufferSize :  buffer.length * 2;
        char[] newBuf = new char[newSize];
        System.arraycopy(buffer, 0, newBuf, 0, buffer.length);
        buffer = newBuf;
        log.debug("internal buffer size expanded to {} ", buffer.length);
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                if (stream != null && stream != System.in) {
                    stream.close();
                    log.trace("Closed input stream");
                }
            } catch (IOException e) {
                log.error("Failed to close FileStreamSourceTask stream: ", e);
            }
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
}
