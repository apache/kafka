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
import org.apache.kafka.copycat.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * FileStreamSourceTask reads from stdin or a file.
 */
public class FileStreamSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(FileStreamSourceTask.class);

    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    private String topic = null;

    private Long streamOffset;

    @Override
    public void start(Properties props) {
        String filename = props.getProperty(FileStreamSourceConnector.FILE_CONFIG);
        if (filename == null) {
            stream = System.in;
            // Tracking offset for stdin doesn't make sense
            streamOffset = null;
        } else {
            try {
                stream = new FileInputStream(filename);
                Long lastRecordedOffset = (Long) context.getOffsetStorageReader().getOffset(null);
                if (lastRecordedOffset != null) {
                    log.debug("Found previous offset, trying to skip to file offset {}", lastRecordedOffset);
                    long skipLeft = lastRecordedOffset;
                    while (skipLeft > 0) {
                        try {
                            long skipped = stream.skip(skipLeft);
                            skipLeft -= skipped;
                        } catch (IOException e) {
                            log.error("Error while trying to seek to previous offset in file: ", e);
                            throw new CopycatException(e);
                        }
                    }
                    log.debug("Skipped to offset {}", lastRecordedOffset);
                }
                streamOffset = (lastRecordedOffset != null) ? lastRecordedOffset : 0L;
            } catch (FileNotFoundException e) {
                throw new CopycatException("Couldn't find file for FileStreamSourceTask: {}", e);
            }
        }
        topic = props.getProperty(FileStreamSourceConnector.TOPIC_CONFIG);
        if (topic == null)
            throw new CopycatException("ConsoleSourceTask config missing topic setting");
        reader = new BufferedReader(new InputStreamReader(stream));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
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

            int nread = 0;
            while (readerCopy.ready()) {
                nread = readerCopy.read(buffer, offset, buffer.length - offset);

                if (nread > 0) {
                    offset += nread;
                    if (offset == buffer.length) {
                        char[] newbuf = new char[buffer.length * 2];
                        System.arraycopy(buffer, 0, newbuf, 0, buffer.length);
                        buffer = newbuf;
                    }

                    String line;
                    do {
                        line = extractLine();
                        if (line != null) {
                            if (records == null)
                                records = new ArrayList<>();
                            records.add(new SourceRecord(null, streamOffset, topic, line));
                        }
                        new ArrayList<SourceRecord>();
                    } while (line != null);
                }
            }

            if (nread <= 0)
                Thread.sleep(1);

            return records;
        } catch (IOException e) {
            // Underlying stream was killed, probably as a result of calling stop. Allow to return
            // null, and driving thread will handle any shutdown if necessary.
        }
        return null;
    }

    private String extractLine() {
        int until = -1, newStart = -1;
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
            return null;
        }
    }

    @Override
    public void stop() {
        log.trace("Stopping");
        synchronized (this) {
            try {
                stream.close();
                log.trace("Closed input stream");
            } catch (IOException e) {
                log.error("Failed to close ConsoleSourceTask stream: ", e);
            }
            reader = null;
            stream = null;
        }
    }
}
