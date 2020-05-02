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

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Map;

/**
  This class handles internal input stream, buffered reader and character buffer for a FileStreamSourceTask
  @see FileStreamSourceTask
 */
class FileStreamBuffer {
    private static final Logger log = LoggerFactory.getLogger(FileStreamBuffer.class);
    private boolean needExtendBuffer = false;
    private String filename;
    private InputStream stream;
    private BufferedReader reader = null;
    private char[] buffer = new char[1024];
    private int offset = 0;
    static int maxBufferSize = Integer.MAX_VALUE;
    Long streamOffset;
    int nread;

    FileStreamBuffer(){};

    /**
     * Create the input stream and buffered reader using the file name provided,
     * if the file does not exist, wait 1 second and return false;
     * skip to the stored offset if there is one. error out if skip fail.
     */
    boolean ensureOpen(FileStreamSourceTask.OffsetStorageReader offsetStorageReader) throws InterruptedException {
        if (stream == null) {
            if (filename == null || filename.isEmpty()) {
                stream = System.in;
                // Tracking offset for stdin doesn't make sense
                streamOffset = null;
                reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
            } else {
                try {
                    stream = Files.newInputStream(Paths.get(filename));
                    reader = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
                    Map<String, Object> offset = offsetStorageReader.getOffset();

                    if (offset != null) {
                        Object lastRecordedOffset = offset.get(FileStreamSourceTask.POSITION_FIELD);
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
                                    close();
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
                    log.debug("Opened {} for reading", logFilename());
                } catch (NoSuchFileException e) {
                    log.warn("Couldn't find file {} for FileStreamSourceTask, sleeping to wait for it to be created", logFilename());
                    close();
                    synchronized (this) {
                        this.wait(1000);
                    }
                    return false;
                } catch (IOException e) {
                    close();
                    log.error("Error while trying to open file {}: ", filename, e);
                    throw new ConnectException(e);
                }
            }
        }
        return true;
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

    /**
     * Extract a line from the character buffer, looking for \n and \r\n for line separator.
     * If found, shift buffer left ward.
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
            // shift buffer left ward.
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
     * close the input stream if the task stopped
     */
    void close() {
        try {
            if (stream != null && stream != System.in) {
                stream.close();
                log.trace("Closed input stream");
            }
        } catch (IOException e) {
            log.error("Failed to close FileStreamSourceTask stream: ", e);
        } finally {
            stream = null;
            reader = null;
        }
    }

    void setFilename(String filename) {
        this.filename = filename;
    }

    private String logFilename() {
        return filename == null ? "stdin" : filename;
    }

    // following are created for unit testing purpose
    FileStreamBuffer(InputStream stream, BufferedReader reader) {
        this.stream = stream;
        this.reader = reader;
    }

    int _bufferSize() {
        return buffer.length;
    }

}
