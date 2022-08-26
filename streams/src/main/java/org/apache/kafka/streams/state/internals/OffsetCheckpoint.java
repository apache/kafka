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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * This class saves out a map of topic/partition=&gt;offsets to a file. The format of the file is UTF-8 text containing the following:
 * <pre>
 *   &lt;version&gt;
 *   &lt;n&gt;
 *   &lt;topic_name_1&gt; &lt;partition_1&gt; &lt;offset_1&gt;
 *   .
 *   .
 *   .
 *   &lt;topic_name_n&gt; &lt;partition_n&gt; &lt;offset_n&gt;
 * </pre>
 *   The first line contains a number designating the format version (currently 0), the get line contains
 *   a number giving the total number of offsets. Each successive line gives a topic/partition/offset triple
 *   separated by spaces.
 */
public class OffsetCheckpoint {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetCheckpoint.class);

    private static final Pattern WHITESPACE_MINIMUM_ONCE = Pattern.compile("\\s+");

    private static final int VERSION = 0;

    // Use a negative sentinel when we don't know the offset instead of skipping it to distinguish it from dirty state
    // and use -4 as the -1 sentinel may be taken by some producer errors and -2 in the
    // subscription means that the state is used by an active task and hence caught-up and
    // -3 is also used in the subscription.
    public static final long OFFSET_UNKNOWN = -4L;

    private final File file;
    private final Object lock;

    public OffsetCheckpoint(final File file) {
        this.file = file;
        lock = new Object();
    }

    /**
     * Write the given offsets to the checkpoint file. All offsets should be non-negative.
     *
     * @throws IOException if any file operation fails with an IO exception
     */
    public void write(final Map<TopicPartition, Long> offsets) throws IOException {
        // if there are no offsets, skip writing the file to save disk IOs
        // but make sure to delete the existing file if one exists
        if (offsets.isEmpty()) {
            Utils.delete(file);
            return;
        }

        synchronized (lock) {
            // write to temp file and then swap with the existing file
            final File temp = new File(file.getAbsolutePath() + ".tmp");
            LOG.trace("Writing tmp checkpoint file {}", temp.getAbsolutePath());

            final FileOutputStream fileOutputStream = new FileOutputStream(temp);
            try (final BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
                writeIntLine(writer, VERSION);
                writeIntLine(writer, offsets.size());

                for (final Map.Entry<TopicPartition, Long> entry : offsets.entrySet()) {
                    final TopicPartition tp = entry.getKey();
                    final Long offset = entry.getValue();
                    if (isValid(offset)) {
                        writeEntry(writer, tp, offset);
                    } else {
                        LOG.error("Received offset={} to write to checkpoint file for {}", offset, tp);
                        throw new IllegalStateException("Attempted to write a negative offset to the checkpoint file");
                    }
                }

                writer.flush();
                fileOutputStream.getFD().sync();
            }

            LOG.trace("Swapping tmp checkpoint file {} {}", temp.toPath(), file.toPath());
            Utils.atomicMoveWithFallback(temp.toPath(), file.toPath());
        }
    }

    /**
     * @throws IOException if file write operations failed with any IO exception
     */
    static void writeIntLine(final BufferedWriter writer,
                             final int number) throws IOException {
        writer.write(Integer.toString(number));
        writer.newLine();
    }

    /**
     * @throws IOException if file write operations failed with any IO exception
     */
    static void writeEntry(final BufferedWriter writer,
                           final TopicPartition part,
                           final long offset) throws IOException {
        writer.write(part.topic());
        writer.write(' ');
        writer.write(Integer.toString(part.partition()));
        writer.write(' ');
        writer.write(Long.toString(offset));
        writer.newLine();
    }


    /**
     * Reads the offsets from the local checkpoint file, skipping any negative offsets it finds.
     *
     * @throws IOException if any file operation fails with an IO exception
     * @throws IllegalArgumentException if the offset checkpoint version is unknown
     */
    public Map<TopicPartition, Long> read() throws IOException {
        synchronized (lock) {
            try (final BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                final int version = readInt(reader);
                switch (version) {
                    case 0:
                        int expectedSize = readInt(reader);
                        final Map<TopicPartition, Long> offsets = new HashMap<>();
                        String line = reader.readLine();
                        while (line != null) {
                            final String[] pieces = WHITESPACE_MINIMUM_ONCE.split(line);
                            if (pieces.length != 3) {
                                throw new IOException(
                                    String.format("Malformed line in offset checkpoint file: '%s'.", line));
                            }

                            final String topic = pieces[0];
                            final int partition = Integer.parseInt(pieces[1]);
                            final TopicPartition tp = new TopicPartition(topic, partition);
                            final long offset = Long.parseLong(pieces[2]);
                            if (isValid(offset)) {
                                offsets.put(tp, offset);
                            } else {
                                LOG.warn("Read offset={} from checkpoint file for {}", offset, tp);
                                --expectedSize;
                            }

                            line = reader.readLine();
                        }
                        if (offsets.size() != expectedSize) {
                            throw new IOException(
                                String.format("Expected %d entries but found only %d", expectedSize, offsets.size()));
                        }
                        return offsets;

                    default:
                        throw new IllegalArgumentException("Unknown offset checkpoint version: " + version);
                }
            } catch (final NoSuchFileException e) {
                return Collections.emptyMap();
            }
        }
    }

    /**
     * @throws IOException if file read ended prematurely
     */
    private int readInt(final BufferedReader reader) throws IOException {
        final String line = reader.readLine();
        if (line == null) {
            throw new EOFException("File ended prematurely.");
        }
        return Integer.parseInt(line);
    }

    /**
     * @throws IOException if there is any IO exception during delete
     */
    public void delete() throws IOException {
        Files.deleteIfExists(file.toPath());
    }

    @Override
    public String toString() {
        return file.getAbsolutePath();
    }

    private boolean isValid(final long offset) {
        return offset >= 0L || offset == OFFSET_UNKNOWN;
    }

}
