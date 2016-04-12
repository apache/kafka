/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

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

    private static final int VERSION = 0;

    private final File file;
    private final Object lock;

    /**
     * @throws IOException
     */
    public OffsetCheckpoint(File file) throws IOException {
        this.file = file;
        this.lock = new Object();
    }

    /**
     * @throws IOException if any file operation fails with an IO exception
     */
    public void write(Map<TopicPartition, Long> offsets) throws IOException {
        synchronized (lock) {
            // write to temp file and then swap with the existing file
            File temp = new File(file.getAbsolutePath() + ".tmp");

            FileOutputStream fileOutputStream = new FileOutputStream(temp);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream));
            try {
                writeIntLine(writer, VERSION);
                writeIntLine(writer, offsets.size());

                for (Map.Entry<TopicPartition, Long> entry : offsets.entrySet())
                    writeEntry(writer, entry.getKey(), entry.getValue());

                writer.flush();
                fileOutputStream.getFD().sync();
            } finally {
                writer.close();
            }

            Utils.atomicMoveWithFallback(temp.toPath(), file.toPath());
        }
    }

    /**
     * @throws IOException if file write operations failed with any IO exception
     */
    private void writeIntLine(BufferedWriter writer, int number) throws IOException {
        writer.write(Integer.toString(number));
        writer.newLine();
    }

    /**
     * @throws IOException if file write operations failed with any IO exception
     */
    private void writeEntry(BufferedWriter writer, TopicPartition part, long offset) throws IOException {
        writer.write(part.topic());
        writer.write(' ');
        writer.write(Integer.toString(part.partition()));
        writer.write(' ');
        writer.write(Long.toString(offset));
        writer.newLine();
    }


    /**
     * @throws IOException if any file operation fails with an IO exception
     * @throws IllegalArgumentException if the offset checkpoint version is unknown
     */
    public Map<TopicPartition, Long> read() throws IOException {
        synchronized (lock) {
            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(file));
            } catch (FileNotFoundException e) {
                return Collections.emptyMap();
            }

            try {
                int version = readInt(reader);
                switch (version) {
                    case 0:
                        int expectedSize = readInt(reader);
                        Map<TopicPartition, Long> offsets = new HashMap<>();
                        String line = reader.readLine();
                        while (line != null) {
                            String[] pieces = line.split("\\s+");
                            if (pieces.length != 3)
                                throw new IOException(String.format("Malformed line in offset checkpoint file: '%s'.",
                                    line));

                            String topic = pieces[0];
                            int partition = Integer.parseInt(pieces[1]);
                            long offset = Long.parseLong(pieces[2]);
                            offsets.put(new TopicPartition(topic, partition), offset);
                            line = reader.readLine();
                        }
                        if (offsets.size() != expectedSize)
                            throw new IOException(String.format("Expected %d entries but found only %d",
                                expectedSize,
                                offsets.size()));
                        return offsets;

                    default:
                        throw new IllegalArgumentException("Unknown offset checkpoint version: " + version);
                }
            } finally {
                reader.close();
            }
        }
    }

    /**
     * @throws IOException if file read ended prematurely
     */
    private int readInt(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null)
            throw new EOFException("File ended prematurely.");
        return Integer.parseInt(line);
    }

    /**
     * @throws IOException if there is any IO exception during delete
     */
    public void delete() throws IOException {
        file.delete();
    }

    @Override
    public String toString() {
        return this.file.getAbsolutePath();
    }

}
