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
package org.apache.kafka.storage.internals.checkpoint;

import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * This class represents a utility to capture a checkpoint in a file. It writes down to the file in the below format.
 * <pre>
 * ========= File beginning =========
 * version: int
 * entries-count: int
 * entry-as-string-on-each-line
 * ========= File end ===============
 * </pre>
 * Each entry is represented as a string on each line in the checkpoint file. {@link EntryFormatter} is used
 * to convert the entry into a string and vice versa.
 *
 * @param <T> entry type.
 */
public class CheckpointFile<T> {
    private static final Logger log = LoggerFactory.getLogger(CheckpointFile.class);

    public final File file;
    private final int version;
    private final EntryFormatter<T> formatter;
    private final LogDirFailureChannel logDirFailureChannel;
    private final String logDir;
    private final Object lock = new Object();
    private final Path absolutePath;
    private final Path tempPath;

    public CheckpointFile(File file,
                          int version,
                          EntryFormatter<T> formatter,
                          LogDirFailureChannel logDirFailureChannel,
                          String logDir) throws IOException {
        this.file = file;
        this.version = version;
        this.formatter = formatter;
        this.logDirFailureChannel = logDirFailureChannel;
        this.logDir = logDir;
        try {
            Files.createFile(file.toPath());
        } catch (FileAlreadyExistsException ex) {
            // Ignore if file already exists.
        }
        absolutePath = file.toPath().toAbsolutePath();
        tempPath = Paths.get(absolutePath + ".tmp");
    }

    public void write(Collection<T> entries) {
        try {
            writeInternal(entries);
        } catch (IOException e) {
            throw handleFailure("writing to", e);
        }
    }

    public void writeIfDirExists(Collection<T> entries) {
        try {
            writeInternal(entries);
        } catch (FileNotFoundException | NoSuchFileException e) {
            log.warn("Failed to write to checkpoint file {}. This is ok if the topic/partition is being deleted", file.getAbsolutePath(), e);
        } catch (IOException e) {
            throw handleFailure("writing to", e);
        }
    }

    public List<T> read() {
        try {
            synchronized (lock) {
                try (BufferedReader reader = Files.newBufferedReader(absolutePath, StandardCharsets.UTF_8)) {
                    return read(absolutePath.toString(), reader, version, formatter);
                }
            }
        } catch (IOException e) {
            throw handleFailure("reading", e);
        }
    }

    public static <T> void write(BufferedWriter writer,
                                 int version,
                                 EntryFormatter<T> formatter,
                                 Collection<T> entries) throws IOException {
        writer.write(Integer.toString(version));
        writer.newLine();
        writer.write(Integer.toString(entries.size()));
        writer.newLine();
        for (T entry : entries) {
            writer.write(formatter.toString(entry));
            writer.newLine();
        }
    }

    public static <T> List<T> read(String location,
                                   BufferedReader reader,
                                   int version,
                                   EntryFormatter<T> formatter) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return List.of();
        }

        int readVersion = toInt(location, line);
        if (readVersion != version) {
            throw new IOException("Unrecognised version:" + readVersion + ", expected version: " + version
                    + " in checkpoint file at: " + location);
        }

        line = reader.readLine();
        if (line == null) {
            return List.of();
        }
        int expectedSize = toInt(location, line);
        List<T> entries = new ArrayList<>(expectedSize);
        line = reader.readLine();
        while (line != null) {
            Optional<T> maybeEntry = formatter.fromString(line);
            if (maybeEntry.isEmpty()) {
                throw buildMalformedLineException(location, line);
            }
            entries.add(maybeEntry.get());
            line = reader.readLine();
        }

        if (entries.size() != expectedSize) {
            throw new IOException("Expected [" + expectedSize + "] entries in checkpoint file ["
                    + location + "], but found only [" + entries.size() + "]");
        }

        return entries;
    }

    private void writeInternal(Collection<T> entries) throws IOException {
        synchronized (lock) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(tempPath.toFile());
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
                write(writer, version, formatter, entries);
                writer.flush();
                fileOutputStream.getFD().sync();
            }
            Utils.atomicMoveWithFallback(tempPath, absolutePath);
        }
    }

    private KafkaStorageException handleFailure(String operation, IOException e) {
        String msg = "Error while " + operation + " checkpoint file " + file.getAbsolutePath();
        logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
        return new KafkaStorageException(msg, e);
    }

    private static int toInt(String location, String line) throws IOException {
        try {
            return Integer.parseInt(line);
        } catch (NumberFormatException e) {
            throw buildMalformedLineException(location, line);
        }
    }

    private static IOException buildMalformedLineException(String location, String line) {
        return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line));
    }

    /**
     * This is used to convert the given entry of type {@code T} into a string and vice versa.
     *
     * @param <T> entry type
     */
    public interface EntryFormatter<T> {
        String toString(T entry);

        Optional<T> fromString(String value);
    }
}
