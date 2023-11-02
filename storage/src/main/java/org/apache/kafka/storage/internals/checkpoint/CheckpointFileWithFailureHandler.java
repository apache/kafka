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
import org.apache.kafka.server.common.CheckpointFile;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CheckpointFileWithFailureHandler<T> {

    public final File file;
    private final LogDirFailureChannel logDirFailureChannel;
    private final String logDir;

    private final CheckpointFile<T> checkpointFile;

    public CheckpointFileWithFailureHandler(File file, int version, CheckpointFile.EntryFormatter<T> formatter,
                                            LogDirFailureChannel logDirFailureChannel, String logDir) throws IOException {
        this.file = file;
        this.logDirFailureChannel = logDirFailureChannel;
        this.logDir = logDir;
        checkpointFile = new CheckpointFile<>(file, version, formatter);
    }

    public void write(Collection<T> entries) {
        try {
            checkpointFile.write(entries);
        } catch (IOException e) {
            String msg = "Error while writing to checkpoint file " + file.getAbsolutePath();
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
            throw new KafkaStorageException(msg, e);
        }
    }

    public List<T> read() {
        try {
            return checkpointFile.read();
        } catch (IOException e) {
            String msg = "Error while reading checkpoint file " + file.getAbsolutePath();
            logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e);
            throw new KafkaStorageException(msg, e);
        }
    }
}
