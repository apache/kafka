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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.apache.kafka.storage.internals.log.LogFileUtils.offsetFromFileName;

public class SnapshotFile {
    private static final Logger log = LoggerFactory.getLogger(SnapshotFile.class);

    public final long offset;
    private volatile File file;

    public SnapshotFile(File file) {
        this(file, offsetFromFileName(file.getName()));
    }

    public SnapshotFile(File file, long offset) {
        this.file = file;
        this.offset = offset;
    }

    public boolean deleteIfExists() throws IOException {
        boolean deleted = Files.deleteIfExists(file.toPath());
        if (deleted) {
            log.info("Deleted producer state snapshot {}", file.getAbsolutePath());
        } else {
            log.info("Failed to delete producer state snapshot {} because it does not exist.", file.getAbsolutePath());
        }
        return deleted;
    }

    public void updateParentDir(File parentDir) {
        String name = file.getName();
        file = new File(parentDir, name);
    }

    public File file() {
        return file;
    }

    public void renameTo(String newSuffix) throws IOException {
        File renamed = new File(Utils.replaceSuffix(file.getPath(), "", newSuffix));
        try {
            Utils.atomicMoveWithFallback(file.toPath(), renamed.toPath());
        } finally {
            file = renamed;
        }
    }

    @Override
    public String toString() {
        return "SnapshotFile(" +
                "offset=" + offset +
                ", file=" + file +
                ')';
    }
}