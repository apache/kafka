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
package org.apache.kafka.snapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import org.apache.kafka.raft.OffsetAndEpoch;

final class Snapshots {
    private static final String SUFFIX =  ".checkpoint";
    private static final String PARTIAL_SUFFIX = String.format("%s.part", SUFFIX);

    private static final NumberFormat OFFSET_FORMATTER = NumberFormat.getInstance();
    private static final NumberFormat EPOCH_FORMATTER = NumberFormat.getInstance();

    static {
        OFFSET_FORMATTER.setMinimumIntegerDigits(20);
        OFFSET_FORMATTER.setGroupingUsed(false);

        EPOCH_FORMATTER.setMinimumIntegerDigits(10);
        EPOCH_FORMATTER.setGroupingUsed(false);
    }

    static Path snapshotDir(Path logDir) {
        return logDir;
    }

    static Path snapshotPath(Path logDir, OffsetAndEpoch snapshotId) {
        return snapshotDir(logDir).resolve(filenameFromSnapshotId(snapshotId) + SUFFIX);
    }

    static String filenameFromSnapshotId(OffsetAndEpoch snapshotId) {
        return String.format("%s-%s", OFFSET_FORMATTER.format(snapshotId.offset), EPOCH_FORMATTER.format(snapshotId.epoch));
    }

    static Path moveRename(Path source, OffsetAndEpoch snapshotId) {
        return source.resolveSibling(filenameFromSnapshotId(snapshotId) + SUFFIX);
    }

    static Path createTempFile(Path logDir, OffsetAndEpoch snapshotId) throws IOException {
        Path dir = snapshotDir(logDir);

        // Create the snapshot directory if it doesn't exists
        Files.createDirectories(dir);

        String prefix = String.format("%s-", filenameFromSnapshotId(snapshotId));

        return Files.createTempFile(dir, prefix, PARTIAL_SUFFIX);
    }
}
