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

package org.apache.kafka.image.writer;

import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;


/**
 * Writes out a metadata image to a SnapshotWriter.
 */
public class RaftSnapshotWriter implements ImageWriter {
    private final SnapshotWriter<ApiMessageAndVersion> snapshotWriter;
    private final int batchSize;
    private List<ApiMessageAndVersion> records;
    private OptionalLong frozenSize = OptionalLong.empty();

    public RaftSnapshotWriter(
        SnapshotWriter<ApiMessageAndVersion> snapshotWriter,
        int batchSize
    ) {
        this.snapshotWriter = snapshotWriter;
        this.batchSize = batchSize;
        this.records = new ArrayList<>();
    }

    @Override
    public void write(ApiMessageAndVersion record) {
        if (records == null) throw new ImageWriterClosedException();
        records.add(record);
        if (records.size() >= batchSize) {
            snapshotWriter.append(records);
            records = new ArrayList<>();
        }
    }

    @Override
    public void close(boolean complete) {
        if (records == null) return;
        try {
            if (complete) {
                if (!records.isEmpty()) {
                    snapshotWriter.append(records);
                }
                frozenSize = OptionalLong.of(snapshotWriter.freeze());
            }
        } finally {
            records = null;
            snapshotWriter.close();
        }
    }

    /**
     * @return the frozen size of the snapshot, or OptionalLong.empty if the snapshot was not frozen.
     */
    public OptionalLong frozenSize() {
        return frozenSize;
    }
}
