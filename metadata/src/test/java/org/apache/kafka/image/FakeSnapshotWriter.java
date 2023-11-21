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

package org.apache.kafka.image;

import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class FakeSnapshotWriter implements SnapshotWriter<ApiMessageAndVersion> {
    private final OffsetAndEpoch snapshotId;
    private final List<List<ApiMessageAndVersion>> batches = new ArrayList<>();
    private boolean frozen = false;
    private boolean closed = false;

    public List<List<ApiMessageAndVersion>> batches() {
        List<List<ApiMessageAndVersion>> result = new ArrayList<>();
        for (List<ApiMessageAndVersion> batch : batches) {
            result.add(Collections.unmodifiableList(batch));
        }
        return Collections.unmodifiableList(result);
    }

    public FakeSnapshotWriter() {
        this(new OffsetAndEpoch(100L, 10));
    }

    public FakeSnapshotWriter(OffsetAndEpoch snapshotId) {
        this.snapshotId = snapshotId;
    }

    @Override
    public OffsetAndEpoch snapshotId() {
        return snapshotId;
    }

    @Override
    public long lastContainedLogOffset() {
        return snapshotId().offset() - 1;
    }

    @Override
    public int lastContainedLogEpoch() {
        return snapshotId().epoch();
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public void append(List<ApiMessageAndVersion> batch) {
        if (frozen) {
            throw new IllegalStateException("Append not supported. Snapshot is already frozen.");
        }
        batches.add(batch);
    }

    @Override
    public long freeze() {
        frozen = true;
        return batches.size() * 100L;
    }

    @Override
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
}