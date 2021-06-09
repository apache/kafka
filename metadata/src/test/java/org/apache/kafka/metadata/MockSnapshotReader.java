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

package org.apache.kafka.metadata;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotReader;

import java.util.Iterator;
import java.util.List;


public class MockSnapshotReader implements SnapshotReader<ApiMessageAndVersion> {
    private final long endOffset;
    private final Iterator<List<ApiMessageAndVersion>> iterator;
    private long curOffset;

    public MockSnapshotReader(long endOffset,
                              Iterator<List<ApiMessageAndVersion>> iterator) {
        this.endOffset = endOffset;
        this.iterator = iterator;
        this.curOffset = 0;
    }

    @Override
    public long endOffset() {
        return endOffset;
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Batch<ApiMessageAndVersion> next() {
        List<ApiMessageAndVersion> records = iterator.next();
        long prevOffset = curOffset;
        curOffset += records.size();
        return Batch.of(prevOffset, 0, records);
    }
}
