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

package org.apache.kafka.shell;

import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;


/**
 * A wrapper BatchReader which materializes all the Batches and accurately tracks the last contained offset.
 */
class TrackingBatchReader<T> implements BatchReader<T> {
    private final BatchReader<T> underlying;
    private final List<Batch<T>> batches;
    private final Iterator<Batch<T>> iter;

    TrackingBatchReader(BatchReader<T> underlying) {
        this.underlying = underlying;
        this.batches = new ArrayList<>();
        while (underlying.hasNext()) {
            batches.add(underlying.next());
        }
        this.iter = batches.iterator();
    }

    @Override
    public long baseOffset() {
        return underlying.baseOffset();
    }

    @Override
    public OptionalLong lastOffset() {
        if (batches.isEmpty()) {
            return OptionalLong.of(-1);
        } else {
            return OptionalLong.of(batches.get(batches.size() - 1).lastOffset());
        }
    }

    @Override
    public void close() {
        underlying.close();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public Batch<T> next() {
        return iter.next();
    }
}
