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

import java.util.Iterator;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.OffsetAndEpoch;

/**
 * Interface of the snapshot reader
 */
public interface  SnapshotReader<T> extends AutoCloseable, Iterator<Batch<T>> {
    /**
     * Returns the end offset and epoch for the snapshot.
     */
    OffsetAndEpoch snapshotId();

    /**
     * Returns the last log offset which is represented in the snapshot.
     */
    long lastContainedLogOffset();

    /**
     * Returns the epoch of the last log offset which is represented in the snapshot.
     */
    int lastContainedLogEpoch();

    /**
     * Returns the timestamp of the last log offset which is represented in the snapshot.
     */
    long lastContainedLogTimestamp();

    /**
     * Closes the snapshot reader.
     */
    void close();
}
