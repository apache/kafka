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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.raft.OffsetAndEpoch;

/**
 * Interface for writing snapshot as a sequence of records.
 */
public interface RawSnapshotWriter extends Closeable {
    /**
     * Returns the end offset and epoch for the snapshot.
     */
    public OffsetAndEpoch snapshotId();

    /**
     * Returns the number of bytes for the snapshot.
     *
     * @throws IOException for any IO error while reading the size
     */
    public long sizeInBytes() throws IOException;

    /**
     * Fully appends the buffer to the snapshot.
     *
     * If the method returns without an exception the given buffer was fully writing the
     * snapshot.
     *
     * @param buffer the buffer to append
     * @throws IOException for any IO error during append
     */
    public void append(ByteBuffer buffer) throws IOException;

    /**
     * Returns true if the snapshot has been frozen, otherwise false is returned.
     *
     * Modification to the snapshot are not allowed once it is frozen.
     */
    public boolean isFrozen();

    /**
     * Freezes the snapshot and marking it as immutable.
     *
     * @throws IOException for any IO error during freezing
     */
    public void freeze() throws IOException;

    /**
     * Closes the snapshot writer.
     *
     * If close is called without first calling freeze the the snapshot is aborted.
     *
     * @throws IOException for any IO error during close
     */
    public void close() throws IOException;
}
