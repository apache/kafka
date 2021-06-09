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
import java.util.List;

public interface SnapshotWriter<T> extends AutoCloseable {
    /**
     * @return the end offset for the snapshot.
     */
    long endOffset();

    /**
     * Write a batch of records.  The writer may do more batching than is
     * requested-- in other words, small batches may be coalesced into large
     * ones, if that is convenient.
     *
     * @param records   The batch of records.
     *
     * @return          True if the data was received; false if the writer is
     *                  not ready for the data yet.  If the writer returns false
     *                  the snapshot generator will try again after a delay.
     */
    boolean append(List<T> records);

    /**
     * Invoked when the snapshot writer is no longer needed.  This should clean
     * up all writer resources.  If this is called prior to completing the snapshot,
     * any partial snapshot on disk should be deleted.
     */
    void close();

    /**
     * Freezes the snapshot by flushing all pending writes and marking it as immutable.
     */
    void freeze() throws IOException;
}
