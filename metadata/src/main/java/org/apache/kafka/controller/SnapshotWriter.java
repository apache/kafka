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

package org.apache.kafka.controller;

import java.io.IOException;
import java.util.List;
import org.apache.kafka.metadata.ApiMessageAndVersion;


interface SnapshotWriter extends AutoCloseable {
    /**
     * @return          The epoch of the snapshot we are writing.
     **/
    long epoch();

    /**
     * Write a batch of records.  The writer may do more batching than is
     * requested-- in other words, small batches may be coalesced into large
     * ones, if that is convenient.
     *
     * @param batch     The batch of records.
     *
     * @return          True if the data was received; false if the writer is
     *                  not ready for the data yet.  If the writer returns false
     *                  the snapshot generator will try again after a delay.
     */
    boolean writeBatch(List<ApiMessageAndVersion> batch) throws IOException;

    /**
     * Invoked when the snapshot writer is no longer needed.  This should clean
     * up all writer resources.  If this is called prior to completing the snapshot,
     * any partial snapshot on disk should be deleted.
     */
    void close();

    /**
     * Invoked to finish writing the snapshot to disk.
     */
    void completeSnapshot() throws IOException;
}
