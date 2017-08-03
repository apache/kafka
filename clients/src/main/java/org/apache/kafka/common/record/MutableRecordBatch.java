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
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.ByteBufferOutputStream;

/**
 * A mutable record batch is one that can be modified in place (without copying). This is used by the broker
 * to override certain fields in the batch before appending it to the log.
 */
public interface MutableRecordBatch extends RecordBatch {

    /**
     * Set the last offset of this batch.
     * @param offset The last offset to use
     */
    void setLastOffset(long offset);

    /**
     * Set the max timestamp for this batch. When using log append time, this effectively overrides the individual
     * timestamps of all the records contained in the batch. To avoid recompression, the record fields are not updated
     * by this method, but clients ignore them if the timestamp time is log append time. Note that firstTimestamp is not
     * updated by this method.
     *
     * This typically requires re-computation of the batch's CRC.
     *
     * @param timestampType The timestamp type
     * @param maxTimestamp The maximum timestamp
     */
    void setMaxTimestamp(TimestampType timestampType, long maxTimestamp);

    /**
     * Set the partition leader epoch for this batch of records.
     * @param epoch The partition leader epoch to use
     */
    void setPartitionLeaderEpoch(int epoch);

    /**
     * Write this record batch into an output stream.
     * @param outputStream The buffer to write the batch to
     */
    void writeTo(ByteBufferOutputStream outputStream);

}
