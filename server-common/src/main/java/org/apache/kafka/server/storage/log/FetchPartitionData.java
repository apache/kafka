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
package org.apache.kafka.server.storage.log;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.Records;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public class FetchPartitionData {
    public final Errors error;
    public final long highWatermark;
    public final long logStartOffset;
    public final Records records;
    public final Optional<FetchResponseData.EpochEndOffset> divergingEpoch;
    public final OptionalLong lastStableOffset;
    public final Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions;
    public final OptionalInt preferredReadReplica;
    public final boolean isReassignmentFetch;

  public FetchPartitionData(Errors error,
                              long highWatermark,
                              long logStartOffset,
                              Records records,
                              Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
                              OptionalLong lastStableOffset,
                              Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions,
                              OptionalInt preferredReadReplica,
                              boolean isReassignmentFetch) {
        this.error = error;
        this.highWatermark = highWatermark;
        this.logStartOffset = logStartOffset;
        this.records = records;
        this.divergingEpoch = divergingEpoch;
        this.lastStableOffset = lastStableOffset;
        this.abortedTransactions = abortedTransactions;
        this.preferredReadReplica = preferredReadReplica;
        this.isReassignmentFetch = isReassignmentFetch;
    }

    @Override
    public String toString() {
        Iterator<? extends RecordBatch> i = records.batches().iterator();
        RecordBatch batch = null;
        while (i.hasNext()) {
            batch = i.next();
        }
        return "FetchPartitionData{" +
            "error=" + error +
            ", highWatermark=" + highWatermark +
            ", logStartOffset=" + logStartOffset +
            ", records=" + records +
            ", records first offset=" + records.batches().iterator().next().baseOffset() +
            ", records first batch last offset=" + records.batches().iterator().next().lastOffset() +
            ", records last batch first offset=" + (batch != null ? batch.baseOffset() + "" : null) +
            ", records last offset=" + (batch != null ? batch.lastOffset() + "" : null) +
            ", divergingEpoch=" + divergingEpoch +
            ", lastStableOffset=" + lastStableOffset +
            ", abortedTransactions=" + abortedTransactions +
            ", preferredReadReplica=" + preferredReadReplica +
            ", isReassignmentFetch=" + isReassignmentFetch +
            '}';
    }
}
