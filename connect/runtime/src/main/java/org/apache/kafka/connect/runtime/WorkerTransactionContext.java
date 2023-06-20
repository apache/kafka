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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link TransactionContext} that can be given to tasks and then queried by the worker to check on
 * requests to abort and commit transactions. This class is thread safe and is designed to accommodate
 * concurrent use without external synchronization.
 */
public class WorkerTransactionContext implements TransactionContext {

    private static final Logger log = LoggerFactory.getLogger(WorkerTransactionContext.class);

    private final Set<SourceRecord> committableRecords = new HashSet<>();
    private final Set<SourceRecord> abortableRecords = new HashSet<>();
    private boolean batchCommitRequested = false;
    private boolean batchAbortRequested = false;

    @Override
    public synchronized void commitTransaction() {
        batchCommitRequested = true;
    }

    @Override
    public synchronized void commitTransaction(SourceRecord record) {
        Objects.requireNonNull(record, "Source record used to define transaction boundaries may not be null");
        committableRecords.add(record);
    }

    @Override
    public synchronized void abortTransaction() {
        batchAbortRequested = true;
    }

    @Override
    public synchronized void abortTransaction(SourceRecord record) {
        Objects.requireNonNull(record, "Source record used to define transaction boundaries may not be null");
        abortableRecords.add(record);
    }

    public synchronized boolean shouldCommitBatch() {
        checkBatchRequestsConsistency();
        boolean result = batchCommitRequested;
        batchCommitRequested = false;
        return result;
    }

    public synchronized boolean shouldAbortBatch() {
        checkBatchRequestsConsistency();
        boolean result = batchAbortRequested;
        batchAbortRequested = false;
        return result;
    }

    public synchronized boolean shouldCommitOn(SourceRecord record) {
        // We could perform this check in the connector-facing methods (such as commitTransaction(SourceRecord)),
        // but the connector might swallow that exception.
        // This way, we can fail the task unconditionally, which is warranted since the alternative may lead to data loss.
        // Essentially, instead of telling the task that it screwed up and trusting it to do the right thing, we rat on it to the
        // worker and let it get punished accordingly.
        checkRecordRequestConsistency(record);
        return committableRecords.remove(record);
    }

    public synchronized boolean shouldAbortOn(SourceRecord record) {
        checkRecordRequestConsistency(record);
        return abortableRecords.remove(record);
    }

    private void checkBatchRequestsConsistency() {
        if (batchCommitRequested && batchAbortRequested) {
            throw new IllegalStateException("Connector requested both commit and abort of same transaction");
        }
    }

    private void checkRecordRequestConsistency(SourceRecord record) {
        if (committableRecords.contains(record) && abortableRecords.contains(record)) {
            log.trace("Connector will fail as it has requested both commit and abort of transaction for same record: {}", record);
            throw new IllegalStateException(String.format(
                    "Connector requested both commit and abort of same record against topic/partition %s/%s",
                    record.topic(), record.kafkaPartition()
            ));
        }
    }

}
