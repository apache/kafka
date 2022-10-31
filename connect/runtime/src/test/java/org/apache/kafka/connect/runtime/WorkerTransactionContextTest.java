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
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WorkerTransactionContextTest {

    private static final SourceRecord RECORD = new SourceRecord(null, null, "t", null, 0, null, null);

    private WorkerTransactionContext context = new WorkerTransactionContext();

    @Test
    public void shouldNotifyOfBatchCommit() {
        context.commitTransaction();
        assertFalse(context.shouldAbortBatch());
        assertFalse(context.shouldAbortOn(RECORD));
        assertFalse(context.shouldCommitOn(RECORD));
        assertTrue(context.shouldCommitBatch());
    }

    @Test
    public void shouldNotifyOfRecordCommit() {
        context.commitTransaction(RECORD);
        assertFalse(context.shouldAbortBatch());
        assertFalse(context.shouldAbortOn(RECORD));
        assertFalse(context.shouldCommitBatch());
        assertTrue(context.shouldCommitOn(RECORD));
    }

    @Test
    public void shouldNotifyOfBatchAbort() {
        context.abortTransaction();
        assertFalse(context.shouldAbortOn(RECORD));
        assertFalse(context.shouldCommitOn(RECORD));
        assertFalse(context.shouldCommitBatch());
        assertTrue(context.shouldAbortBatch());
    }

    @Test
    public void shouldNotifyOfRecordAbort() {
        context.abortTransaction(RECORD);
        assertFalse(context.shouldAbortBatch());
        assertFalse(context.shouldCommitOn(RECORD));
        assertFalse(context.shouldCommitBatch());
        assertTrue(context.shouldAbortOn(RECORD));
    }

    @Test
    public void shouldNotCommitBatchRepeatedly() {
        context.commitTransaction();
        assertTrue(context.shouldCommitBatch());
        assertFalse(context.shouldCommitBatch());
    }

    @Test
    public void shouldNotCommitRecordRepeatedly() {
        context.commitTransaction(RECORD);
        assertTrue(context.shouldCommitOn(RECORD));
        assertFalse(context.shouldCommitOn(RECORD));
    }

    @Test
    public void shouldNotAbortBatchRepeatedly() {
        context.abortTransaction();
        assertTrue(context.shouldAbortBatch());
        assertFalse(context.shouldAbortBatch());
    }

    @Test
    public void shouldNotAbortRecordRepeatedly() {
        context.abortTransaction(RECORD);
        assertTrue(context.shouldAbortOn(RECORD));
        assertFalse(context.shouldAbortOn(RECORD));
    }

    @Test
    public void shouldDisallowConflictingRequests() {
        context.commitTransaction();
        context.abortTransaction();
        assertThrows(IllegalStateException.class, context::shouldCommitBatch);
        assertThrows(IllegalStateException.class, context::shouldAbortBatch);

        context = new WorkerTransactionContext();
        context.commitTransaction(RECORD);
        context.abortTransaction(RECORD);
        assertThrows(IllegalStateException.class, () -> context.shouldCommitOn(RECORD));
        assertThrows(IllegalStateException.class, () -> context.shouldAbortOn(RECORD));
    }

}
