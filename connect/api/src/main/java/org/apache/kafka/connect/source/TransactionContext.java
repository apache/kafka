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
package org.apache.kafka.connect.source;

/**
 * Provided to source tasks to allow them to define their own producer transaction boundaries when
 * exactly-once support is enabled.
 */
public interface TransactionContext {

    /**
     * Request a transaction commit after the next batch of records from {@link SourceTask#poll()}
     * is processed.
     */
    void commitTransaction();

    /**
     * Request a transaction commit after a source record is processed. The source record will be the
     * last record in the committed transaction.
     * <p>
     * If a task requests that the last record in a batch that it returns from {@link SourceTask#poll()}
     * be committed by invoking this method, and also requests that that same batch be aborted by
     * invoking {@link #abortTransaction()}, the record-based operation (in this case, committing
     * the transaction) will take precedence.
     * @param record the record to commit the transaction after; may not be null.
     */
    void commitTransaction(SourceRecord record);

    /**
     * Requests a transaction abort after the next batch of records from {@link SourceTask#poll()}. All of
     * the records in that transaction will be discarded and will not appear in a committed transaction.
     * However, offsets for that transaction will still be committed so than the records in that transaction
     * are not reprocessed. If the data should instead be reprocessed, the task should not invoke this method
     * and should instead throw an exception.
     */
    void abortTransaction();

    /**
     * Requests a transaction abort after a source record is processed. The source record will be the
     * last record in the aborted transaction. All of the records in that transaction will be discarded
     * and will not appear in a committed transaction. However, offsets for that transaction will still
     * be committed so that the records in that transaction are not reprocessed. If the data should be
     * reprocessed, the task should not invoke this method and should instead throw an exception.
     * <p>
     * If a task requests that the last record in a batch that it returns from {@link SourceTask#poll()}
     * be aborted by invoking this method, and also requests that that same batch be committed by
     * invoking {@link #commitTransaction()}, the record-based operation (in this case, aborting
     * the transaction) will take precedence.
     * @param record the record to abort the transaction after; may not be null.
     */
    void abortTransaction(SourceRecord record);
}
