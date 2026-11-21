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
package org.apache.kafka.connect.sink;

import org.apache.kafka.common.annotation.InterfaceAudience;

import java.util.Collection;

/**
 * Opt-in task contract for share-group consumption, requiring runtime support to enable that mode.
 * <p>
 * In share mode, {@link #initialize(SinkTaskContext)}, {@link #start(java.util.Map)},
 * {@link #put(Collection)} and {@link #stop()} are invoked serially. Records may be duplicated or
 * delivered out of order, and multiple tasks may write records from the same source partition.
 * Tasks must not depend on exclusive partition ownership or {@link #open(Collection)} and
 * {@link #close(Collection)} notifications.
 * <p>
 * Successful {@code put} calls durably complete all supplied records. Writes cannot be deferred to
 * {@link #flush(java.util.Map)} or {@link #preCommit(java.util.Map)} offset frontiers. Destination
 * writes and Kafka acknowledgements remain separate, so successful writes may be redelivered and
 * failed calls may leave partial destination effects.
 * <p>
 * Context offset changes, assignment lookup, partition pause/resume and errant-record reporting are
 * unavailable in share mode.
 */
@InterfaceAudience.Public
public abstract class ShareSinkTask extends SinkTask {

    /**
     * Durably handle every record supplied to this call before returning successfully.
     * If the call fails, all supplied records may be retried even when some destination writes succeeded.
     * <p>
     * The collection wrapper may be modified, but record objects and their reachable data must remain
     * unchanged so that retries observe the original input. Modifying the collection does not reduce
     * the set of records this call must complete.
     *
     * @param records the records to durably handle
     */
    @Override
    public abstract void put(Collection<SinkRecord> records);
}
