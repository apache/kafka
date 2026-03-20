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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.connect.errors.ConnectException;

/**
 * Thrown when a gap is detected between MM2's last replicated offset
 * and the broker's earliest available offset, indicating messages were
 * permanently lost due to log retention before replication completed.
 *
 * This is intentionally fail-fast. Silent data loss in a Write-Ahead Log
 * replication scenario corrupts the DR cluster's state in ways that may
 * not be observable for a long time. A failed task is immediately visible
 * via the Connect REST API and operator dashboards; a silent skip is not.
 *
 * Extends ConnectException so the Connect framework automatically marks
 * the task as FAILED and surfaces the error in task status.
 */
public class LogTruncationException extends ConnectException {

    private final String topic;
    private final int    partition;
    private final long   lastReplicatedOffset;
    private final long   earliestAvailableOffset;

    public LogTruncationException(String topic, int partition,
                                  long lastReplicatedOffset,
                                  long earliestAvailableOffset) {
        super(String.format(
            "Log truncation detected on %s-%d: "
            + "last replicated offset=%d, broker earliest offset=%d, "
            + "approximately %d messages lost permanently.",
            topic, partition,
            lastReplicatedOffset,
            earliestAvailableOffset,
            earliestAvailableOffset - lastReplicatedOffset - 1
        ));
        this.topic                   = topic;
        this.partition               = partition;
        this.lastReplicatedOffset    = lastReplicatedOffset;
        this.earliestAvailableOffset = earliestAvailableOffset;
    }

    public String getTopic()                   { return topic; }
    public int    getPartition()               { return partition; }
    public long   getLastReplicatedOffset()    { return lastReplicatedOffset; }
    public long   getEarliestAvailableOffset() { return earliestAvailableOffset; }
}
