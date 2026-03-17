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

package org.apache.kafka.server.share.dlq;

import java.util.concurrent.CompletableFuture;

/**
 * The main interface to identify implementations of dead letter queues for share groups.
 */
public interface ShareGroupDLQ {
    class ShareGroupDLQThrowable extends Throwable {
        ShareGroupDLQThrowable(String message) {
            // We don't want the stack trace to be filled.
            super(message, null, false, false);
        }
    }

    Throwable STALE_BATCH = new ShareGroupDLQThrowable("Offset part of stale batch.");
    Throwable BEHIND_LSO = new ShareGroupDLQThrowable("Offset before LSO.");
    Throwable ABORTED_TRANSACTION = new ShareGroupDLQThrowable("Offset part of aborted transaction.");
    Throwable CLIENT_REJECT = new ShareGroupDLQThrowable("Offset rejected by client.");
    Throwable DELIVERY_COUNT_EXCEEDED = new ShareGroupDLQThrowable("Offset delivery count exceeded the threshold.");
    Throwable ACQUISITION_LOCK_TIMEOUT = new ShareGroupDLQThrowable("Acquisition lock timed out.");

    /**
     * Main method exposed to the world to enqueuing a record to the share groups dead letter queue.
     *
     * @param param A java record encapsulating required and optional information about the kafka record
     *              being dead letter queued.
     * @return A completable future of Void type, mainly to signal exceptions.
     */
    CompletableFuture<Void> enqueue(ShareGroupDLQRecordParameter param);
}
