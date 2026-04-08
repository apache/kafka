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
package org.apache.kafka.server.purgatory;

import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.protocol.Errors;

public class DeleteRecordsPartitionStatus {
    private final long requiredOffset;
    private final DeleteRecordsPartitionResult responseStatus;
    private volatile boolean acksPending;

    public DeleteRecordsPartitionStatus(long requiredOffset, DeleteRecordsPartitionResult responseStatus) {
        this.requiredOffset = requiredOffset;
        this.responseStatus = responseStatus;
        this.acksPending = false;
    }

    public boolean acksPending() {
        return acksPending;
    }

    public void setAcksPending(boolean acksPending) {
        this.acksPending = acksPending;
    }


    public DeleteRecordsPartitionResult responseStatus() {
        return responseStatus;
    }

    public long requiredOffset() {
        return requiredOffset;
    }

    @Override
    public String toString() {
        return String.format("[acksPending: %b, error: %s, lowWatermark: %d, requiredOffset: %d]",
                acksPending, Errors.forCode(responseStatus.errorCode()).toString(), responseStatus.lowWatermark(),
                requiredOffset);
    }


}
