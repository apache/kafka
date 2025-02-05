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
package org.apache.kafka.server;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.internals.log.AsyncOffsetReadFutureHolder;
import org.apache.kafka.storage.internals.log.OffsetResultHolder.FileRecordsOrError;

import java.util.Optional;

public class ListOffsetsPartitionStatus {
    
    private final Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt;
    private final Optional<Long> lastFetchableOffset;
    private final Optional<ApiException> maybeOffsetsError;

    private volatile Optional<ListOffsetsPartitionResponse> responseOpt = Optional.empty();
    private volatile boolean completed = false;

    private ListOffsetsPartitionStatus(
        Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt,
        Optional<Long> lastFetchableOffset,
        Optional<ApiException> maybeOffsetsError
    ) {
        this.futureHolderOpt = futureHolderOpt;
        this.lastFetchableOffset = lastFetchableOffset;
        this.maybeOffsetsError = maybeOffsetsError;
    }

    public static ListOffsetsPartitionStatus build(
        Optional<ListOffsetsPartitionResponse> responseOpt,
        Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt,
        Optional<Long> lastFetchableOffset,
        Optional<ApiException> maybeOffsetsError
    ) {
        ListOffsetsPartitionStatus status = new ListOffsetsPartitionStatus(
            futureHolderOpt,
            lastFetchableOffset,
            maybeOffsetsError
        );
        status.responseOpt = responseOpt;
        return status;
    }

    public Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt() {
        return futureHolderOpt;
    }

    public Optional<Long> lastFetchableOffset() {
        return lastFetchableOffset;
    }

    public Optional<ApiException> maybeOffsetsError() {
        return maybeOffsetsError;
    }

    public void responseOpt(Optional<ListOffsetsPartitionResponse> responseOpt) {
        this.responseOpt = responseOpt;
    }

    public Optional<ListOffsetsPartitionResponse> responseOpt() {
        return responseOpt;
    }
    
    public void completed(boolean completed) {
        this.completed = completed;
    }

    public boolean completed() {
        return completed;
    }

    @Override
    public String toString() {
        return String.format("[responseOpt: %s, lastFetchableOffset: %s, " +
                        "maybeOffsetsError: %s, completed: %s]",
                responseOpt, lastFetchableOffset, maybeOffsetsError, completed);
    }
}
