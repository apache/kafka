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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.record.FileRecords.TimestampAndOffset;

import java.util.Objects;
import java.util.Optional;

public class OffsetResultHolder {

    private Optional<TimestampAndOffset> timestampAndOffsetOpt;
    private Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt;
    private Optional<ApiException> maybeOffsetsError = Optional.empty();
    private Optional<Long> lastFetchableOffset = Optional.empty();

    public OffsetResultHolder() {
        this(Optional.empty(), Optional.empty());
    }

    public OffsetResultHolder(
            Optional<TimestampAndOffset> timestampAndOffsetOpt,
            Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt
    ) {
        this.timestampAndOffsetOpt = timestampAndOffsetOpt;
        this.futureHolderOpt = futureHolderOpt;
    }

    public OffsetResultHolder(Optional<TimestampAndOffset> timestampAndOffsetOpt) {
        this(timestampAndOffsetOpt, Optional.empty());
    }

    public OffsetResultHolder(TimestampAndOffset timestampAndOffsetOpt) {
        this(Optional.of(timestampAndOffsetOpt), Optional.empty());
    }

    public Optional<TimestampAndOffset> timestampAndOffsetOpt() {
        return timestampAndOffsetOpt;
    }

    public Optional<AsyncOffsetReadFutureHolder<FileRecordsOrError>> futureHolderOpt() {
        return futureHolderOpt;
    }

    public Optional<ApiException> maybeOffsetsError() {
        return maybeOffsetsError;
    }

    public Optional<Long> lastFetchableOffset() {
        return lastFetchableOffset;
    }

    public void timestampAndOffsetOpt(Optional<TimestampAndOffset> timestampAndOffsetOpt) {
        this.timestampAndOffsetOpt = timestampAndOffsetOpt;
    }

    public void maybeOffsetsError(Optional<ApiException> maybeOffsetsError) {
        this.maybeOffsetsError = maybeOffsetsError;
    }

    public void lastFetchableOffset(Optional<Long> lastFetchableOffset) {
        this.lastFetchableOffset = lastFetchableOffset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OffsetResultHolder that = (OffsetResultHolder) o;
        return Objects.equals(timestampAndOffsetOpt, that.timestampAndOffsetOpt) && Objects.equals(futureHolderOpt, that.futureHolderOpt) && Objects.equals(maybeOffsetsError, that.maybeOffsetsError) && Objects.equals(lastFetchableOffset, that.lastFetchableOffset);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(timestampAndOffsetOpt);
        result = 31 * result + Objects.hashCode(futureHolderOpt);
        result = 31 * result + Objects.hashCode(maybeOffsetsError);
        result = 31 * result + Objects.hashCode(lastFetchableOffset);
        return result;
    }

    public static class FileRecordsOrError {
        private Optional<Exception> exception;
        private Optional<TimestampAndOffset> timestampAndOffset;

        public FileRecordsOrError(
                Optional<Exception> exception,
                Optional<TimestampAndOffset> timestampAndOffset
        ) {
            if (exception.isPresent() && timestampAndOffset.isPresent()) {
                throw new IllegalArgumentException("Either exception or timestampAndOffset should be present, but not both");
            }
            this.exception = exception;
            this.timestampAndOffset = timestampAndOffset;
        }

        public Optional<Exception> exception() {
            return exception;
        }

        public Optional<TimestampAndOffset> timestampAndOffset() {
            return timestampAndOffset;
        }
        
        public boolean hasException() {
            return exception.isPresent();
        }
        
        public boolean hasTimestampAndOffset() {
            return timestampAndOffset.isPresent();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FileRecordsOrError that = (FileRecordsOrError) o;
            return Objects.equals(exception, that.exception) && Objects.equals(timestampAndOffset, that.timestampAndOffset);
        }

        @Override
        public int hashCode() {
            int result = Objects.hashCode(exception);
            result = 31 * result + Objects.hashCode(timestampAndOffset);
            return result;
        }
    }
}
