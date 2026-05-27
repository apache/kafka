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

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.storage.internals.log.LeaderHwChange;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.RecordValidationStats;

import java.util.List;
import java.util.Optional;

/**
 * Result metadata of a log append operation on the log
 */
public record LogAppendResult(
    LogAppendSummary logAppendSummary,
    Optional<Throwable> exception,
    boolean hasCustomErrorMessage
) {

    public record LogAppendSummary(
        long firstOffset,
        long lastOffset,
        long logAppendTime,
        long logStartOffset,
        RecordValidationStats recordValidationStats,
        List<ProduceResponse.RecordError> recordErrors,
        LeaderHwChange leaderHwChange
    ) {
        public LogAppendSummary {
            recordErrors = List.copyOf(recordErrors);
            if (recordValidationStats == null) {
                recordValidationStats = RecordValidationStats.EMPTY;
            }
        }

        public static LogAppendSummary fromAppendInfo(LogAppendInfo info) {
            return new LogAppendSummary(
                info.firstOffset(),
                info.lastOffset(),
                info.logAppendTime(),
                info.logStartOffset(),
                info.recordValidationStats(),
                info.recordErrors(),
                info.leaderHwChange()
            );
        }
    }

    public Errors error() {
        return exception
                .map(Errors::forException)
                .orElse(Errors.NONE);
    }

    public String errorMessage() {
        return exception
                .filter(e -> hasCustomErrorMessage)
                .map(Throwable::getMessage)
                .orElse(null);
    }
}
