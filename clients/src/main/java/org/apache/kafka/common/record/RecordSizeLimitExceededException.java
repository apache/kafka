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
package org.apache.kafka.common.record;

import org.apache.kafka.common.InvalidRecordException;

/**
 * Thrown when a record's declared (decompressed) body size is within the range the JVM can allocate but exceeds the
 * configured per-record maximum ({@code max.decompressed.message.bytes}). Unlike a plain
 * {@link InvalidRecordException}, this indicates a policy violation by an otherwise well-formed record (e.g. one that
 * was stored before the limit was lowered) rather than corruption, so callers such as fetch down-conversion can
 * handle it separately. It is an {@link InvalidRecordException}, so all other callers treat it as before.
 */
public class RecordSizeLimitExceededException extends InvalidRecordException {

    private static final long serialVersionUID = 1;

    public RecordSizeLimitExceededException(String message) {
        super(message);
    }

    /**
     * Build the exception for a record whose declared body size exceeds {@code maxRecordBodySize}. A size above
     * {@link Records#SOFT_MAX_ARRAY_LENGTH} can never belong to a real record, so it is reported as a plain
     * {@link InvalidRecordException} (corruption); otherwise a {@link RecordSizeLimitExceededException}.
     */
    static InvalidRecordException forSize(int sizeInBytes, int maxRecordBodySize) {
        String message = "Invalid record size: " + sizeInBytes +
            " exceeds the configured maximum record size of " + maxRecordBodySize + ".";
        if (sizeInBytes > Records.SOFT_MAX_ARRAY_LENGTH)
            return new InvalidRecordException(message);
        return new RecordSizeLimitExceededException(message);
    }
}
