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

import java.io.Serial;

/**
 * Thrown when MirrorMaker 2 determines that records which were never replicated have already been
 * removed from the source cluster, i.e. the offset the replication consumer wanted to read from is
 * below the log start offset of the source partition.
 *
 * <p>This is unrecoverable from the connector's point of view: the missing records cannot be
 * produced to the target cluster, so the task fails fast rather than silently skipping ahead to a
 * later offset and leaving an undetected gap in the replicated stream.
 *
 * <p>Only raised when {@link MirrorSourceConfig#OFFSET_VALIDATION_ENABLED} is set to {@code true}.
 */
public class DataLossException extends ConnectException {

    @Serial
    private static final long serialVersionUID = 1L;

    public DataLossException(String message) {
        super(message);
    }

    public DataLossException(String message, Throwable cause) {
        super(message, cause);
    }
}
