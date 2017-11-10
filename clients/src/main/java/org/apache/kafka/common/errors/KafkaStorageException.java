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
package org.apache.kafka.common.errors;

/**
 * Miscellaneous disk-related IOException occurred when handling a request.
 * Client should request metadata update and retry if the response shows KafkaStorageException
 *
 * Here are the guidelines on how to handle KafkaStorageException and IOException:
 *
 * 1) If the server has not finished loading logs, IOException does not need to be converted to KafkaStorageException
 * 2) After the server has finished loading logs, IOException should be caught and trigger LogDirFailureChannel.maybeAddOfflineLogDir()
 *    Then the IOException should either be swallowed and logged, or be converted and re-thrown as KafkaStorageException
 * 3) It is preferred for IOException to be caught in Log rather than in ReplicaManager or LogSegment.
 *
 */
public class KafkaStorageException extends InvalidMetadataException {

    private static final long serialVersionUID = 1L;

    public KafkaStorageException() {
        super();
    }

    public KafkaStorageException(String message) {
        super(message);
    }

    public KafkaStorageException(Throwable cause) {
        super(cause);
    }

    public KafkaStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
