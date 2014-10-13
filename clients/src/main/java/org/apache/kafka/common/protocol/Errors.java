/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.protocol;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.errors.*;


/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN(-1, new UnknownServerException("The server experienced an unexpected error when processing the request")),
    NONE(0, null),
    OFFSET_OUT_OF_RANGE(1, new OffsetOutOfRangeException("The requested offset is not within the range of offsets maintained by the server.")),
    CORRUPT_MESSAGE(2, new CorruptRecordException("The message contents does not match the message CRC or the message is otherwise corrupt.")),
    UNKNOWN_TOPIC_OR_PARTITION(3, new UnknownTopicOrPartitionException("This server does not host this topic-partition.")),
    // TODO: errorCode 4 for InvalidFetchSize
    LEADER_NOT_AVAILABLE(5, new LeaderNotAvailableException("There is no leader for this topic-partition as we are in the middle of a leadership election.")),
    NOT_LEADER_FOR_PARTITION(6, new NotLeaderForPartitionException("This server is not the leader for that topic-partition.")),
    REQUEST_TIMED_OUT(7, new TimeoutException("The request timed out.")),
    // TODO: errorCode 8, 9, 11
    MESSAGE_TOO_LARGE(10, new RecordTooLargeException("The request included a message larger than the max message size the server will accept.")),
    OFFSET_METADATA_TOO_LARGE(12, new OffsetMetadataTooLarge("The metadata field of the offset request was too large.")),
    NETWORK_EXCEPTION(13, new NetworkException("The server disconnected before a response was received.")),
    // TODO: errorCode 14, 15, 16
    INVALID_TOPIC_EXCEPTION(17, new InvalidTopicException("The request attempted to perform an operation on an invalid topic.")),
    RECORD_LIST_TOO_LARGE(18, new RecordBatchTooLargeException("The request included message batch larger than the configured segment size on the server.")),
    NOT_ENOUGH_REPLICAS(19, new NotEnoughReplicasException("Messages are rejected since there are fewer in-sync replicas than required.")),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20, new NotEnoughReplicasAfterAppendException("Messages are written to the log, but to fewer in-sync replicas than required."));
    private static Map<Class<?>, Errors> classToError = new HashMap<Class<?>, Errors>();
    private static Map<Short, Errors> codeToError = new HashMap<Short, Errors>();
    static {
        for (Errors error : Errors.values()) {
            codeToError.put(error.code(), error);
            if (error.exception != null)
                classToError.put(error.exception.getClass(), error);
        }

    }

    private final short code;
    private final ApiException exception;

    private Errors(int code, ApiException exception) {
        this.code = (short) code;
        this.exception = exception;
    }

    /**
     * An instance of the exception
     */
    public ApiException exception() {
        return this.exception;
    }

    /**
     * The error code for the exception
     */
    public short code() {
        return this.code;
    }

    /**
     * Throw the exception corresponding to this error if there is one
     */
    public void maybeThrow() {
        if (exception != null)
            throw this.exception;
    }

    /**
     * Throw the exception if there is one
     */
    public static Errors forCode(short code) {
        Errors error = codeToError.get(code);
        return error == null ? UNKNOWN : error;
    }

    /**
     * Return the error instance associated with this exception (or UKNOWN if there is none)
     */
    public static Errors forException(Throwable t) {
        Errors error = classToError.get(t.getClass());
        return error == null ? UNKNOWN : error;
    }
}
