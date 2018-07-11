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
package org.apache.kafka.common.protocol;

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ConcurrentTransactionsException;
import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.CoordinatorLoadInProgressException;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.DuplicateSequenceException;
import org.apache.kafka.common.errors.DelegationTokenAuthorizationException;
import org.apache.kafka.common.errors.DelegationTokenDisabledException;
import org.apache.kafka.common.errors.DelegationTokenExpiredException;
import org.apache.kafka.common.errors.DelegationTokenNotFoundException;
import org.apache.kafka.common.errors.DelegationTokenOwnerMismatchException;
import org.apache.kafka.common.errors.ListenerNotFoundException;
import org.apache.kafka.common.errors.FetchSessionIdNotFoundException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.InvalidCommitOffsetSizeException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidFetchSessionEpochException;
import org.apache.kafka.common.errors.InvalidFetchSizeException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.errors.InvalidPrincipalTypeException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.common.errors.InvalidSessionTimeoutException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.InvalidTxnTimeoutException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.LogDirNotFoundException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.OperationNotAttemptedException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.ReassignmentInProgressException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Note that client library will convert an unknown error code to the non-retriable UnknownServerException if the client library
 * version is old and does not recognize the newly-added error code. Therefore when a new server-side error is added,
 * we may need extra logic to convert the new error code to another existing error code before sending the response back to
 * the client if the request version suggests that the client may not recognize the new error code.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnknownServerException(message);
            }
        }),
    NONE(0, null,
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return null;
            }
        }),
    OFFSET_OUT_OF_RANGE(1, "The requested offset is not within the range of offsets maintained by the server.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new OffsetOutOfRangeException(message);
            }
        }),
    CORRUPT_MESSAGE(2, "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new CorruptRecordException(message);
            }
        }),
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host this topic-partition.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnknownTopicOrPartitionException(message);
            }
        }),
    INVALID_FETCH_SIZE(4, "The requested fetch size is invalid.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidFetchSizeException(message);
            }
        }),
    LEADER_NOT_AVAILABLE(5, "There is no leader for this topic-partition as we are in the middle of a leadership election.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new LeaderNotAvailableException(message);
            }
        }),
    NOT_LEADER_FOR_PARTITION(6, "This server is not the leader for that topic-partition.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new NotLeaderForPartitionException(message);
            }
        }),
    REQUEST_TIMED_OUT(7, "The request timed out.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new TimeoutException(message);
            }
        }),
    BROKER_NOT_AVAILABLE(8, "The broker is not available.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new BrokerNotAvailableException(message);
            }
        }),
    REPLICA_NOT_AVAILABLE(9, "The replica is not available for the requested topic-partition",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ReplicaNotAvailableException(message);
            }
        }),
    MESSAGE_TOO_LARGE(10, "The request included a message larger than the max message size the server will accept.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new RecordTooLargeException(message);
            }
        }),
    STALE_CONTROLLER_EPOCH(11, "The controller moved to another broker.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ControllerMovedException(message);
            }
        }),
    OFFSET_METADATA_TOO_LARGE(12, "The metadata field of the offset request was too large.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new OffsetMetadataTooLarge(message);
            }
        }),
    NETWORK_EXCEPTION(13, "The server disconnected before a response was received.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new NetworkException(message);
            }
        }),
    COORDINATOR_LOAD_IN_PROGRESS(14, "The coordinator is loading and hence can't process requests.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new CoordinatorLoadInProgressException(message);
            }
        }),
    COORDINATOR_NOT_AVAILABLE(15, "The coordinator is not available.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new CoordinatorNotAvailableException(message);
            }
        }),
    NOT_COORDINATOR(16, "This is not the correct coordinator.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new NotCoordinatorException(message);
            }
        }),
    INVALID_TOPIC_EXCEPTION(17, "The request attempted to perform an operation on an invalid topic.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidTopicException(message);
            }
        }),
    RECORD_LIST_TOO_LARGE(18, "The request included message batch larger than the configured segment size on the server.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new RecordBatchTooLargeException(message);
            }
        }),
    NOT_ENOUGH_REPLICAS(19, "Messages are rejected since there are fewer in-sync replicas than required.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new NotEnoughReplicasException(message);
            }
        }),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20, "Messages are written to the log, but to fewer in-sync replicas than required.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new NotEnoughReplicasAfterAppendException(message);
            }
        }),
    INVALID_REQUIRED_ACKS(21, "Produce request specified an invalid value for required acks.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidRequiredAcksException(message);
            }
        }),
    ILLEGAL_GENERATION(22, "Specified group generation id is not valid.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new IllegalGenerationException(message);
            }
        }),
    INCONSISTENT_GROUP_PROTOCOL(23,
            "The group member's supported protocols are incompatible with those of existing members" +
                " or first group member tried to join with empty protocol type or empty protocol list.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InconsistentGroupProtocolException(message);
            }
        }),
    INVALID_GROUP_ID(24, "The configured groupId is invalid",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidGroupIdException(message);
            }
        }),
    UNKNOWN_MEMBER_ID(25, "The coordinator is not aware of this member.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnknownMemberIdException(message);
            }
        }),
    INVALID_SESSION_TIMEOUT(26,
            "The session timeout is not within the range allowed by the broker " +
            "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidSessionTimeoutException(message);
            }
        }),
    REBALANCE_IN_PROGRESS(27, "The group is rebalancing, so a rejoin is needed.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new RebalanceInProgressException(message);
            }
        }),
    INVALID_COMMIT_OFFSET_SIZE(28, "The committing offset data size is not valid",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidCommitOffsetSizeException(message);
            }
        }),
    TOPIC_AUTHORIZATION_FAILED(29, "Topic authorization failed.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new TopicAuthorizationException(message);
            }
        }),
    GROUP_AUTHORIZATION_FAILED(30, "Group authorization failed.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new GroupAuthorizationException(message);
            }
        }),
    CLUSTER_AUTHORIZATION_FAILED(31, "Cluster authorization failed.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ClusterAuthorizationException(message);
            }
        }),
    INVALID_TIMESTAMP(32, "The timestamp of the message is out of acceptable range.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidTimestampException(message);
            }
        }),
    UNSUPPORTED_SASL_MECHANISM(33, "The broker does not support the requested SASL mechanism.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnsupportedSaslMechanismException(message);
            }
        }),
    ILLEGAL_SASL_STATE(34, "Request is not valid given the current SASL state.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new IllegalSaslStateException(message);
            }
        }),
    UNSUPPORTED_VERSION(35, "The version of API is not supported.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnsupportedVersionException(message);
            }
        }),
    TOPIC_ALREADY_EXISTS(36, "Topic with this name already exists.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new TopicExistsException(message);
            }
        }),
    INVALID_PARTITIONS(37, "Number of partitions is below 1.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidPartitionsException(message);
            }
        }),
    INVALID_REPLICATION_FACTOR(38, "Replication factor is below 1 or larger than the number of available brokers.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidReplicationFactorException(message);
            }
        }),
    INVALID_REPLICA_ASSIGNMENT(39, "Replica assignment is invalid.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidReplicaAssignmentException(message);
            }
        }),
    INVALID_CONFIG(40, "Configuration is invalid.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidConfigurationException(message);
            }
        }),
    NOT_CONTROLLER(41, "This is not the correct controller for this cluster.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new NotControllerException(message);
            }
        }),
    INVALID_REQUEST(42, "This most likely occurs because of a request being malformed by the " +
                "client library or the message was sent to an incompatible broker. See the broker logs " +
                "for more details.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidRequestException(message);
            }
        }),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43, "The message format version on the broker does not support the request.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnsupportedForMessageFormatException(message);
            }
        }),
    POLICY_VIOLATION(44, "Request parameters do not satisfy the configured policy.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new PolicyViolationException(message);
            }
        }),
    OUT_OF_ORDER_SEQUENCE_NUMBER(45, "The broker received an out of order sequence number",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new OutOfOrderSequenceException(message);
            }
        }),
    DUPLICATE_SEQUENCE_NUMBER(46, "The broker received a duplicate sequence number",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new DuplicateSequenceException(message);
            }
        }),
    INVALID_PRODUCER_EPOCH(47, "Producer attempted an operation with an old epoch. Either there is a newer producer " +
            "with the same transactionalId, or the producer's transaction has been expired by the broker.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ProducerFencedException(message);
            }
        }),
    INVALID_TXN_STATE(48, "The producer attempted a transactional operation in an invalid state",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidTxnStateException(message);
            }
        }),
    INVALID_PRODUCER_ID_MAPPING(49, "The producer attempted to use a producer id which is not currently assigned to " +
            "its transactional id",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidPidMappingException(message);
            }
        }),
    INVALID_TRANSACTION_TIMEOUT(50, "The transaction timeout is larger than the maximum value allowed by " +
                "the broker (as configured by transaction.max.timeout.ms).",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidTxnTimeoutException(message);
            }
        }),
    CONCURRENT_TRANSACTIONS(51, "The producer attempted to update a transaction " +
                "while another concurrent operation on the same transaction was ongoing",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ConcurrentTransactionsException(message);
            }
        }),
    TRANSACTION_COORDINATOR_FENCED(52, "Indicates that the transaction coordinator sending a WriteTxnMarker " +
            "is no longer the current coordinator for a given producer",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new TransactionCoordinatorFencedException(message);
            }
        }),
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED(53, "Transactional Id authorization failed",
                                                  new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new TransactionalIdAuthorizationException(message);
        }
    }),
    SECURITY_DISABLED(54, "Security features are disabled.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new SecurityDisabledException(message);
        }
    }),
    OPERATION_NOT_ATTEMPTED(55, "The broker did not attempt to execute this operation. This may happen for batched RPCs " +
            "where some operations in the batch failed, causing the broker to respond without trying the rest.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new OperationNotAttemptedException(message);
            }
    }),
    KAFKA_STORAGE_ERROR(56, "Disk error when trying to access log file on the disk.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new KafkaStorageException(message);
            }
    }),
    LOG_DIR_NOT_FOUND(57, "The user-specified log directory is not found in the broker config.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new LogDirNotFoundException(message);
            }
    }),
    SASL_AUTHENTICATION_FAILED(58, "SASL Authentication failed.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new SaslAuthenticationException(message);
            }
    }),
    UNKNOWN_PRODUCER_ID(59, "This exception is raised by the broker if it could not locate the producer metadata " +
            "associated with the producerId in question. This could happen if, for instance, the producer's records " +
            "were deleted because their retention time had elapsed. Once the last records of the producerId are " +
            "removed, the producer's metadata is removed from the broker, and future appends by the producer will " +
            "return this exception.",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new UnknownProducerIdException(message);
            }
    }),
    REASSIGNMENT_IN_PROGRESS(60, "A partition reassignment is in progress",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ReassignmentInProgressException(message);
            }
        }),
    DELEGATION_TOKEN_AUTH_DISABLED(61, "Delegation Token feature is not enabled.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new DelegationTokenDisabledException(message);
        }
    }),
    DELEGATION_TOKEN_NOT_FOUND(62, "Delegation Token is not found on server.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new DelegationTokenNotFoundException(message);
        }
    }),
    DELEGATION_TOKEN_OWNER_MISMATCH(63, "Specified Principal is not valid Owner/Renewer.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new DelegationTokenOwnerMismatchException(message);
        }
    }),
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64, "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and " + "on delegation token authenticated channels.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new UnsupportedByAuthenticationException(message);
        }
    }),
    DELEGATION_TOKEN_AUTHORIZATION_FAILED(65, "Delegation Token authorization failed.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new DelegationTokenAuthorizationException(message);
        }
    }),
    DELEGATION_TOKEN_EXPIRED(66, "Delegation Token is expired.", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new DelegationTokenExpiredException(message);
        }
    }),
    INVALID_PRINCIPAL_TYPE(67, "Supplied principalType is not supported", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new InvalidPrincipalTypeException(message);
        }
    }),
    NON_EMPTY_GROUP(68, "The group is not empty", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new GroupNotEmptyException(message);
        }
    }),
    GROUP_ID_NOT_FOUND(69, "The group id does not exist", new ApiExceptionBuilder() {
        @Override
        public ApiException build(String message) {
            return new GroupIdNotFoundException(message);
        }
    }),
    FETCH_SESSION_ID_NOT_FOUND(70, "The fetch session ID was not found",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new FetchSessionIdNotFoundException(message);
            }
    }),
    INVALID_FETCH_SESSION_EPOCH(71, "The fetch session epoch is invalid",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new InvalidFetchSessionEpochException(message);
            }
    }),
    LISTENER_NOT_FOUND(72, "There is no listener on the leader broker that matches the listener on which metadata request was processed",
        new ApiExceptionBuilder() {
            @Override
            public ApiException build(String message) {
                return new ListenerNotFoundException(message);
            }
    }),;

    private interface ApiExceptionBuilder {
        ApiException build(String message);
    }

    private static final Logger log = LoggerFactory.getLogger(Errors.class);

    private static Map<Class<?>, Errors> classToError = new HashMap<>();
    private static Map<Short, Errors> codeToError = new HashMap<>();

    static {
        for (Errors error : Errors.values()) {
            codeToError.put(error.code(), error);
            if (error.exception != null)
                classToError.put(error.exception.getClass(), error);
        }
    }

    private final short code;
    private final ApiExceptionBuilder builder;
    private final ApiException exception;

    Errors(int code, String defaultExceptionString, ApiExceptionBuilder builder) {
        this.code = (short) code;
        this.builder = builder;
        this.exception = builder.build(defaultExceptionString);
    }

    /**
     * An instance of the exception
     */
    public ApiException exception() {
        return this.exception;
    }

    /**
     * Create an instance of the ApiException that contains the given error message.
     *
     * @param message    The message string to set.
     * @return           The exception.
     */
    public ApiException exception(String message) {
        if (message == null) {
            // If no error message was specified, return an exception with the default error message.
            return exception;
        }
        // Return an exception with the given error message.
        return builder.build(message);
    }

    /**
     * Returns the class name of the exception or null if this is {@code Errors.NONE}.
     */
    public String exceptionName() {
        return exception == null ? null : exception.getClass().getName();
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
        if (exception != null) {
            throw this.exception;
        }
    }

    /**
     * Get a friendly description of the error (if one is available).
     * @return the error message
     */
    public String message() {
        if (exception != null)
            return exception.getMessage();
        return toString();
    }

    /**
     * Throw the exception if there is one
     */
    public static Errors forCode(short code) {
        Errors error = codeToError.get(code);
        if (error != null) {
            return error;
        } else {
            log.warn("Unexpected error code: {}.", code);
            return UNKNOWN_SERVER_ERROR;
        }
    }

    /**
     * Return the error instance associated with this exception or any of its superclasses (or UNKNOWN if there is none).
     * If there are multiple matches in the class hierarchy, the first match starting from the bottom is used.
     */
    public static Errors forException(Throwable t) {
        Class<?> clazz = t.getClass();
        while (clazz != null) {
            Errors error = classToError.get(clazz);
            if (error != null)
                return error;
            clazz = clazz.getSuperclass();
        }
        return UNKNOWN_SERVER_ERROR;
    }

    private static String toHtml() {
        final StringBuilder b = new StringBuilder();
        b.append("<table class=\"data-table\"><tbody>\n");
        b.append("<tr>");
        b.append("<th>Error</th>\n");
        b.append("<th>Code</th>\n");
        b.append("<th>Retriable</th>\n");
        b.append("<th>Description</th>\n");
        b.append("</tr>\n");
        for (Errors error : Errors.values()) {
            b.append("<tr>");
            b.append("<td>");
            b.append(error.name());
            b.append("</td>");
            b.append("<td>");
            b.append(error.code());
            b.append("</td>");
            b.append("<td>");
            b.append(error.exception() != null && error.exception() instanceof RetriableException ? "True" : "False");
            b.append("</td>");
            b.append("<td>");
            b.append(error.exception() != null ? error.exception().getMessage() : "");
            b.append("</td>");
            b.append("</tr>\n");
        }
        b.append("</table>\n");
        return b.toString();
    }

    public static void main(String[] args) {
        System.out.println(toHtml());
    }
}
