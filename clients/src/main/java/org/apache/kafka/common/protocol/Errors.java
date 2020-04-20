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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ConcurrentTransactionsException;
import org.apache.kafka.common.errors.GroupSubscribedToTopicException;
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
import org.apache.kafka.common.errors.FencedLeaderEpochException;
import org.apache.kafka.common.errors.ListenerNotFoundException;
import org.apache.kafka.common.errors.FetchSessionIdNotFoundException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
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
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.MemberIdRequiredException;
import org.apache.kafka.common.errors.ElectionNotNeededException;
import org.apache.kafka.common.errors.EligibleLeadersNotAvailableException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.OffsetNotAvailableException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.OperationNotAttemptedException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.UnstableOffsetCommitException;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.common.errors.PreferredLeaderNotAvailableException;
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
import org.apache.kafka.common.errors.TopicDeletionDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.TransactionalIdAuthorizationException;
import org.apache.kafka.common.errors.TransactionCoordinatorFencedException;
import org.apache.kafka.common.errors.UnknownLeaderEpochException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedByAuthenticationException;
import org.apache.kafka.common.errors.UnsupportedCompressionTypeException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.StaleBrokerEpochException;
import org.apache.kafka.common.errors.WillExceedPartitionLimitsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

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
    UNKNOWN_SERVER_ERROR(-1, "The server experienced an unexpected error when processing the request.",
            UnknownServerException::new),
    NONE(0, null, message -> null),
    OFFSET_OUT_OF_RANGE(1, "The requested offset is not within the range of offsets maintained by the server.",
            OffsetOutOfRangeException::new),
    CORRUPT_MESSAGE(2, "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
            CorruptRecordException::new),
    UNKNOWN_TOPIC_OR_PARTITION(3, "This server does not host this topic-partition.",
            UnknownTopicOrPartitionException::new),
    INVALID_FETCH_SIZE(4, "The requested fetch size is invalid.",
            InvalidFetchSizeException::new),
    LEADER_NOT_AVAILABLE(5, "There is no leader for this topic-partition as we are in the middle of a leadership election.",
            LeaderNotAvailableException::new),
    NOT_LEADER_FOR_PARTITION(6, "This server is not the leader for that topic-partition.",
            NotLeaderForPartitionException::new),
    REQUEST_TIMED_OUT(7, "The request timed out.",
            TimeoutException::new),
    BROKER_NOT_AVAILABLE(8, "The broker is not available.",
            BrokerNotAvailableException::new),
    REPLICA_NOT_AVAILABLE(9, "The replica is not available for the requested topic-partition.",
            ReplicaNotAvailableException::new),
    MESSAGE_TOO_LARGE(10, "The request included a message larger than the max message size the server will accept.",
            RecordTooLargeException::new),
    STALE_CONTROLLER_EPOCH(11, "The controller moved to another broker.",
            ControllerMovedException::new),
    OFFSET_METADATA_TOO_LARGE(12, "The metadata field of the offset request was too large.",
            OffsetMetadataTooLarge::new),
    NETWORK_EXCEPTION(13, "The server disconnected before a response was received.",
            NetworkException::new),
    COORDINATOR_LOAD_IN_PROGRESS(14, "The coordinator is loading and hence can't process requests.",
            CoordinatorLoadInProgressException::new),
    COORDINATOR_NOT_AVAILABLE(15, "The coordinator is not available.",
            CoordinatorNotAvailableException::new),
    NOT_COORDINATOR(16, "This is not the correct coordinator.",
            NotCoordinatorException::new),
    INVALID_TOPIC_EXCEPTION(17, "The request attempted to perform an operation on an invalid topic.",
            InvalidTopicException::new),
    RECORD_LIST_TOO_LARGE(18, "The request included message batch larger than the configured segment size on the server.",
            RecordBatchTooLargeException::new),
    NOT_ENOUGH_REPLICAS(19, "Messages are rejected since there are fewer in-sync replicas than required.",
            NotEnoughReplicasException::new),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20, "Messages are written to the log, but to fewer in-sync replicas than required.",
            NotEnoughReplicasAfterAppendException::new),
    INVALID_REQUIRED_ACKS(21, "Produce request specified an invalid value for required acks.",
            InvalidRequiredAcksException::new),
    ILLEGAL_GENERATION(22, "Specified group generation id is not valid.",
            IllegalGenerationException::new),
    INCONSISTENT_GROUP_PROTOCOL(23,
            "The group member's supported protocols are incompatible with those of existing members " +
            "or first group member tried to join with empty protocol type or empty protocol list.",
            InconsistentGroupProtocolException::new),
    INVALID_GROUP_ID(24, "The configured groupId is invalid.",
            InvalidGroupIdException::new),
    UNKNOWN_MEMBER_ID(25, "The coordinator is not aware of this member.",
            UnknownMemberIdException::new),
    INVALID_SESSION_TIMEOUT(26,
            "The session timeout is not within the range allowed by the broker " +
            "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
            InvalidSessionTimeoutException::new),
    REBALANCE_IN_PROGRESS(27, "The group is rebalancing, so a rejoin is needed.",
            RebalanceInProgressException::new),
    INVALID_COMMIT_OFFSET_SIZE(28, "The committing offset data size is not valid.",
            InvalidCommitOffsetSizeException::new),
    TOPIC_AUTHORIZATION_FAILED(29, "Topic authorization failed.", TopicAuthorizationException::new),
    GROUP_AUTHORIZATION_FAILED(30, "Group authorization failed.", GroupAuthorizationException::new),
    CLUSTER_AUTHORIZATION_FAILED(31, "Cluster authorization failed.",
            ClusterAuthorizationException::new),
    INVALID_TIMESTAMP(32, "The timestamp of the message is out of acceptable range.",
            InvalidTimestampException::new),
    UNSUPPORTED_SASL_MECHANISM(33, "The broker does not support the requested SASL mechanism.",
            UnsupportedSaslMechanismException::new),
    ILLEGAL_SASL_STATE(34, "Request is not valid given the current SASL state.",
            IllegalSaslStateException::new),
    UNSUPPORTED_VERSION(35, "The version of API is not supported.",
            UnsupportedVersionException::new),
    TOPIC_ALREADY_EXISTS(36, "Topic with this name already exists.",
            TopicExistsException::new),
    INVALID_PARTITIONS(37, "Number of partitions is below 1.",
            InvalidPartitionsException::new),
    INVALID_REPLICATION_FACTOR(38, "Replication factor is below 1 or larger than the number of available brokers.",
            InvalidReplicationFactorException::new),
    INVALID_REPLICA_ASSIGNMENT(39, "Replica assignment is invalid.",
            InvalidReplicaAssignmentException::new),
    INVALID_CONFIG(40, "Configuration is invalid.",
            InvalidConfigurationException::new),
    NOT_CONTROLLER(41, "This is not the correct controller for this cluster.",
            NotControllerException::new),
    INVALID_REQUEST(42, "This most likely occurs because of a request being malformed by the " +
            "client library or the message was sent to an incompatible broker. See the broker logs " +
            "for more details.",
            InvalidRequestException::new),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43, "The message format version on the broker does not support the request.",
            UnsupportedForMessageFormatException::new),
    POLICY_VIOLATION(44, "Request parameters do not satisfy the configured policy.",
            PolicyViolationException::new),
    OUT_OF_ORDER_SEQUENCE_NUMBER(45, "The broker received an out of order sequence number.",
            OutOfOrderSequenceException::new),
    DUPLICATE_SEQUENCE_NUMBER(46, "The broker received a duplicate sequence number.",
            DuplicateSequenceException::new),
    INVALID_PRODUCER_EPOCH(47, "Producer attempted an operation with an old epoch. Either there is a newer producer " +
            "with the same transactionalId, or the producer's transaction has been expired by the broker.",
            ProducerFencedException::new),
    INVALID_TXN_STATE(48, "The producer attempted a transactional operation in an invalid state.",
            InvalidTxnStateException::new),
    INVALID_PRODUCER_ID_MAPPING(49, "The producer attempted to use a producer id which is not currently assigned to " +
            "its transactional id.",
            InvalidPidMappingException::new),
    INVALID_TRANSACTION_TIMEOUT(50, "The transaction timeout is larger than the maximum value allowed by " +
            "the broker (as configured by transaction.max.timeout.ms).",
            InvalidTxnTimeoutException::new),
    CONCURRENT_TRANSACTIONS(51, "The producer attempted to update a transaction " +
            "while another concurrent operation on the same transaction was ongoing.",
            ConcurrentTransactionsException::new),
    TRANSACTION_COORDINATOR_FENCED(52, "Indicates that the transaction coordinator sending a WriteTxnMarker " +
            "is no longer the current coordinator for a given producer.",
            TransactionCoordinatorFencedException::new),
    TRANSACTIONAL_ID_AUTHORIZATION_FAILED(53, "Transactional Id authorization failed.",
            TransactionalIdAuthorizationException::new),
    SECURITY_DISABLED(54, "Security features are disabled.",
            SecurityDisabledException::new),
    OPERATION_NOT_ATTEMPTED(55, "The broker did not attempt to execute this operation. This may happen for " +
            "batched RPCs where some operations in the batch failed, causing the broker to respond without " +
            "trying the rest.",
            OperationNotAttemptedException::new),
    KAFKA_STORAGE_ERROR(56, "Disk error when trying to access log file on the disk.",
            KafkaStorageException::new),
    LOG_DIR_NOT_FOUND(57, "The user-specified log directory is not found in the broker config.",
            LogDirNotFoundException::new),
    SASL_AUTHENTICATION_FAILED(58, "SASL Authentication failed.",
            SaslAuthenticationException::new),
    UNKNOWN_PRODUCER_ID(59, "This exception is raised by the broker if it could not locate the producer metadata " +
            "associated with the producerId in question. This could happen if, for instance, the producer's records " +
            "were deleted because their retention time had elapsed. Once the last records of the producerId are " +
            "removed, the producer's metadata is removed from the broker, and future appends by the producer will " +
            "return this exception.",
            UnknownProducerIdException::new),
    REASSIGNMENT_IN_PROGRESS(60, "A partition reassignment is in progress.",
            ReassignmentInProgressException::new),
    DELEGATION_TOKEN_AUTH_DISABLED(61, "Delegation Token feature is not enabled.",
            DelegationTokenDisabledException::new),
    DELEGATION_TOKEN_NOT_FOUND(62, "Delegation Token is not found on server.",
            DelegationTokenNotFoundException::new),
    DELEGATION_TOKEN_OWNER_MISMATCH(63, "Specified Principal is not valid Owner/Renewer.",
            DelegationTokenOwnerMismatchException::new),
    DELEGATION_TOKEN_REQUEST_NOT_ALLOWED(64, "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL " +
            "channels and on delegation token authenticated channels.",
            UnsupportedByAuthenticationException::new),
    DELEGATION_TOKEN_AUTHORIZATION_FAILED(65, "Delegation Token authorization failed.",
            DelegationTokenAuthorizationException::new),
    DELEGATION_TOKEN_EXPIRED(66, "Delegation Token is expired.",
            DelegationTokenExpiredException::new),
    INVALID_PRINCIPAL_TYPE(67, "Supplied principalType is not supported.",
            InvalidPrincipalTypeException::new),
    NON_EMPTY_GROUP(68, "The group is not empty.",
            GroupNotEmptyException::new),
    GROUP_ID_NOT_FOUND(69, "The group id does not exist.",
            GroupIdNotFoundException::new),
    FETCH_SESSION_ID_NOT_FOUND(70, "The fetch session ID was not found.",
            FetchSessionIdNotFoundException::new),
    INVALID_FETCH_SESSION_EPOCH(71, "The fetch session epoch is invalid.",
            InvalidFetchSessionEpochException::new),
    LISTENER_NOT_FOUND(72, "There is no listener on the leader broker that matches the listener on which " +
            "metadata request was processed.",
            ListenerNotFoundException::new),
    TOPIC_DELETION_DISABLED(73, "Topic deletion is disabled.",
            TopicDeletionDisabledException::new),
    FENCED_LEADER_EPOCH(74, "The leader epoch in the request is older than the epoch on the broker.",
            FencedLeaderEpochException::new),
    UNKNOWN_LEADER_EPOCH(75, "The leader epoch in the request is newer than the epoch on the broker.",
            UnknownLeaderEpochException::new),
    UNSUPPORTED_COMPRESSION_TYPE(76, "The requesting client does not support the compression type of given partition.",
            UnsupportedCompressionTypeException::new),
    STALE_BROKER_EPOCH(77, "Broker epoch has changed.",
            StaleBrokerEpochException::new),
    OFFSET_NOT_AVAILABLE(78, "The leader high watermark has not caught up from a recent leader " +
            "election so the offsets cannot be guaranteed to be monotonically increasing.",
            OffsetNotAvailableException::new),
    MEMBER_ID_REQUIRED(79, "The group member needs to have a valid member id before actually entering a consumer group.",
            MemberIdRequiredException::new),
    PREFERRED_LEADER_NOT_AVAILABLE(80, "The preferred leader was not available.",
            PreferredLeaderNotAvailableException::new),
    GROUP_MAX_SIZE_REACHED(81, "The consumer group has reached its max size.", GroupMaxSizeReachedException::new),
    FENCED_INSTANCE_ID(82, "The broker rejected this static consumer since " +
            "another consumer with the same group.instance.id has registered with a different member.id.",
            FencedInstanceIdException::new),
    ELIGIBLE_LEADERS_NOT_AVAILABLE(83, "Eligible topic partition leaders are not available.",
            EligibleLeadersNotAvailableException::new),
    ELECTION_NOT_NEEDED(84, "Leader election not needed for topic partition.", ElectionNotNeededException::new),
    NO_REASSIGNMENT_IN_PROGRESS(85, "No partition reassignment is in progress.",
            NoReassignmentInProgressException::new),
    GROUP_SUBSCRIBED_TO_TOPIC(86, "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
        GroupSubscribedToTopicException::new),
    INVALID_RECORD(87, "This record has failed the validation on broker and hence be rejected.", InvalidRecordException::new),
    UNSTABLE_OFFSET_COMMIT(88, "There are unstable offsets that need to be cleared.", UnstableOffsetCommitException::new),
    WILL_EXCEED_PARTITION_LIMITS(89, "Cannot satisfy request without exceeding the partition limits", WillExceedPartitionLimitsException::new);

    private static final Logger log = LoggerFactory.getLogger(Errors.class);

    private static Map<Class<?>, Errors> classToError = new HashMap<>();
    private static Map<Short, Errors> codeToError = new HashMap<>();

    static {
        for (Errors error : Errors.values()) {
            if (codeToError.put(error.code(), error) != null)
                throw new ExceptionInInitializerError("Code " + error.code() + " for error " +
                        error + " has already been used");

            if (error.exception != null)
                classToError.put(error.exception.getClass(), error);
        }
    }

    private final short code;
    private final Function<String, ApiException> builder;
    private final ApiException exception;

    Errors(int code, String defaultExceptionString, Function<String, ApiException> builder) {
        this.code = (short) code;
        this.builder = builder;
        this.exception = builder.apply(defaultExceptionString);
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
        return builder.apply(message);
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
