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

import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BrokerNotAvailableException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.ControllerMovedException;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.GroupLoadInProgressException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.InconsistentGroupProtocolException;
import org.apache.kafka.common.errors.InvalidCommitOffsetSizeException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidFetchSizeException;
import org.apache.kafka.common.errors.InvalidGroupIdException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidRequiredAcksException;
import org.apache.kafka.common.errors.InvalidSessionTimeoutException;
import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.UnsupportedForMessageFormatException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.NotCoordinatorForGroupException;
import org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetMetadataTooLarge;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.ReplicaNotAvailableException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains all the client-server errors--those errors that must be sent from the server to the client. These
 * are thus part of the protocol. The names can be changed but the error code cannot.
 *
 * Do not add exceptions that occur only on the client or only on the server here.
 */
public enum Errors {
    UNKNOWN(-1, new UnknownServerException("The server experienced an unexpected error when processing the request")),
    NONE(0, null),
    OFFSET_OUT_OF_RANGE(1,
            new OffsetOutOfRangeException("The requested offset is not within the range of offsets maintained by the server.")),
    CORRUPT_MESSAGE(2,
            new CorruptRecordException("This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.")),
    UNKNOWN_TOPIC_OR_PARTITION(3,
            new UnknownTopicOrPartitionException("This server does not host this topic-partition.")),
    INVALID_FETCH_SIZE(4,
            new InvalidFetchSizeException("The requested fetch size is invalid.")),
    LEADER_NOT_AVAILABLE(5,
            new LeaderNotAvailableException("There is no leader for this topic-partition as we are in the middle of a leadership election.")),
    NOT_LEADER_FOR_PARTITION(6,
            new NotLeaderForPartitionException("This server is not the leader for that topic-partition.")),
    REQUEST_TIMED_OUT(7,
            new TimeoutException("The request timed out.")),
    BROKER_NOT_AVAILABLE(8,
            new BrokerNotAvailableException("The broker is not available.")),
    REPLICA_NOT_AVAILABLE(9,
            new ReplicaNotAvailableException("The replica is not available for the requested topic-partition")),
    MESSAGE_TOO_LARGE(10,
            new RecordTooLargeException("The request included a message larger than the max message size the server will accept.")),
    STALE_CONTROLLER_EPOCH(11,
            new ControllerMovedException("The controller moved to another broker.")),
    OFFSET_METADATA_TOO_LARGE(12,
            new OffsetMetadataTooLarge("The metadata field of the offset request was too large.")),
    NETWORK_EXCEPTION(13,
            new NetworkException("The server disconnected before a response was received.")),
    GROUP_LOAD_IN_PROGRESS(14,
            new GroupLoadInProgressException("The coordinator is loading and hence can't process requests for this group.")),
    GROUP_COORDINATOR_NOT_AVAILABLE(15,
            new GroupCoordinatorNotAvailableException("The group coordinator is not available.")),
    NOT_COORDINATOR_FOR_GROUP(16,
            new NotCoordinatorForGroupException("This is not the correct coordinator for this group.")),
    INVALID_TOPIC_EXCEPTION(17,
            new InvalidTopicException("The request attempted to perform an operation on an invalid topic.")),
    RECORD_LIST_TOO_LARGE(18,
            new RecordBatchTooLargeException("The request included message batch larger than the configured segment size on the server.")),
    NOT_ENOUGH_REPLICAS(19,
            new NotEnoughReplicasException("Messages are rejected since there are fewer in-sync replicas than required.")),
    NOT_ENOUGH_REPLICAS_AFTER_APPEND(20,
            new NotEnoughReplicasAfterAppendException("Messages are written to the log, but to fewer in-sync replicas than required.")),
    INVALID_REQUIRED_ACKS(21,
            new InvalidRequiredAcksException("Produce request specified an invalid value for required acks.")),
    ILLEGAL_GENERATION(22,
            new IllegalGenerationException("Specified group generation id is not valid.")),
    INCONSISTENT_GROUP_PROTOCOL(23,
            new InconsistentGroupProtocolException("The group member's supported protocols are incompatible with those of existing members.")),
    INVALID_GROUP_ID(24,
            new InvalidGroupIdException("The configured groupId is invalid")),
    UNKNOWN_MEMBER_ID(25,
            new UnknownMemberIdException("The coordinator is not aware of this member.")),
    INVALID_SESSION_TIMEOUT(26,
            new InvalidSessionTimeoutException("The session timeout is not within the range allowed by the broker " +
                    "(as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).")),
    REBALANCE_IN_PROGRESS(27,
            new RebalanceInProgressException("The group is rebalancing, so a rejoin is needed.")),
    INVALID_COMMIT_OFFSET_SIZE(28,
            new InvalidCommitOffsetSizeException("The committing offset data size is not valid")),
    TOPIC_AUTHORIZATION_FAILED(29,
            new TopicAuthorizationException("Topic authorization failed.")),
    GROUP_AUTHORIZATION_FAILED(30,
            new GroupAuthorizationException("Group authorization failed.")),
    CLUSTER_AUTHORIZATION_FAILED(31,
            new ClusterAuthorizationException("Cluster authorization failed.")),
    INVALID_TIMESTAMP(32,
            new InvalidTimestampException("The timestamp of the message is out of acceptable range.")),
    UNSUPPORTED_SASL_MECHANISM(33,
            new UnsupportedSaslMechanismException("The broker does not support the requested SASL mechanism.")),
    ILLEGAL_SASL_STATE(34,
            new IllegalSaslStateException("Request is not valid given the current SASL state.")),
    UNSUPPORTED_VERSION(35,
            new UnsupportedVersionException("The version of API is not supported.")),
    TOPIC_ALREADY_EXISTS(36,
            new TopicExistsException("Topic with this name already exists.")),
    INVALID_PARTITIONS(37,
            new InvalidPartitionsException("Number of partitions is invalid.")),
    INVALID_REPLICATION_FACTOR(38,
            new InvalidReplicationFactorException("Replication-factor is invalid.")),
    INVALID_REPLICA_ASSIGNMENT(39,
            new InvalidReplicaAssignmentException("Replica assignment is invalid.")),
    INVALID_CONFIG(40,
            new InvalidConfigurationException("Configuration is invalid.")),
    NOT_CONTROLLER(41,
        new NotControllerException("This is not the correct controller for this cluster.")),
    INVALID_REQUEST(42,
        new InvalidRequestException("This most likely occurs because of a request being malformed by the client library or" +
            " the message was sent to an incompatible broker. See the broker logs for more details.")),
    UNSUPPORTED_FOR_MESSAGE_FORMAT(43,
        new UnsupportedForMessageFormatException("The message format version on the broker does not support the request."));

    private static final Logger log = LoggerFactory.getLogger(Errors.class);

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
            return UNKNOWN;
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
        return UNKNOWN;
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
