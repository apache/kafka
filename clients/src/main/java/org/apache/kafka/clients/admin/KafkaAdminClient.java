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

package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.LeastLoadedNode;
import org.apache.kafka.clients.MetadataRecoveryStrategy;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.StaleMetadataException;
import org.apache.kafka.clients.admin.CreateTopicsResult.TopicMetadataAndConfig;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResult;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec.TimestampSpec;
import org.apache.kafka.clients.admin.internals.AbortTransactionHandler;
import org.apache.kafka.clients.admin.internals.AdminApiDriver;
import org.apache.kafka.clients.admin.internals.AdminApiFuture;
import org.apache.kafka.clients.admin.internals.AdminApiFuture.SimpleAdminApiFuture;
import org.apache.kafka.clients.admin.internals.AdminApiHandler;
import org.apache.kafka.clients.admin.internals.AdminBootstrapAddresses;
import org.apache.kafka.clients.admin.internals.AdminFetchMetricsManager;
import org.apache.kafka.clients.admin.internals.AdminMetadataManager;
import org.apache.kafka.clients.admin.internals.AllBrokersStrategy;
import org.apache.kafka.clients.admin.internals.AlterConsumerGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.CoordinatorKey;
import org.apache.kafka.clients.admin.internals.DeleteConsumerGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.DeleteConsumerGroupsHandler;
import org.apache.kafka.clients.admin.internals.DeleteRecordsHandler;
import org.apache.kafka.clients.admin.internals.DescribeClassicGroupsHandler;
import org.apache.kafka.clients.admin.internals.DescribeConsumerGroupsHandler;
import org.apache.kafka.clients.admin.internals.DescribeProducersHandler;
import org.apache.kafka.clients.admin.internals.DescribeShareGroupsHandler;
import org.apache.kafka.clients.admin.internals.DescribeTransactionsHandler;
import org.apache.kafka.clients.admin.internals.FenceProducersHandler;
import org.apache.kafka.clients.admin.internals.ListConsumerGroupOffsetsHandler;
import org.apache.kafka.clients.admin.internals.ListOffsetsHandler;
import org.apache.kafka.clients.admin.internals.ListTransactionsHandler;
import org.apache.kafka.clients.admin.internals.PartitionLeaderStrategy;
import org.apache.kafka.clients.admin.internals.RemoveMembersFromConsumerGroupHandler;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicCollection.TopicIdCollection;
import org.apache.kafka.common.TopicCollection.TopicNameCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.TopicPartitionReplica;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.MismatchedEndpointTypeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnacceptableCredentialException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedEndpointTypeException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDir;
import org.apache.kafka.common.message.AlterReplicaLogDirsRequestData.AlterReplicaLogDirTopic;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirPartitionResult;
import org.apache.kafka.common.message.AlterReplicaLogDirsResponseData.AlterReplicaLogDirTopicResult;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData.FinalizedFeatureKey;
import org.apache.kafka.common.message.ApiVersionsResponseData.SupportedFeatureKey;
import org.apache.kafka.common.message.CreateAclsRequestData;
import org.apache.kafka.common.message.CreateAclsRequestData.AclCreation;
import org.apache.kafka.common.message.CreateAclsResponseData.AclCreationResult;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData.CreatableRenewers;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopicCollection;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicCollection;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicConfigs;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.message.DeleteAclsRequestData;
import org.apache.kafka.common.message.DeleteAclsRequestData.DeleteAclsFilter;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsFilterResult;
import org.apache.kafka.common.message.DeleteAclsResponseData.DeleteAclsMatchingAcl;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsRequestData.DeleteTopicState;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeConfigsRequestData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsRequestData.DescribableLogDirTopic;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.message.DescribeQuorumResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData;
import org.apache.kafka.common.message.DescribeTopicPartitionsRequestData.TopicRequest;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData.UserName;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.ListClientMetricsResourcesRequestData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.RemoveRaftVoterRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.UnregisterBrokerRequestData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData.UpdatableFeatureResult;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.KafkaMetricsContext;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.AddRaftVoterRequest;
import org.apache.kafka.common.requests.AddRaftVoterResponse;
import org.apache.kafka.common.requests.AlterClientQuotasRequest;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.AlterPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.AlterReplicaLogDirsRequest;
import org.apache.kafka.common.requests.AlterReplicaLogDirsResponse;
import org.apache.kafka.common.requests.AlterUserScramCredentialsRequest;
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.ApiVersionsRequest;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.CreateAclsRequest;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.CreateDelegationTokenRequest;
import org.apache.kafka.common.requests.CreateDelegationTokenResponse;
import org.apache.kafka.common.requests.CreatePartitionsRequest;
import org.apache.kafka.common.requests.CreatePartitionsResponse;
import org.apache.kafka.common.requests.CreateTopicsRequest;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.requests.DeleteAclsRequest;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DeleteTopicsRequest;
import org.apache.kafka.common.requests.DeleteTopicsResponse;
import org.apache.kafka.common.requests.DescribeAclsRequest;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasRequest;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeClusterRequest;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeConfigsRequest;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeDelegationTokenRequest;
import org.apache.kafka.common.requests.DescribeDelegationTokenResponse;
import org.apache.kafka.common.requests.DescribeLogDirsRequest;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.apache.kafka.common.requests.DescribeQuorumRequest;
import org.apache.kafka.common.requests.DescribeQuorumRequest.Builder;
import org.apache.kafka.common.requests.DescribeQuorumResponse;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsRequest;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ElectLeadersRequest;
import org.apache.kafka.common.requests.ElectLeadersResponse;
import org.apache.kafka.common.requests.ExpireDelegationTokenRequest;
import org.apache.kafka.common.requests.ExpireDelegationTokenResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsRequest;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.ListClientMetricsResourcesRequest;
import org.apache.kafka.common.requests.ListClientMetricsResourcesResponse;
import org.apache.kafka.common.requests.ListGroupsRequest;
import org.apache.kafka.common.requests.ListGroupsResponse;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsRequest;
import org.apache.kafka.common.requests.ListPartitionReassignmentsResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RemoveRaftVoterRequest;
import org.apache.kafka.common.requests.RemoveRaftVoterResponse;
import org.apache.kafka.common.requests.RenewDelegationTokenRequest;
import org.apache.kafka.common.requests.RenewDelegationTokenResponse;
import org.apache.kafka.common.requests.UnregisterBrokerRequest;
import org.apache.kafka.common.requests.UnregisterBrokerResponse;
import org.apache.kafka.common.requests.UpdateFeaturesRequest;
import org.apache.kafka.common.requests.UpdateFeaturesResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.token.delegation.DelegationToken;
import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryReporter;
import org.apache.kafka.common.telemetry.internals.ClientTelemetryUtils;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.clients.admin.internals.AdminUtils.validAclOperations;
import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_NAME;
import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_PARTITION;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignablePartitionResponse;
import static org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData.ReassignableTopicResponse;
import static org.apache.kafka.common.message.ListPartitionReassignmentsRequestData.ListPartitionReassignmentsTopics;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingPartitionReassignment;
import static org.apache.kafka.common.message.ListPartitionReassignmentsResponseData.OngoingTopicReassignment;
import static org.apache.kafka.common.requests.MetadataRequest.convertToMetadataRequestTopic;
import static org.apache.kafka.common.requests.MetadataRequest.convertTopicIdsToMetadataRequestTopic;
import static org.apache.kafka.common.utils.Utils.closeQuietly;

/**
 * The default implementation of {@link Admin}. An instance of this class is created by invoking one of the
 * {@code create()} methods in {@code AdminClient}. Users should not refer to this class directly.
 *
 * <p>
 * This class is thread-safe.
 * </p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class KafkaAdminClient extends AdminClient {

    /**
     * The next integer to use to name a KafkaAdminClient which the user hasn't specified an explicit name for.
     */
    private static final AtomicInteger ADMIN_CLIENT_ID_SEQUENCE = new AtomicInteger(1);

    /**
     * The prefix to use for the JMX metrics for this class
     */
    private static final String JMX_PREFIX = "kafka.admin.client";

    /**
     * An invalid shutdown time which indicates that a shutdown has not yet been performed.
     */
    private static final long INVALID_SHUTDOWN_TIME = -1;

    /**
     * The default reason for a LeaveGroupRequest.
     */
    static final String DEFAULT_LEAVE_GROUP_REASON = "member was removed by an admin";

    /**
     * Thread name prefix for admin client network thread
     */
    static final String NETWORK_THREAD_PREFIX = "kafka-admin-client-thread";

    private final Logger log;
    private final LogContext logContext;

    /**
     * The default timeout to use for an operation.
     */
    private final int defaultApiTimeoutMs;

    /**
     * The timeout to use for a single request.
     */
    private final int requestTimeoutMs;

    /**
     * The name of this AdminClient instance.
     */
    private final String clientId;

    /**
     * Provides the time.
     */
    private final Time time;

    /**
     * The cluster metadata manager used by the KafkaClient.
     */
    private final AdminMetadataManager metadataManager;

    /**
     * The metrics for this KafkaAdminClient.
     */
    final Metrics metrics;

    /**
     * The network client to use.
     */
    private final KafkaClient client;

    /**
     * The runnable used in the service thread for this admin client.
     */
    private final AdminClientRunnable runnable;

    /**
     * The network service thread for this admin client.
     */
    private final Thread thread;

    /**
     * During a close operation, this is the time at which we will time out all pending operations
     * and force the RPC thread to exit. If the admin client is not closing, this will be 0.
     */
    private final AtomicLong hardShutdownTimeMs = new AtomicLong(INVALID_SHUTDOWN_TIME);

    /**
     * A factory which creates TimeoutProcessors for the RPC thread.
     */
    private final TimeoutProcessorFactory timeoutProcessorFactory;

    private final int maxRetries;

    private final long retryBackoffMs;
    private final long retryBackoffMaxMs;
    private final ExponentialBackoff retryBackoff;
    private final MetadataRecoveryStrategy metadataRecoveryStrategy;
    private final Map<TopicPartition, Integer> partitionLeaderCache;
    private final AdminFetchMetricsManager adminFetchMetricsManager;
    private final Optional<ClientTelemetryReporter> clientTelemetryReporter;

    /**
     * The telemetry requests client instance id.
     */
    private Uuid clientInstanceId;

    /**
     * Get or create a list value from a map.
     *
     * @param map   The map to get or create the element from.
     * @param key   The key.
     * @param <K>   The key type.
     * @param <V>   The value type.
     * @return      The list value.
     */
    static <K, V> List<V> getOrCreateListValue(Map<K, List<V>> map, K key) {
        return map.computeIfAbsent(key, k -> new LinkedList<>());
    }

    /**
     * Send an exception to every element in a collection of KafkaFutureImpls.
     *
     * @param futures   The collection of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    private static <T> void completeAllExceptionally(Collection<KafkaFutureImpl<T>> futures, Throwable exc) {
        completeAllExceptionally(futures.stream(), exc);
    }

    /**
     * Send an exception to all futures in the provided stream
     *
     * @param futures   The stream of KafkaFutureImpl objects.
     * @param exc       The exception
     * @param <T>       The KafkaFutureImpl result type.
     */
    private static <T> void completeAllExceptionally(Stream<KafkaFutureImpl<T>> futures, Throwable exc) {
        futures.forEach(future -> future.completeExceptionally(exc));
    }

    /**
     * Get the current time remaining before a deadline as an integer.
     *
     * @param now           The current time in milliseconds.
     * @param deadlineMs    The deadline time in milliseconds.
     * @return              The time delta in milliseconds.
     */
    static int calcTimeoutMsRemainingAsInt(long now, long deadlineMs) {
        long deltaMs = deadlineMs - now;
        if (deltaMs > Integer.MAX_VALUE)
            deltaMs = Integer.MAX_VALUE;
        else if (deltaMs < Integer.MIN_VALUE)
            deltaMs = Integer.MIN_VALUE;
        return (int) deltaMs;
    }

    /**
     * Generate the client id based on the configuration.
     *
     * @param config    The configuration
     *
     * @return          The client id
     */
    static String generateClientId(AdminClientConfig config) {
        String clientId = config.getString(AdminClientConfig.CLIENT_ID_CONFIG);
        if (!clientId.isEmpty())
            return clientId;
        return "adminclient-" + ADMIN_CLIENT_ID_SEQUENCE.getAndIncrement();
    }

    String getClientId() {
        return clientId;
    }

    /**
     * Get the deadline for a particular call.
     *
     * @param now               The current time in milliseconds.
     * @param optionTimeoutMs   The timeout option given by the user.
     *
     * @return                  The deadline in milliseconds.
     */
    private long calcDeadlineMs(long now, Integer optionTimeoutMs) {
        if (optionTimeoutMs != null)
            return now + Math.max(0, optionTimeoutMs);
        return now + defaultApiTimeoutMs;
    }

    /**
     * Pretty-print an exception.
     *
     * @param throwable     The exception.
     *
     * @return              A compact human-readable string.
     */
    static String prettyPrintException(Throwable throwable) {
        if (throwable == null)
            return "Null exception.";
        if (throwable.getMessage() != null) {
            return throwable.getClass().getSimpleName() + ": " + throwable.getMessage();
        }
        return throwable.getClass().getSimpleName();
    }

    static KafkaAdminClient createInternal(AdminClientConfig config, TimeoutProcessorFactory timeoutProcessorFactory) {
        return createInternal(config, timeoutProcessorFactory, null);
    }

    static KafkaAdminClient createInternal(
        AdminClientConfig config,
        TimeoutProcessorFactory timeoutProcessorFactory,
        HostResolver hostResolver
    ) {
        Metrics metrics = null;
        NetworkClient networkClient = null;
        Time time = Time.SYSTEM;
        String clientId = generateClientId(config);
        ApiVersions apiVersions = new ApiVersions();
        LogContext logContext = createLogContext(clientId);
        Optional<ClientTelemetryReporter> clientTelemetryReporter;

        try {
            // Since we only request node information, it's safe to pass true for allowAutoTopicCreation (and it
            // simplifies communication with older brokers)
            AdminBootstrapAddresses adminAddresses = AdminBootstrapAddresses.fromConfig(config);
            AdminMetadataManager metadataManager = new AdminMetadataManager(logContext,
                config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                config.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG),
                adminAddresses.usingBootstrapControllers());
            metadataManager.update(Cluster.bootstrap(adminAddresses.addresses()), time.milliseconds());
            List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(clientId, config);
            clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);
            clientTelemetryReporter.ifPresent(reporters::add);
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(AdminClientConfig.METRICS_NUM_SAMPLES_CONFIG))
                .timeWindow(config.getLong(AdminClientConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                .recordLevel(Sensor.RecordingLevel.forName(config.getString(AdminClientConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                .tags(metricTags);
            MetricsContext metricsContext = new KafkaMetricsContext(JMX_PREFIX,
                    config.originalsWithPrefix(CommonClientConfigs.METRICS_CONTEXT_PREFIX));
            metrics = new Metrics(metricConfig, reporters, time, metricsContext);
            networkClient = ClientUtils.createNetworkClient(config,
                clientId,
                metrics,
                "admin-client",
                logContext,
                apiVersions,
                time,
                1,
                (int) TimeUnit.HOURS.toMillis(1),
                null,
                metadataManager.updater(),
                (hostResolver == null) ? new DefaultHostResolver() : hostResolver,
                null,
                clientTelemetryReporter.map(ClientTelemetryReporter::telemetrySender).orElse(null));
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics, networkClient,
                timeoutProcessorFactory, logContext, clientTelemetryReporter);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            closeQuietly(networkClient, "NetworkClient");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    // Visible for tests
    static KafkaAdminClient createInternal(AdminClientConfig config,
                                           AdminMetadataManager metadataManager,
                                           KafkaClient client,
                                           Time time) {
        Metrics metrics = null;
        String clientId = generateClientId(config);
        Optional<ClientTelemetryReporter> clientTelemetryReporter = CommonClientConfigs.telemetryReporter(clientId, config);

        try {
            metrics = new Metrics(new MetricConfig(), new LinkedList<>(), time);
            LogContext logContext = createLogContext(clientId);
            return new KafkaAdminClient(config, clientId, time, metadataManager, metrics,
                client, null, logContext, clientTelemetryReporter);
        } catch (Throwable exc) {
            closeQuietly(metrics, "Metrics");
            throw new KafkaException("Failed to create new KafkaAdminClient", exc);
        }
    }

    static LogContext createLogContext(String clientId) {
        return new LogContext("[AdminClient clientId=" + clientId + "] ");
    }

    private KafkaAdminClient(AdminClientConfig config,
                             String clientId,
                             Time time,
                             AdminMetadataManager metadataManager,
                             Metrics metrics,
                             KafkaClient client,
                             TimeoutProcessorFactory timeoutProcessorFactory,
                             LogContext logContext,
                             Optional<ClientTelemetryReporter> clientTelemetryReporter) {
        this.clientId = clientId;
        this.log = logContext.logger(KafkaAdminClient.class);
        this.logContext = logContext;
        this.requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        this.defaultApiTimeoutMs = configureDefaultApiTimeoutMs(config);
        this.time = time;
        this.metadataManager = metadataManager;
        this.metrics = metrics;
        this.client = client;
        this.runnable = new AdminClientRunnable();
        String threadName = NETWORK_THREAD_PREFIX + " | " + clientId;
        this.thread = new KafkaThread(threadName, runnable, true);
        this.timeoutProcessorFactory = (timeoutProcessorFactory == null) ?
            new TimeoutProcessorFactory() : timeoutProcessorFactory;
        this.maxRetries = config.getInt(AdminClientConfig.RETRIES_CONFIG);
        this.retryBackoffMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG);
        this.retryBackoffMaxMs = config.getLong(AdminClientConfig.RETRY_BACKOFF_MAX_MS_CONFIG);
        this.retryBackoff = new ExponentialBackoff(
            retryBackoffMs,
            CommonClientConfigs.RETRY_BACKOFF_EXP_BASE,
            retryBackoffMaxMs,
            CommonClientConfigs.RETRY_BACKOFF_JITTER);
        List<MetricsReporter> reporters = CommonClientConfigs.metricsReporters(this.clientId, config);
        this.clientTelemetryReporter = clientTelemetryReporter;
        this.clientTelemetryReporter.ifPresent(reporters::add);
        this.metadataRecoveryStrategy = MetadataRecoveryStrategy.forName(config.getString(AdminClientConfig.METADATA_RECOVERY_STRATEGY_CONFIG));
        this.partitionLeaderCache = new HashMap<>();
        this.adminFetchMetricsManager = new AdminFetchMetricsManager(metrics);
        config.logUnused();
        AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics, time.milliseconds());
        log.debug("Kafka admin client initialized");
        thread.start();
    }

    /**
     * If a default.api.timeout.ms has been explicitly specified, raise an error if it conflicts with request.timeout.ms.
     * If no default.api.timeout.ms has been configured, then set its value as the max of the default and request.timeout.ms. Also we should probably log a warning.
     * Otherwise, use the provided values for both configurations.
     *
     * @param config The configuration
     */
    private int configureDefaultApiTimeoutMs(AdminClientConfig config) {
        int requestTimeoutMs = config.getInt(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG);
        int defaultApiTimeoutMs = config.getInt(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

        if (defaultApiTimeoutMs < requestTimeoutMs) {
            if (config.originals().containsKey(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG)) {
                throw new ConfigException("The specified value of " + AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG +
                        " must be no smaller than the value of " + AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG + ".");
            } else {
                log.warn("Overriding the default value for {} ({}) with the explicitly configured request timeout {}",
                        AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, this.defaultApiTimeoutMs,
                        requestTimeoutMs);
                return requestTimeoutMs;
            }
        }
        return defaultApiTimeoutMs;
    }

    @Override
    public void close(Duration timeout) {
        long waitTimeMs = timeout.toMillis();
        if (waitTimeMs < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");
        waitTimeMs = Math.min(TimeUnit.DAYS.toMillis(365), waitTimeMs); // Limit the timeout to a year.
        long now = time.milliseconds();
        long newHardShutdownTimeMs = now + waitTimeMs;
        long prev = INVALID_SHUTDOWN_TIME;
        clientTelemetryReporter.ifPresent(ClientTelemetryReporter::initiateClose);
        metrics.close();
        while (true) {
            if (hardShutdownTimeMs.compareAndSet(prev, newHardShutdownTimeMs)) {
                if (prev == INVALID_SHUTDOWN_TIME) {
                    log.debug("Initiating close operation.");
                } else {
                    log.debug("Moving hard shutdown time forward.");
                }
                client.wakeup(); // Wake the thread, if it is blocked inside poll().
                break;
            }
            prev = hardShutdownTimeMs.get();
            if (prev < newHardShutdownTimeMs) {
                log.debug("Hard shutdown time is already earlier than requested.");
                newHardShutdownTimeMs = prev;
                break;
            }
        }
        if (log.isDebugEnabled()) {
            long deltaMs = Math.max(0, newHardShutdownTimeMs - time.milliseconds());
            log.debug("Waiting for the I/O thread to exit. Hard shutdown in {} ms.", deltaMs);
        }
        try {
            // close() can be called by AdminClient thread when it invokes callback. That will
            // cause deadlock, so check for that condition.
            if (Thread.currentThread() != thread) {
                // Wait for the thread to be joined.
                thread.join(waitTimeMs);
            }
            log.debug("Kafka admin client closed.");
        } catch (InterruptedException e) {
            log.debug("Interrupted while joining I/O thread", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * An interface for providing a node for a call.
     */
    private interface NodeProvider {
        Node provide();
        boolean supportsUseControllers();
    }

    private class MetadataUpdateNodeIdProvider implements NodeProvider {
        @Override
        public Node provide() {
            long now = time.milliseconds();
            LeastLoadedNode leastLoadedNode = client.leastLoadedNode(now);
            if (metadataRecoveryStrategy == MetadataRecoveryStrategy.REBOOTSTRAP
                    && !leastLoadedNode.hasNodeAvailableOrConnectionReady()) {
                metadataManager.rebootstrap(now);
            }

            return leastLoadedNode.node();
        }

        @Override
        public boolean supportsUseControllers() {
            return true;
        }
    }

    private class ConstantNodeIdProvider implements NodeProvider {
        private final int nodeId;
        private final boolean supportsUseControllers;

        ConstantNodeIdProvider(int nodeId, boolean supportsUseControllers) {
            this.nodeId = nodeId;
            this.supportsUseControllers = supportsUseControllers;
        }

        ConstantNodeIdProvider(int nodeId) {
            this.nodeId = nodeId;
            this.supportsUseControllers = false;
        }

        @Override
        public Node provide() {
            if (metadataManager.isReady() &&
                    (metadataManager.nodeById(nodeId) != null)) {
                return metadataManager.nodeById(nodeId);
            }
            // If we can't find the node with the given constant ID, we schedule a
            // metadata update and hope it appears.  This behavior is useful for avoiding
            // flaky behavior in tests when the cluster is starting up and not all nodes
            // have appeared.
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public boolean supportsUseControllers() {
            return supportsUseControllers;
        }
    }

    /**
     * Provides the controller node.
     */
    private class ControllerNodeProvider implements NodeProvider {
        private final boolean supportsUseControllers;

        ControllerNodeProvider(boolean supportsUseControllers) {
            this.supportsUseControllers = supportsUseControllers;
        }

        ControllerNodeProvider() {
            this.supportsUseControllers = false;
        }

        @Override
        public Node provide() {
            if (metadataManager.isReady() &&
                    (metadataManager.controller() != null)) {
                return metadataManager.controller();
            }
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public boolean supportsUseControllers() {
            return supportsUseControllers;
        }
    }

    /**
     * Provides the least loaded node.
     */
    private class LeastLoadedNodeProvider implements NodeProvider {
        @Override
        public Node provide() {
            if (metadataManager.isReady()) {
                // This may return null if all nodes are busy.
                // In that case, we will postpone node assignment.
                return client.leastLoadedNode(time.milliseconds()).node();
            }
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public boolean supportsUseControllers() {
            return false;
        }
    }

    /**
     * Provides the least loaded broker, or the active kcontroller if we're using
     * bootstrap.controllers.
     */
    private class ConstantBrokerOrActiveKController implements NodeProvider {
        private final int nodeId;

        ConstantBrokerOrActiveKController(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Node provide() {
            if (metadataManager.isReady()) {
                if (metadataManager.usingBootstrapControllers()) {
                    return metadataManager.controller();
                } else if (metadataManager.nodeById(nodeId) != null) {
                    return metadataManager.nodeById(nodeId);
                }
            }
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public boolean supportsUseControllers() {
            return true;
        }
    }

    /**
     * Provides the least loaded broker, or the active kcontroller if we're using
     * bootstrap.controllers.
     */
    private class LeastLoadedBrokerOrActiveKController implements NodeProvider {
        @Override
        public Node provide() {
            if (metadataManager.isReady()) {
                if (metadataManager.usingBootstrapControllers()) {
                    return metadataManager.controller();
                } else {
                    // This may return null if all nodes are busy.
                    // In that case, we will postpone node assignment.
                    return client.leastLoadedNode(time.milliseconds()).node();
                }
            }
            metadataManager.requestUpdate();
            return null;
        }

        @Override
        public boolean supportsUseControllers() {
            return true;
        }
    }

    abstract class Call {
        private final boolean internal;
        private final String callName;
        private final long deadlineMs;
        private final NodeProvider nodeProvider;
        protected int tries;
        private Node curNode = null;
        private long nextAllowedTryMs;

        Call(boolean internal,
             String callName,
             long nextAllowedTryMs,
             int tries,
             long deadlineMs,
             NodeProvider nodeProvider
        ) {
            this.internal = internal;
            this.callName = callName;
            this.nextAllowedTryMs = nextAllowedTryMs;
            this.tries = tries;
            this.deadlineMs = deadlineMs;
            this.nodeProvider = nodeProvider;
        }

        Call(boolean internal, String callName, long deadlineMs, NodeProvider nodeProvider) {
            this(internal, callName, 0, 0, deadlineMs, nodeProvider);
        }

        Call(String callName, long deadlineMs, NodeProvider nodeProvider) {
            this(false, callName, 0, 0, deadlineMs, nodeProvider);
        }

        Call(String callName, long nextAllowedTryMs, int tries, long deadlineMs, NodeProvider nodeProvider) {
            this(false, callName, nextAllowedTryMs, tries, deadlineMs, nodeProvider);
        }

        protected Node curNode() {
            return curNode;
        }

        /**
         * Handle a failure.
         *
         * Depending on what the exception is and how many times we have already tried, we may choose to
         * fail the Call, or retry it. It is important to print the stack traces here in some cases,
         * since they are not necessarily preserved in ApiVersionException objects.
         *
         * @param now           The current time in milliseconds.
         * @param throwable     The failure exception.
         */
        final void fail(long now, Throwable throwable) {
            if (curNode != null) {
                runnable.nodeReadyDeadlines.remove(curNode);
                curNode = null;
            }
            // If the admin client is closing, we can't retry.
            if (runnable.closing) {
                handleFailure(throwable);
                return;
            }
            // If this is an UnsupportedVersionException that we can retry, do so. Note that a
            // protocol downgrade will not count against the total number of retries we get for
            // this RPC. That is why 'tries' is not incremented.
            if ((throwable instanceof UnsupportedVersionException) &&
                     handleUnsupportedVersionException((UnsupportedVersionException) throwable)) {
                log.debug("{} attempting protocol downgrade and then retry.", this);
                runnable.pendingCalls.add(this);
                return;
            }
            nextAllowedTryMs = now + retryBackoff.backoff(tries++);

            // If the call has timed out, fail.
            if (calcTimeoutMsRemainingAsInt(now, deadlineMs) <= 0) {
                handleTimeoutFailure(now, throwable);
                return;
            }
            // If the exception is not retriable, fail.
            if (!(throwable instanceof RetriableException)) {
                if (log.isDebugEnabled()) {
                    log.debug("{} failed with non-retriable exception after {} attempt(s)", this, tries,
                        new Exception(prettyPrintException(throwable)));
                }
                handleFailure(throwable);
                return;
            }
            // If we are out of retries, fail.
            if (tries > maxRetries) {
                handleTimeoutFailure(now, throwable);
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("{} failed: {}. Beginning retry #{}",
                    this, prettyPrintException(throwable), tries);
            }
            maybeRetry(now, throwable);
        }

        void maybeRetry(long now, Throwable throwable) {
            runnable.pendingCalls.add(this);
        }

        private void handleTimeoutFailure(long now, Throwable cause) {
            if (log.isDebugEnabled()) {
                log.debug("{} timed out at {} after {} attempt(s)", this, now, tries,
                    new Exception(prettyPrintException(cause)));
            }
            if (cause instanceof TimeoutException) {
                handleFailure(cause);
            } else {
                handleFailure(new TimeoutException(this + " timed out at " + now
                    + " after " + tries + " attempt(s)", cause));
            }
        }

        /**
         * Create an AbstractRequest.Builder for this Call.
         *
         * @param timeoutMs The timeout in milliseconds.
         *
         * @return          The AbstractRequest builder.
         */
        abstract AbstractRequest.Builder<?> createRequest(int timeoutMs);

        /**
         * Process the call response.
         *
         * @param abstractResponse  The AbstractResponse.
         *
         */
        abstract void handleResponse(AbstractResponse abstractResponse);

        /**
         * Handle a failure. This will only be called if the failure exception was not
         * retriable, or if we hit a timeout.
         *
         * @param throwable     The exception.
         */
        abstract void handleFailure(Throwable throwable);

        /**
         * Handle an UnsupportedVersionException.
         *
         * @param exception     The exception.
         *
         * @return              True if the exception can be handled; false otherwise.
         */
        boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
            return false;
        }

        @Override
        public String toString() {
            return "Call(callName=" + callName + ", deadlineMs=" + deadlineMs +
                ", tries=" + tries + ", nextAllowedTryMs=" + nextAllowedTryMs + ")";
        }

        public boolean isInternal() {
            return internal;
        }
    }

    static class TimeoutProcessorFactory {
        TimeoutProcessor create(long now) {
            return new TimeoutProcessor(now);
        }
    }

    static class TimeoutProcessor {
        /**
         * The current time in milliseconds.
         */
        private final long now;

        /**
         * The number of milliseconds until the next timeout.
         */
        private int nextTimeoutMs;

        /**
         * Create a new timeout processor.
         *
         * @param now           The current time in milliseconds since the epoch.
         */
        TimeoutProcessor(long now) {
            this.now = now;
            this.nextTimeoutMs = Integer.MAX_VALUE;
        }

        /**
         * Check for calls which have timed out.
         * Timed out calls will be removed and failed.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param calls         The collection of calls.
         *
         * @return              The number of calls which were timed out.
         */
        int handleTimeouts(Collection<Call> calls, String msg) {
            int numTimedOut = 0;
            for (Iterator<Call> iter = calls.iterator(); iter.hasNext(); ) {
                Call call = iter.next();
                int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
                if (remainingMs < 0) {
                    call.fail(now, new TimeoutException(msg + " Call: " + call.callName));
                    iter.remove();
                    numTimedOut++;
                } else {
                    nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
                }
            }
            return numTimedOut;
        }

        /**
         * Check whether a call should be timed out.
         * The remaining milliseconds until the next timeout will be updated.
         *
         * @param call      The call.
         *
         * @return          True if the call should be timed out.
         */
        boolean callHasExpired(Call call) {
            int remainingMs = calcTimeoutMsRemainingAsInt(now, call.deadlineMs);
            if (remainingMs < 0)
                return true;
            nextTimeoutMs = Math.min(nextTimeoutMs, remainingMs);
            return false;
        }

        int nextTimeoutMs() {
            return nextTimeoutMs;
        }
    }

    private final class AdminClientRunnable implements Runnable {
        /**
         * Calls which have not yet been assigned to a node.
         * Only accessed from this thread.
         */
        private final ArrayList<Call> pendingCalls = new ArrayList<>();

        /**
         * Maps nodes to calls that we want to send.
         * Only accessed from this thread.
         */
        private final Map<Node, List<Call>> callsToSend = new HashMap<>();

        /**
         * Maps node ID strings to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<String, Call> callsInFlight = new HashMap<>();

        /**
         * Maps correlation IDs to calls that have been sent.
         * Only accessed from this thread.
         */
        private final Map<Integer, Call> correlationIdToCalls = new HashMap<>();

        /**
         * Pending calls. Protected by the object monitor.
         */
        private final List<Call> newCalls = new LinkedList<>();

        /**
         * Maps node ID strings to their readiness deadlines.  A node will appear in this
         * map if there are callsToSend which are waiting for it to be ready, and there
         * are no calls in flight using the node.
         */
        private final Map<Node, Long> nodeReadyDeadlines = new HashMap<>();

        /**
         * Whether the admin client is closing.
         */
        private volatile boolean closing = false;

        /**
         * Time out the elements in the pendingCalls list which are expired.
         *
         * @param processor     The timeout processor.
         */
        private void timeoutPendingCalls(TimeoutProcessor processor) {
            int numTimedOut = processor.handleTimeouts(pendingCalls, "Timed out waiting for a node assignment.");
            if (numTimedOut > 0)
                log.debug("Timed out {} pending calls.", numTimedOut);
        }

        /**
         * Time out calls which have been assigned to nodes.
         *
         * @param processor     The timeout processor.
         */
        private int timeoutCallsToSend(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (List<Call> callList : callsToSend.values()) {
                numTimedOut += processor.handleTimeouts(callList,
                    "Timed out waiting to send the call.");
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) with assigned nodes.", numTimedOut);
            return numTimedOut;
        }

        /**
         * Drain all the calls from newCalls into pendingCalls.
         *
         * This function holds the lock for the minimum amount of time, to avoid blocking
         * users of AdminClient who will also take the lock to add new calls.
         */
        private synchronized void drainNewCalls() {
            transitionToPendingAndClearList(newCalls);
        }

        /**
         * Add some calls to pendingCalls, and then clear the input list.
         * Also clears Call#curNode.
         *
         * @param calls         The calls to add.
         */
        private void transitionToPendingAndClearList(List<Call> calls) {
            for (Call call : calls) {
                call.curNode = null;
                pendingCalls.add(call);
            }
            calls.clear();
        }

        /**
         * Choose nodes for the calls in the pendingCalls list.
         *
         * @param now           The current time in milliseconds.
         * @return              The minimum time until a call is ready to be retried if any of the pending
         *                      calls are backing off after a failure
         */
        private long maybeDrainPendingCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            log.trace("Trying to choose nodes for {} at {}", pendingCalls, now);

            List<Call> toRemove = new ArrayList<>();
            // Using pendingCalls.size() to get the list size before the for-loop to avoid infinite loop.
            // If call.fail keeps adding the call to pendingCalls,
            // the loop like for (int i = 0; i < pendingCalls.size(); i++) can't stop.
            int pendingSize = pendingCalls.size();
            // pendingCalls could be modified in this loop,
            // hence using for-loop instead of iterator to avoid ConcurrentModificationException.
            for (int i = 0; i < pendingSize; i++) {
                Call call = pendingCalls.get(i);
                // If the call is being retried, await the proper backoff before finding the node
                if (now < call.nextAllowedTryMs) {
                    pollTimeout = Math.min(pollTimeout, call.nextAllowedTryMs - now);
                } else if (maybeDrainPendingCall(call, now)) {
                    toRemove.add(call);
                }
            }

            // Use remove instead of removeAll to avoid delete all matched elements
            for (Call call : toRemove) {
                pendingCalls.remove(call);
            }
            return pollTimeout;
        }

        /**
         * Check whether a pending call can be assigned a node. Return true if the pending call was either
         * transferred to the callsToSend collection or if the call was failed. Return false if it
         * should remain pending.
         */
        private boolean maybeDrainPendingCall(Call call, long now) {
            try {
                Node node = call.nodeProvider.provide();
                if (node != null) {
                    log.trace("Assigned {} to node {}", call, node);
                    call.curNode = node;
                    getOrCreateListValue(callsToSend, node).add(call);
                    return true;
                } else {
                    log.trace("Unable to assign {} to a node.", call);
                    return false;
                }
            } catch (Throwable t) {
                // Handle authentication errors while choosing nodes.
                log.debug("Unable to choose node for {}", call, t);
                call.fail(now, t);
                return true;
            }
        }

        /**
         * Send the calls which are ready.
         *
         * @param now                   The current time in milliseconds.
         * @return                      The minimum timeout we need for poll().
         */
        private long sendEligibleCalls(long now) {
            long pollTimeout = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                List<Call> calls = entry.getValue();
                if (calls.isEmpty()) {
                    iter.remove();
                    continue;
                }
                Node node = entry.getKey();
                if (callsInFlight.containsKey(node.idString())) {
                    log.trace("Still waiting for other calls to finish on node {}.", node);
                    nodeReadyDeadlines.remove(node);
                    continue;
                }
                if (!client.ready(node, now)) {
                    Long deadline = nodeReadyDeadlines.get(node);
                    if (deadline != null) {
                        if (now >= deadline) {
                            log.info("Disconnecting from {} and revoking {} node assignment(s) " +
                                "because the node is taking too long to become ready.",
                                node.idString(), calls.size());
                            transitionToPendingAndClearList(calls);
                            client.disconnect(node.idString());
                            nodeReadyDeadlines.remove(node);
                            iter.remove();
                            continue;
                        }
                        pollTimeout = Math.min(pollTimeout, deadline - now);
                    } else {
                        nodeReadyDeadlines.put(node, now + requestTimeoutMs);
                    }
                    long nodeTimeout = client.pollDelayMs(node, now);
                    pollTimeout = Math.min(pollTimeout, nodeTimeout);
                    log.trace("Client is not ready to send to {}. Must delay {} ms", node, nodeTimeout);
                    continue;
                }
                // Subtract the time we spent waiting for the node to become ready from
                // the total request time.
                int remainingRequestTime;
                Long deadlineMs = nodeReadyDeadlines.remove(node);
                if (deadlineMs == null) {
                    remainingRequestTime = requestTimeoutMs;
                } else {
                    remainingRequestTime = calcTimeoutMsRemainingAsInt(now, deadlineMs);
                }
                while (!calls.isEmpty()) {
                    Call call = calls.remove(0);
                    int timeoutMs = Math.min(remainingRequestTime,
                        calcTimeoutMsRemainingAsInt(now, call.deadlineMs));
                    AbstractRequest.Builder<?> requestBuilder;
                    try {
                        requestBuilder = call.createRequest(timeoutMs);
                    } catch (Throwable t) {
                        call.fail(now, new KafkaException(String.format(
                            "Internal error sending %s to %s.", call.callName, node), t));
                        continue;
                    }
                    ClientRequest clientRequest = client.newClientRequest(node.idString(),
                        requestBuilder, now, true, timeoutMs, null);
                    log.debug("Sending {} to {}. correlationId={}, timeoutMs={}",
                        requestBuilder, node, clientRequest.correlationId(), timeoutMs);
                    client.send(clientRequest, now);
                    callsInFlight.put(node.idString(), call);
                    correlationIdToCalls.put(clientRequest.correlationId(), call);
                    break;
                }
            }
            return pollTimeout;
        }

        /**
         * Time out expired calls that are in flight.
         *
         * Calls that are in flight may have been partially or completely sent over the wire. They may
         * even be in the process of being processed by the remote server. At the moment, our only option
         * to time them out is to close the entire connection.
         *
         * @param processor         The timeout processor.
         */
        private void timeoutCallsInFlight(TimeoutProcessor processor) {
            int numTimedOut = 0;
            for (Map.Entry<String, Call> entry : callsInFlight.entrySet()) {
                Call call = entry.getValue();
                String nodeId = entry.getKey();
                if (processor.callHasExpired(call)) {
                    log.info("Disconnecting from {} due to timeout while awaiting {}", nodeId, call);
                    client.disconnect(nodeId);
                    numTimedOut++;
                    // We don't remove anything from the callsInFlight data structure. Because the connection
                    // has been closed, the calls should be returned by the next client#poll(),
                    // and handled at that point.
                }
            }
            if (numTimedOut > 0)
                log.debug("Timed out {} call(s) in flight.", numTimedOut);
        }

        /**
         * Handle responses from the server.
         *
         * @param now                   The current time in milliseconds.
         * @param responses             The latest responses from KafkaClient.
         */
        private void handleResponses(long now, List<ClientResponse> responses) {
            for (ClientResponse response : responses) {
                int correlationId = response.requestHeader().correlationId();

                Call call = correlationIdToCalls.get(correlationId);
                if (call == null) {
                    // If the server returns information about a correlation ID we didn't use yet,
                    // an internal server error has occurred. Close the connection and log an error message.
                    log.error("Internal server error on {}: server returned information about unknown " +
                        "correlation ID {}, requestHeader = {}", response.destination(), correlationId,
                        response.requestHeader());
                    client.disconnect(response.destination());
                    continue;
                }

                // Stop tracking this call.
                correlationIdToCalls.remove(correlationId);
                if (!callsInFlight.remove(response.destination(), call)) {
                    log.error("Internal server error on {}: ignoring call {} in correlationIdToCall " +
                        "that did not exist in callsInFlight", response.destination(), call);
                    continue;
                }

                // Handle the result of the call. This may involve retrying the call, if we got a
                // retriable exception.
                if (response.versionMismatch() != null) {
                    call.fail(now, response.versionMismatch());
                } else if (response.wasDisconnected()) {
                    AuthenticationException authException = client.authenticationException(call.curNode());
                    if (authException != null) {
                        call.fail(now, authException);
                    } else {
                        call.fail(now, new DisconnectException(String.format(
                            "Cancelled %s request with correlation id %d due to node %s being disconnected",
                            call.callName, correlationId, response.destination())));
                    }
                } else {
                    try {
                        call.handleResponse(response.responseBody());
                        adminFetchMetricsManager.recordLatency(response.destination(), response.requestLatencyMs());
                        if (log.isTraceEnabled())
                            log.trace("{} got response {}", call, response.responseBody());
                    } catch (Throwable t) {
                        if (log.isTraceEnabled())
                            log.trace("{} handleResponse failed with {}", call, prettyPrintException(t));
                        call.fail(now, t);
                    }
                }
            }
        }

        /**
         * Unassign calls that have not yet been sent based on some predicate. For example, this
         * is used to reassign the calls that have been assigned to a disconnected node.
         *
         * @param shouldUnassign Condition for reassignment. If the predicate is true, then the calls will
         *                       be put back in the pendingCalls collection and they will be reassigned
         */
        private void unassignUnsentCalls(Predicate<Node> shouldUnassign) {
            for (Iterator<Map.Entry<Node, List<Call>>> iter = callsToSend.entrySet().iterator(); iter.hasNext(); ) {
                Map.Entry<Node, List<Call>> entry = iter.next();
                Node node = entry.getKey();
                List<Call> awaitingCalls = entry.getValue();

                if (awaitingCalls.isEmpty()) {
                    iter.remove();
                } else if (shouldUnassign.test(node)) {
                    nodeReadyDeadlines.remove(node);
                    transitionToPendingAndClearList(awaitingCalls);
                    iter.remove();
                }
            }
        }

        private boolean hasActiveExternalCalls(Collection<Call> calls) {
            for (Call call : calls) {
                if (!call.isInternal()) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Return true if there are currently active external calls.
         */
        private boolean hasActiveExternalCalls() {
            if (hasActiveExternalCalls(pendingCalls)) {
                return true;
            }
            for (List<Call> callList : callsToSend.values()) {
                if (hasActiveExternalCalls(callList)) {
                    return true;
                }
            }
            return hasActiveExternalCalls(correlationIdToCalls.values());
        }

        private boolean threadShouldExit(long now, long curHardShutdownTimeMs) {
            if (!hasActiveExternalCalls()) {
                log.trace("All work has been completed, and the I/O thread is now exiting.");
                return true;
            }
            if (now >= curHardShutdownTimeMs) {
                log.info("Forcing a hard I/O thread shutdown. Requests in progress will be aborted.");
                return true;
            }
            log.debug("Hard shutdown in {} ms.", curHardShutdownTimeMs - now);
            return false;
        }

        @Override
        public void run() {
            log.debug("Thread starting");
            try {
                processRequests();
            } finally {
                closing = true;
                AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);

                int numTimedOut = 0;
                TimeoutProcessor timeoutProcessor = new TimeoutProcessor(Long.MAX_VALUE);
                synchronized (this) {
                    numTimedOut += timeoutProcessor.handleTimeouts(newCalls, "The AdminClient thread has exited.");
                }
                numTimedOut += timeoutProcessor.handleTimeouts(pendingCalls, "The AdminClient thread has exited.");
                numTimedOut += timeoutCallsToSend(timeoutProcessor);
                numTimedOut += timeoutProcessor.handleTimeouts(correlationIdToCalls.values(),
                        "The AdminClient thread has exited.");
                if (numTimedOut > 0) {
                    log.info("Timed out {} remaining operation(s) during close.", numTimedOut);
                }
                closeQuietly(client, "KafkaClient");
                closeQuietly(metrics, "Metrics");
                log.debug("Exiting AdminClientRunnable thread.");
            }
        }

        private void processRequests() {
            long now = time.milliseconds();
            while (true) {
                // Copy newCalls into pendingCalls.
                drainNewCalls();

                // Check if the AdminClient thread should shut down.
                long curHardShutdownTimeMs = hardShutdownTimeMs.get();
                if ((curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) && threadShouldExit(now, curHardShutdownTimeMs))
                    break;

                // Handle timeouts.
                TimeoutProcessor timeoutProcessor = timeoutProcessorFactory.create(now);
                timeoutPendingCalls(timeoutProcessor);
                timeoutCallsToSend(timeoutProcessor);
                timeoutCallsInFlight(timeoutProcessor);

                long pollTimeout = Math.min(1200000, timeoutProcessor.nextTimeoutMs());
                if (curHardShutdownTimeMs != INVALID_SHUTDOWN_TIME) {
                    pollTimeout = Math.min(pollTimeout, curHardShutdownTimeMs - now);
                }

                // Choose nodes for our pending calls.
                pollTimeout = Math.min(pollTimeout, maybeDrainPendingCalls(now));
                long metadataFetchDelayMs = metadataManager.metadataFetchDelayMs(now);
                if (metadataFetchDelayMs == 0) {
                    metadataManager.transitionToUpdatePending(now);
                    Call metadataCall = makeMetadataCall(now);
                    // Create a new metadata fetch call and add it to the end of pendingCalls.
                    // Assign a node for just the new call (we handled the other pending nodes above).

                    if (!maybeDrainPendingCall(metadataCall, now))
                        pendingCalls.add(metadataCall);
                }
                pollTimeout = Math.min(pollTimeout, sendEligibleCalls(now));

                if (metadataFetchDelayMs > 0) {
                    pollTimeout = Math.min(pollTimeout, metadataFetchDelayMs);
                }

                // Ensure that we use a small poll timeout if there are pending calls which need to be sent
                if (!pendingCalls.isEmpty())
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);

                // Wait for network responses.
                log.trace("Entering KafkaClient#poll(timeout={})", pollTimeout);
                List<ClientResponse> responses = client.poll(Math.max(0L, pollTimeout), now);
                log.trace("KafkaClient#poll retrieved {} response(s)", responses.size());

                // unassign calls to disconnected nodes
                unassignUnsentCalls(client::connectionFailed);

                // Update the current time and handle the latest responses.
                now = time.milliseconds();
                handleResponses(now, responses);
            }
        }

        /**
         * Queue a call for sending.
         *
         * If the AdminClient thread has exited, this will fail. Otherwise, it will succeed (even
         * if the AdminClient is shutting down). This function should called when retrying an
         * existing call.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void enqueue(Call call, long now) {
            if (call.tries > maxRetries) {
                log.debug("Max retries {} for {} reached", maxRetries, call);
                call.handleTimeoutFailure(time.milliseconds(), new TimeoutException(
                    "Exceeded maxRetries after " + call.tries + " tries."));
                return;
            }
            if (log.isDebugEnabled()) {
                log.debug("Queueing {} with a timeout {} ms from now.", call,
                    Math.min(requestTimeoutMs, call.deadlineMs - now));
            }
            boolean accepted = false;
            synchronized (this) {
                if (!closing) {
                    newCalls.add(call);
                    accepted = true;
                }
            }
            if (accepted) {
                client.wakeup(); // wake the thread if it is in poll()
            } else {
                log.debug("The AdminClient thread has exited. Timing out {}.", call);
                call.handleTimeoutFailure(time.milliseconds(),
                    new TimeoutException("The AdminClient thread has exited."));
            }
        }

        /**
         * Initiate a new call.
         *
         * This will fail if the AdminClient is scheduled to shut down.
         *
         * @param call      The new call object.
         * @param now       The current time in milliseconds.
         */
        void call(Call call, long now) {
            if (hardShutdownTimeMs.get() != INVALID_SHUTDOWN_TIME) {
                log.debug("Cannot accept new call {} when AdminClient is closing.", call);
                call.handleFailure(new IllegalStateException("Cannot accept new calls when AdminClient is closing."));
            } else if (metadataManager.usingBootstrapControllers() &&
                    (!call.nodeProvider.supportsUseControllers())) {
                call.fail(now, new UnsupportedEndpointTypeException("This Admin API is not " +
                    "yet supported when communicating directly with the controller quorum."));
            } else {
                enqueue(call, now);
            }
        }

        /**
         * Create a new metadata call.
         */
        private Call makeMetadataCall(long now) {
            if (metadataManager.usingBootstrapControllers()) {
                return makeControllerMetadataCall(now);
            } else {
                return makeBrokerMetadataCall(now);
            }
        }

        private Call makeControllerMetadataCall(long now) {
            // Use DescribeCluster here, as specified by KIP-919.
            return new Call(true, "describeCluster", calcDeadlineMs(now, requestTimeoutMs),
                    new MetadataUpdateNodeIdProvider()) {
                @Override
                public DescribeClusterRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
                        .setIncludeClusterAuthorizedOperations(false)
                        .setEndpointType(EndpointType.CONTROLLER.id()));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeClusterResponse response = (DescribeClusterResponse) abstractResponse;
                    Cluster cluster;
                    try {
                        cluster = parseDescribeClusterResponse(response.data());
                    } catch (ApiException e) {
                        handleFailure(e);
                        return;
                    }
                    long now = time.milliseconds();
                    metadataManager.update(cluster, now);

                    // Unassign all unsent requests after a metadata refresh to allow for a new
                    // destination to be selected from the new metadata
                    unassignUnsentCalls(node -> true);
                }

                @Override
                boolean handleUnsupportedVersionException(final UnsupportedVersionException e) {
                    metadataManager.updateFailed(e);
                    return false;
                }

                @Override
                public void handleFailure(Throwable e) {
                    metadataManager.updateFailed(e);
                }
            };
        }

        private Call makeBrokerMetadataCall(long now) {
            // We use MetadataRequest here so that we can continue to support brokers that are too
            // old to handle DescribeCluster.
            return new Call(true, "fetchMetadata", calcDeadlineMs(now, requestTimeoutMs),
                    new MetadataUpdateNodeIdProvider()) {
                @Override
                public MetadataRequest.Builder createRequest(int timeoutMs) {
                    // Since this only requests node information, it's safe to pass true
                    // for allowAutoTopicCreation (and it simplifies communication with
                    // older brokers)
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(Collections.emptyList())
                        .setAllowAutoTopicCreation(true));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    MetadataResponse response = (MetadataResponse) abstractResponse;
                    long now = time.milliseconds();

                    if (response.topLevelError() == Errors.REBOOTSTRAP_REQUIRED)
                        metadataManager.initiateRebootstrap();
                    else
                        metadataManager.update(response.buildCluster(), now);

                    // Unassign all unsent requests after a metadata refresh to allow for a new
                    // destination to be selected from the new metadata
                    unassignUnsentCalls(node -> true);
                }

                @Override
                boolean handleUnsupportedVersionException(final UnsupportedVersionException e) {
                    metadataManager.updateFailed(e);
                    return false;
                }

                @Override
                public void handleFailure(Throwable e) {
                    metadataManager.updateFailed(e);
                }
            };
        }
    }

    static Cluster parseDescribeClusterResponse(DescribeClusterResponseData response) {
        ApiError apiError = new ApiError(response.errorCode(), response.errorMessage());
        if (apiError.isFailure()) {
            throw apiError.exception();
        }
        if (response.endpointType() != EndpointType.CONTROLLER.id()) {
            throw new MismatchedEndpointTypeException("Expected response from CONTROLLER " +
                "endpoint, but got response from endpoint type " + (int) response.endpointType());
        }
        List<Node> nodes = new ArrayList<>();
        Node controllerNode = null;
        for (DescribeClusterResponseData.DescribeClusterBroker node : response.brokers()) {
            Node newNode = new Node(node.brokerId(), node.host(), node.port(), node.rack());
            nodes.add(newNode);
            if (node.brokerId() == response.controllerId()) {
                controllerNode = newNode;
            }
        }
        return new Cluster(response.clusterId(),
            nodes,
            Collections.emptyList(),
            Collections.emptySet(),
            Collections.emptySet(),
            controllerNode);
    }

    /**
     * Returns true if a topic name cannot be represented in an RPC.  This function does NOT check
     * whether the name is too long, contains invalid characters, etc.  It is better to enforce
     * those policies on the server, so that they can be changed in the future if needed.
     */
    private static boolean topicNameIsUnrepresentable(String topicName) {
        return topicName == null || topicName.isEmpty();
    }

    private static boolean topicIdIsUnrepresentable(Uuid topicId) {
        return topicId == null || topicId.equals(Uuid.ZERO_UUID);
    }

    // for testing
    int numPendingCalls() {
        return runnable.pendingCalls.size();
    }

    /**
     * Fail futures in the given stream which are not done.
     * Used when a response handler expected a result for some entity but no result was present.
     */
    private static <K, V> void completeUnrealizedFutures(
            Stream<Map.Entry<K, KafkaFutureImpl<V>>> futures,
            Function<K, String> messageFormatter) {
        futures.filter(entry -> !entry.getValue().isDone()).forEach(entry ->
                entry.getValue().completeExceptionally(new ApiException(messageFormatter.apply(entry.getKey()))));
    }

    /**
     * Fail futures in the given Map which were retried due to exceeding quota. We propagate
     * the initial error back to the caller if the request timed out.
     */
    private static <K, V> void maybeCompleteQuotaExceededException(
            boolean shouldRetryOnQuotaViolation,
            Throwable throwable,
            Map<K, KafkaFutureImpl<V>> futures,
            Map<K, ThrottlingQuotaExceededException> quotaExceededExceptions,
            int throttleTimeDelta) {
        if (shouldRetryOnQuotaViolation && throwable instanceof TimeoutException) {
            quotaExceededExceptions.forEach((key, value) -> futures.get(key).completeExceptionally(
                new ThrottlingQuotaExceededException(
                    Math.max(0, value.throttleTimeMs() - throttleTimeDelta),
                    value.getMessage())));
        }
    }

    @Override
    public CreateTopicsResult createTopics(final Collection<NewTopic> newTopics,
                                           final CreateTopicsOptions options) {
        final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> topicFutures = new HashMap<>(newTopics.size());
        final CreatableTopicCollection topics = new CreatableTopicCollection();
        for (NewTopic newTopic : newTopics) {
            if (topicNameIsUnrepresentable(newTopic.name())) {
                KafkaFutureImpl<TopicMetadataAndConfig> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    newTopic.name() + "' cannot be represented in a request."));
                topicFutures.put(newTopic.name(), future);
            } else if (!topicFutures.containsKey(newTopic.name())) {
                topicFutures.put(newTopic.name(), new KafkaFutureImpl<>());
                topics.add(newTopic.convertToCreatableTopic());
            }
        }
        if (!topics.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            final Call call = getCreateTopicsCall(options, topicFutures, topics,
                Collections.emptyMap(), now, deadline);
            runnable.call(call, now);
        }
        return new CreateTopicsResult(new HashMap<>(topicFutures));
    }

    private Call getCreateTopicsCall(final CreateTopicsOptions options,
                                     final Map<String, KafkaFutureImpl<TopicMetadataAndConfig>> futures,
                                     final CreatableTopicCollection topics,
                                     final Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                     final long now,
                                     final long deadline) {
        return new Call("createTopics", deadline, new ControllerNodeProvider()) {
            @Override
            public CreateTopicsRequest.Builder createRequest(int timeoutMs) {
                return new CreateTopicsRequest.Builder(
                    new CreateTopicsRequestData()
                        .setTopics(topics)
                        .setTimeoutMs(timeoutMs)
                        .setValidateOnly(options.shouldValidateOnly()));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse);
                // Handle server responses for particular topics.
                final CreateTopicsResponse response = (CreateTopicsResponse) abstractResponse;
                final CreatableTopicCollection retryTopics = new CreatableTopicCollection();
                final Map<String, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                for (CreatableTopicResult result : response.data().topics()) {
                    KafkaFutureImpl<TopicMetadataAndConfig> future = futures.get(result.name());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                    response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    retryTopics.add(topics.find(result.name()).duplicate());
                                    retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
                                } else {
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            TopicMetadataAndConfig topicMetadataAndConfig;
                            if (result.topicConfigErrorCode() != Errors.NONE.code()) {
                                topicMetadataAndConfig = new TopicMetadataAndConfig(
                                    Errors.forCode(result.topicConfigErrorCode()).exception());
                            } else if (result.numPartitions() == CreateTopicsResult.UNKNOWN) {
                                topicMetadataAndConfig = new TopicMetadataAndConfig(new UnsupportedVersionException(
                                    "Topic metadata and configs in CreateTopics response not supported"));
                            } else {
                                List<CreatableTopicConfigs> configs = result.configs();
                                Config topicConfig = new Config(configs.stream()
                                    .map(this::configEntry)
                                    .collect(Collectors.toSet()));
                                topicMetadataAndConfig = new TopicMetadataAndConfig(result.topicId(), result.numPartitions(),
                                    result.replicationFactor(),
                                    topicConfig);
                            }
                            future.complete(topicMetadataAndConfig);
                        }
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    final long now = time.milliseconds();
                    final Call call = getCreateTopicsCall(options, futures, retryTopics,
                        retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            private ConfigEntry configEntry(CreatableTopicConfigs config) {
                return new ConfigEntry(
                    config.name(),
                    config.value(),
                    configSource(DescribeConfigsResponse.ConfigSource.forId(config.configSource())),
                    config.isSensitive(),
                    config.readOnly(),
                    Collections.emptyList(),
                    null,
                    null);
            }

            @Override
            void handleFailure(Throwable throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                    throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    @Override
    public DeleteTopicsResult deleteTopics(final TopicCollection topics,
                                           final DeleteTopicsOptions options) {
        if (topics instanceof TopicIdCollection)
            return DeleteTopicsResult.ofTopicIds(handleDeleteTopicsUsingIds(((TopicIdCollection) topics).topicIds(), options));
        else if (topics instanceof TopicNameCollection)
            return DeleteTopicsResult.ofTopicNames(handleDeleteTopicsUsingNames(((TopicNameCollection) topics).topicNames(), options));
        else
            throw new IllegalArgumentException("The TopicCollection: " + topics + " provided did not match any supported classes for deleteTopics.");
    }

    private Map<String, KafkaFuture<Void>> handleDeleteTopicsUsingNames(final Collection<String> topicNames,
                                                                        final DeleteTopicsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicNames.size());
        final List<String> validTopicNames = new ArrayList<>(topicNames.size());
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures.put(topicName, new KafkaFutureImpl<>());
                validTopicNames.add(topicName);
            }
        }
        if (!validTopicNames.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            final Call call = getDeleteTopicsCall(options, topicFutures, validTopicNames,
                Collections.emptyMap(), now, deadline);
            runnable.call(call, now);
        }
        return new HashMap<>(topicFutures);
    }

    private Map<Uuid, KafkaFuture<Void>> handleDeleteTopicsUsingIds(final Collection<Uuid> topicIds,
                                                                    final DeleteTopicsOptions options) {
        final Map<Uuid, KafkaFutureImpl<Void>> topicFutures = new HashMap<>(topicIds.size());
        final List<Uuid> validTopicIds = new ArrayList<>(topicIds.size());
        for (Uuid topicId : topicIds) {
            if (topicId.equals(Uuid.ZERO_UUID)) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic ID '" +
                    topicId + "' cannot be represented in a request."));
                topicFutures.put(topicId, future);
            } else if (!topicFutures.containsKey(topicId)) {
                topicFutures.put(topicId, new KafkaFutureImpl<>());
                validTopicIds.add(topicId);
            }
        }
        if (!validTopicIds.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            final Call call = getDeleteTopicsWithIdsCall(options, topicFutures, validTopicIds,
                Collections.emptyMap(), now, deadline);
            runnable.call(call, now);
        }
        return new HashMap<>(topicFutures);
    }

    private Call getDeleteTopicsCall(final DeleteTopicsOptions options,
                                     final Map<String, KafkaFutureImpl<Void>> futures,
                                     final List<String> topics,
                                     final Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                     final long now,
                                     final long deadline) {
        return new Call("deleteTopics", deadline, new ControllerNodeProvider()) {
            @Override
            DeleteTopicsRequest.Builder createRequest(int timeoutMs) {
                return new DeleteTopicsRequest.Builder(
                    new DeleteTopicsRequestData()
                        .setTopicNames(topics)
                        .setTimeoutMs(timeoutMs));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse);
                // Handle server responses for particular topics.
                final DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
                final List<String> retryTopics = new ArrayList<>();
                final Map<String, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                for (DeletableTopicResult result : response.data().responses()) {
                    KafkaFutureImpl<Void> future = futures.get(result.name());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                    response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    retryTopics.add(result.name());
                                    retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
                                } else {
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            future.complete(null);
                        }
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    final long now = time.milliseconds();
                    final Call call = getDeleteTopicsCall(options, futures, retryTopics,
                        retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                    throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    private Call getDeleteTopicsWithIdsCall(final DeleteTopicsOptions options,
                                            final Map<Uuid, KafkaFutureImpl<Void>> futures,
                                            final List<Uuid> topicIds,
                                            final Map<Uuid, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                            final long now,
                                            final long deadline) {
        return new Call("deleteTopics", deadline, new ControllerNodeProvider()) {
            @Override
            DeleteTopicsRequest.Builder createRequest(int timeoutMs) {
                return new DeleteTopicsRequest.Builder(
                        new DeleteTopicsRequestData()
                                .setTopics(topicIds.stream().map(
                                    topic -> new DeleteTopicState().setTopicId(topic)).collect(Collectors.toList()))
                                .setTimeoutMs(timeoutMs));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse);
                // Handle server responses for particular topics.
                final DeleteTopicsResponse response = (DeleteTopicsResponse) abstractResponse;
                final List<Uuid> retryTopics = new ArrayList<>();
                final Map<Uuid, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                for (DeletableTopicResult result : response.data().responses()) {
                    KafkaFutureImpl<Void> future = futures.get(result.topicId());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic ID {}", result.topicId());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                        response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    retryTopics.add(result.topicId());
                                    retryTopicQuotaExceededExceptions.put(result.topicId(), quotaExceededException);
                                } else {
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            future.complete(null);
                        }
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    final long now = time.milliseconds();
                    final Call call = getDeleteTopicsWithIdsCall(options, futures, retryTopics,
                            retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                        throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // Fail all the other remaining futures
                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    @Override
    public ListTopicsResult listTopics(final ListTopicsOptions options) {
        final KafkaFutureImpl<Map<String, TopicListing>> topicListingFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("listTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                Map<String, TopicListing> topicListing = new HashMap<>();
                for (MetadataResponse.TopicMetadata topicMetadata : response.topicMetadata()) {
                    String topicName = topicMetadata.topic();
                    boolean isInternal = topicMetadata.isInternal();
                    if (!topicMetadata.isInternal() || options.shouldListInternal())
                        topicListing.put(topicName, new TopicListing(topicName, topicMetadata.topicId(), isInternal));
                }
                topicListingFuture.complete(topicListing);
            }

            @Override
            void handleFailure(Throwable throwable) {
                topicListingFuture.completeExceptionally(throwable);
            }
        }, now);
        return new ListTopicsResult(topicListingFuture);
    }

    @Override
    public DescribeTopicsResult describeTopics(final TopicCollection topics, DescribeTopicsOptions options) {
        if (topics instanceof TopicIdCollection)
            return DescribeTopicsResult.ofTopicIds(handleDescribeTopicsByIds(((TopicIdCollection) topics).topicIds(), options));
        else if (topics instanceof TopicNameCollection)
            return DescribeTopicsResult.ofTopicNames(handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi(((TopicNameCollection) topics).topicNames(), options));
        else
            throw new IllegalArgumentException("The TopicCollection: " + topics + " provided did not match any supported classes for describeTopics.");
    }

    private Call generateDescribeTopicsCallWithMetadataApi(
        List<String> topicNamesList,
        Map<String, KafkaFutureImpl<TopicDescription>> topicFutures,
        DescribeTopicsOptions options,
        long now
    ) {
        return new Call("describeTopics", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            private boolean supportsDisablingTopicCreation = true;

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                if (supportsDisablingTopicCreation)
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(convertToMetadataRequestTopic(topicNamesList))
                        .setAllowAutoTopicCreation(false)
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations()));
                else
                    return MetadataRequest.Builder.allTopics();
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // Handle server responses for particular topics.
                Cluster cluster = response.buildCluster();
                Map<String, Errors> errors = response.errors();
                for (Map.Entry<String, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    String topicName = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();
                    Errors topicError = errors.get(topicName);
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }
                    if (!cluster.topics().contains(topicName)) {
                        future.completeExceptionally(new UnknownTopicOrPartitionException("Topic " + topicName + " not found."));
                        continue;
                    }
                    Uuid topicId = cluster.topicId(topicName);
                    Integer authorizedOperations = response.topicAuthorizedOperations(topicName).get();
                    TopicDescription topicDescription = getTopicDescriptionFromCluster(cluster, topicName, topicId, authorizedOperations);
                    future.complete(topicDescription);
                }
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                if (supportsDisablingTopicCreation) {
                    supportsDisablingTopicCreation = false;
                    return true;
                }
                return false;
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
    }

    private Call generateDescribeTopicsCallWithDescribeTopicPartitionsApi(
        List<String> topicNamesList,
        Map<String, KafkaFutureImpl<TopicDescription>> topicFutures,
        Map<Integer, Node> nodes,
        DescribeTopicsOptions options,
        long now
    ) {
        final Map<String, TopicRequest> topicsRequests = new LinkedHashMap<>();
        topicNamesList.stream().sorted().forEach(topic ->
            topicsRequests.put(topic, new TopicRequest().setName(topic))
        );
        return new Call("describeTopicPartitions", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {
            TopicDescription partiallyFinishedTopicDescription = null;

            @Override
            DescribeTopicPartitionsRequest.Builder createRequest(int timeoutMs) {
                DescribeTopicPartitionsRequestData request = new DescribeTopicPartitionsRequestData()
                    .setTopics(new ArrayList<>(topicsRequests.values()))
                    .setResponsePartitionLimit(options.partitionSizeLimitPerResponse());
                if (partiallyFinishedTopicDescription != null) {
                    // If the previous cursor points to partition 0, it will not be set here. Instead, the previous
                    // cursor topic will be the first topic in the request.
                    request.setCursor(new DescribeTopicPartitionsRequestData.Cursor()
                        .setTopicName(partiallyFinishedTopicDescription.name())
                        .setPartitionIndex(partiallyFinishedTopicDescription.partitions().size())
                    );
                }
                return new DescribeTopicPartitionsRequest.Builder(request);
            }

            @SuppressWarnings("NPathComplexity")
            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeTopicPartitionsResponse response = (DescribeTopicPartitionsResponse) abstractResponse;
                DescribeTopicPartitionsResponseData.Cursor responseCursor = response.data().nextCursor();
                // The topicDescription for the cursor topic of the current batch.
                TopicDescription nextTopicDescription = null;

                for (DescribeTopicPartitionsResponseTopic topic : response.data().topics()) {
                    String topicName = topic.name();
                    Errors error = Errors.forCode(topic.errorCode());

                    KafkaFutureImpl<TopicDescription> future = topicFutures.get(topicName);

                    if (error != Errors.NONE) {
                        future.completeExceptionally(error.exception());
                        topicsRequests.remove(topicName);
                        if (responseCursor != null && responseCursor.topicName().equals(topicName)) {
                            responseCursor = null;
                        }
                        continue;
                    }

                    TopicDescription currentTopicDescription = getTopicDescriptionFromDescribeTopicsResponseTopic(topic, nodes, options.includeAuthorizedOperations());

                    if (partiallyFinishedTopicDescription != null && partiallyFinishedTopicDescription.name().equals(topicName)) {
                        // Add the partitions for the cursor topic of the previous batch.
                        partiallyFinishedTopicDescription.partitions().addAll(currentTopicDescription.partitions());
                        continue;
                    }

                    if (responseCursor != null && responseCursor.topicName().equals(topicName)) {
                        // In the same batch of result, it may need to handle the partitions for the previous cursor
                        // topic and the current cursor topic. Cache the result in the nextTopicDescription.
                        nextTopicDescription = currentTopicDescription;
                        continue;
                    }

                    topicsRequests.remove(topicName);
                    future.complete(currentTopicDescription);
                }

                if (partiallyFinishedTopicDescription != null &&
                        (responseCursor == null || !responseCursor.topicName().equals(partiallyFinishedTopicDescription.name()))) {
                    // We can't simply check nextTopicDescription != null here to close the partiallyFinishedTopicDescription.
                    // Because the responseCursor topic may not show in the response.
                    String topicName = partiallyFinishedTopicDescription.name();
                    topicFutures.get(topicName).complete(partiallyFinishedTopicDescription);
                    topicsRequests.remove(topicName);
                    partiallyFinishedTopicDescription = null;
                }
                if (nextTopicDescription != null) {
                    partiallyFinishedTopicDescription = nextTopicDescription;
                }

                if (!topicsRequests.isEmpty()) {
                    runnable.call(this, time.milliseconds());
                }
            }

            @Override
            boolean handleUnsupportedVersionException(UnsupportedVersionException exception) {
                final long now = time.milliseconds();
                runnable.call(generateDescribeTopicsCallWithMetadataApi(topicNamesList, topicFutures, options, now), now);
                return false;
            }

            @Override
            void handleFailure(Throwable throwable) {
                if (!(throwable instanceof UnsupportedVersionException)) {
                    completeAllExceptionally(topicFutures.values(), throwable);
                }
            }
        };
    }

    private Map<String, KafkaFuture<TopicDescription>> handleDescribeTopicsByNamesWithDescribeTopicPartitionsApi(
        final Collection<String> topicNames,
        DescribeTopicsOptions options
    ) {
        final Map<String, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicNames.size());
        final ArrayList<String> topicNamesList = new ArrayList<>();
        for (String topicName : topicNames) {
            if (topicNameIsUnrepresentable(topicName)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                    topicName + "' cannot be represented in a request."));
                topicFutures.put(topicName, future);
            } else if (!topicFutures.containsKey(topicName)) {
                topicFutures.put(topicName, new KafkaFutureImpl<>());
                topicNamesList.add(topicName);
            }
        }

        if (topicNamesList.isEmpty()) {
            return new HashMap<>(topicFutures);
        }

        // First, we need to retrieve the node info.
        DescribeClusterResult clusterResult = describeCluster();
        clusterResult.nodes().whenComplete(
            (nodes, exception) -> {
                if (exception != null) {
                    completeAllExceptionally(topicFutures.values(), exception);
                    return;
                }

                final long now = time.milliseconds();
                Map<Integer, Node> nodeIdMap = nodes.stream().collect(Collectors.toMap(Node::id, node -> node));
                runnable.call(
                    generateDescribeTopicsCallWithDescribeTopicPartitionsApi(topicNamesList, topicFutures, nodeIdMap, options, now),
                    now
                );
            });

        return new HashMap<>(topicFutures);
    }

    private Map<Uuid, KafkaFuture<TopicDescription>> handleDescribeTopicsByIds(Collection<Uuid> topicIds, DescribeTopicsOptions options) {

        final Map<Uuid, KafkaFutureImpl<TopicDescription>> topicFutures = new HashMap<>(topicIds.size());
        final List<Uuid> topicIdsList = new ArrayList<>();
        for (Uuid topicId : topicIds) {
            if (topicIdIsUnrepresentable(topicId)) {
                KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
                future.completeExceptionally(new InvalidTopicException("The given topic id '" +
                        topicId + "' cannot be represented in a request."));
                topicFutures.put(topicId, future);
            } else if (!topicFutures.containsKey(topicId)) {
                topicFutures.put(topicId, new KafkaFutureImpl<>());
                topicIdsList.add(topicId);
            }
        }
        final long now = time.milliseconds();
        Call call = new Call("describeTopicsWithIds", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(convertTopicIdsToMetadataRequestTopic(topicIdsList))
                        .setAllowAutoTopicCreation(false)
                        .setIncludeTopicAuthorizedOperations(options.includeAuthorizedOperations()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse response = (MetadataResponse) abstractResponse;
                // Handle server responses for particular topics.
                Cluster cluster = response.buildCluster();
                Map<Uuid, Errors> errors = response.errorsByTopicId();
                for (Map.Entry<Uuid, KafkaFutureImpl<TopicDescription>> entry : topicFutures.entrySet()) {
                    Uuid topicId = entry.getKey();
                    KafkaFutureImpl<TopicDescription> future = entry.getValue();

                    String topicName = cluster.topicName(topicId);
                    if (topicName == null) {
                        future.completeExceptionally(new UnknownTopicIdException("TopicId " + topicId + " not found."));
                        continue;
                    }
                    Errors topicError = errors.get(topicId);
                    if (topicError != null) {
                        future.completeExceptionally(topicError.exception());
                        continue;
                    }

                    Integer authorizedOperations = response.topicAuthorizedOperations(topicName).get();
                    TopicDescription topicDescription = getTopicDescriptionFromCluster(cluster, topicName, topicId, authorizedOperations);
                    future.complete(topicDescription);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(topicFutures.values(), throwable);
            }
        };
        if (!topicIdsList.isEmpty()) {
            runnable.call(call, now);
        }
        return new HashMap<>(topicFutures);
    }

    private TopicDescription getTopicDescriptionFromDescribeTopicsResponseTopic(
        DescribeTopicPartitionsResponseTopic topic,
        Map<Integer, Node> nodes,
        boolean includeAuthorizedOperations
    ) {
        List<DescribeTopicPartitionsResponsePartition> partitionInfos = topic.partitions();
        List<TopicPartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
        for (DescribeTopicPartitionsResponsePartition partitionInfo : partitionInfos) {
            partitions.add(DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo(partitionInfo, nodes));
        }
        Set<AclOperation> authorisedOperations = includeAuthorizedOperations ? validAclOperations(topic.topicAuthorizedOperations()) : null;
        return new TopicDescription(topic.name(), topic.isInternal(), partitions, authorisedOperations, topic.topicId());
    }

    private TopicDescription getTopicDescriptionFromCluster(Cluster cluster, String topicName, Uuid topicId,
                                                            Integer authorizedOperations) {
        boolean isInternal = cluster.internalTopics().contains(topicName);
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topicName);
        List<TopicPartitionInfo> partitions = new ArrayList<>(partitionInfos.size());
        for (PartitionInfo partitionInfo : partitionInfos) {
            TopicPartitionInfo topicPartitionInfo = new TopicPartitionInfo(
                    partitionInfo.partition(), leader(partitionInfo), Arrays.asList(partitionInfo.replicas()),
                    Arrays.asList(partitionInfo.inSyncReplicas()));
            partitions.add(topicPartitionInfo);
        }
        partitions.sort(Comparator.comparingInt(TopicPartitionInfo::partition));
        return new TopicDescription(topicName, isInternal, partitions, validAclOperations(authorizedOperations), topicId);
    }

    private Node leader(PartitionInfo partitionInfo) {
        if (partitionInfo.leader() == null || partitionInfo.leader().id() == Node.noNode().id())
            return null;
        return partitionInfo.leader();
    }

    @Override
    public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
        final KafkaFutureImpl<Collection<Node>> describeClusterFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Node> controllerFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<String> clusterIdFuture = new KafkaFutureImpl<>();
        final KafkaFutureImpl<Set<AclOperation>> authorizedOperationsFuture = new KafkaFutureImpl<>();

        final long now = time.milliseconds();
        runnable.call(new Call("listNodes", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            private boolean useMetadataRequest = false;

            @Override
            AbstractRequest.Builder createRequest(int timeoutMs) {
                if (!useMetadataRequest) {
                    return new DescribeClusterRequest.Builder(new DescribeClusterRequestData()
                        .setIncludeClusterAuthorizedOperations(options.includeAuthorizedOperations())
                        .setEndpointType(metadataManager.usingBootstrapControllers() ?
                                EndpointType.CONTROLLER.id() : EndpointType.BROKER.id()));
                } else {
                    // Since this only requests node information, it's safe to pass true for allowAutoTopicCreation (and it
                    // simplifies communication with older brokers)
                    return new MetadataRequest.Builder(new MetadataRequestData()
                        .setTopics(Collections.emptyList())
                        .setAllowAutoTopicCreation(true)
                        .setIncludeClusterAuthorizedOperations(
                            options.includeAuthorizedOperations()));
                }
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                if (!useMetadataRequest) {
                    DescribeClusterResponse response = (DescribeClusterResponse) abstractResponse;

                    Errors error = Errors.forCode(response.data().errorCode());
                    if (error != Errors.NONE) {
                        ApiError apiError = new ApiError(error, response.data().errorMessage());
                        handleFailure(apiError.exception());
                        return;
                    }

                    Map<Integer, Node> nodes = response.nodes();
                    describeClusterFuture.complete(nodes.values());
                    // Controller is null if controller id is equal to NO_CONTROLLER_ID
                    controllerFuture.complete(nodes.get(response.data().controllerId()));
                    clusterIdFuture.complete(response.data().clusterId());
                    authorizedOperationsFuture.complete(
                        validAclOperations(response.data().clusterAuthorizedOperations()));
                } else {
                    MetadataResponse response = (MetadataResponse) abstractResponse;
                    describeClusterFuture.complete(response.brokers());
                    controllerFuture.complete(controller(response));
                    clusterIdFuture.complete(response.clusterId());
                    authorizedOperationsFuture.complete(
                        validAclOperations(response.clusterAuthorizedOperations()));
                }
            }

            private Node controller(MetadataResponse response) {
                if (response.controller() == null || response.controller().id() == MetadataResponse.NO_CONTROLLER_ID)
                    return null;
                return response.controller();
            }

            @Override
            void handleFailure(Throwable throwable) {
                describeClusterFuture.completeExceptionally(throwable);
                controllerFuture.completeExceptionally(throwable);
                clusterIdFuture.completeExceptionally(throwable);
                authorizedOperationsFuture.completeExceptionally(throwable);
            }

            @Override
            boolean handleUnsupportedVersionException(final UnsupportedVersionException exception) {
                if (metadataManager.usingBootstrapControllers()) {
                    return false;
                }
                if (useMetadataRequest) {
                    return false;
                }

                useMetadataRequest = true;
                return true;
            }
        }, now);

        return new DescribeClusterResult(describeClusterFuture, controllerFuture, clusterIdFuture,
            authorizedOperationsFuture);
    }

    @Override
    public DescribeAclsResult describeAcls(final AclBindingFilter filter, DescribeAclsOptions options) {
        if (filter.isUnknown()) {
            KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
            future.completeExceptionally(new InvalidRequestException("The AclBindingFilter " +
                    "must not contain UNKNOWN elements."));
            return new DescribeAclsResult(future);
        }
        final long now = time.milliseconds();
        final KafkaFutureImpl<Collection<AclBinding>> future = new KafkaFutureImpl<>();
        runnable.call(new Call("describeAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            @Override
            DescribeAclsRequest.Builder createRequest(int timeoutMs) {
                return new DescribeAclsRequest.Builder(filter);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeAclsResponse response = (DescribeAclsResponse) abstractResponse;
                if (response.error().isFailure()) {
                    future.completeExceptionally(response.error().exception());
                } else {
                    future.complete(DescribeAclsResponse.aclBindings(response.acls()));
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        }, now);
        return new DescribeAclsResult(future);
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        final long now = time.milliseconds();
        final Map<AclBinding, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final List<AclCreation> aclCreations = new ArrayList<>();
        final List<AclBinding> aclBindingsSent = new ArrayList<>();
        for (AclBinding acl : acls) {
            if (futures.get(acl) == null) {
                KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
                futures.put(acl, future);
                String indefinite = acl.toFilter().findIndefiniteField();
                if (indefinite == null) {
                    aclCreations.add(CreateAclsRequest.aclCreation(acl));
                    aclBindingsSent.add(acl);
                } else {
                    future.completeExceptionally(new InvalidRequestException("Invalid ACL creation: " +
                        indefinite));
                }
            }
        }
        final CreateAclsRequestData data = new CreateAclsRequestData().setCreations(aclCreations);
        runnable.call(new Call("createAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            @Override
            CreateAclsRequest.Builder createRequest(int timeoutMs) {
                return new CreateAclsRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateAclsResponse response = (CreateAclsResponse) abstractResponse;
                List<AclCreationResult> responses = response.results();
                Iterator<AclCreationResult> iter = responses.iterator();
                for (AclBinding aclBinding : aclBindingsSent) {
                    KafkaFutureImpl<Void> future = futures.get(aclBinding);
                    if (!iter.hasNext()) {
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no creation result for the given ACL: " + aclBinding));
                    } else {
                        AclCreationResult creation = iter.next();
                        Errors error = Errors.forCode(creation.errorCode());
                        ApiError apiError = new ApiError(error, creation.errorMessage());
                        if (apiError.isFailure())
                            future.completeExceptionally(apiError.exception());
                        else
                            future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new CreateAclsResult(new HashMap<>(futures));
    }

    @Override
    public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
        final long now = time.milliseconds();
        final Map<AclBindingFilter, KafkaFutureImpl<FilterResults>> futures = new HashMap<>();
        final List<AclBindingFilter> aclBindingFiltersSent = new ArrayList<>();
        final List<DeleteAclsFilter> deleteAclsFilters = new ArrayList<>();
        for (AclBindingFilter filter : filters) {
            if (futures.get(filter) == null) {
                aclBindingFiltersSent.add(filter);
                deleteAclsFilters.add(DeleteAclsRequest.deleteAclsFilter(filter));
                futures.put(filter, new KafkaFutureImpl<>());
            }
        }
        final DeleteAclsRequestData data = new DeleteAclsRequestData().setFilters(deleteAclsFilters);
        runnable.call(new Call("deleteAcls", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedBrokerOrActiveKController()) {

            @Override
            DeleteAclsRequest.Builder createRequest(int timeoutMs) {
                return new DeleteAclsRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DeleteAclsResponse response = (DeleteAclsResponse) abstractResponse;
                List<DeleteAclsResponseData.DeleteAclsFilterResult> results = response.filterResults();
                Iterator<DeleteAclsResponseData.DeleteAclsFilterResult> iter = results.iterator();
                for (AclBindingFilter bindingFilter : aclBindingFiltersSent) {
                    KafkaFutureImpl<FilterResults> future = futures.get(bindingFilter);
                    if (!iter.hasNext()) {
                        future.completeExceptionally(new UnknownServerException(
                            "The broker reported no deletion result for the given filter."));
                    } else {
                        DeleteAclsFilterResult filterResult = iter.next();
                        ApiError error = new ApiError(Errors.forCode(filterResult.errorCode()), filterResult.errorMessage());
                        if (error.isFailure()) {
                            future.completeExceptionally(error.exception());
                        } else {
                            List<FilterResult> filterResults = new ArrayList<>();
                            for (DeleteAclsMatchingAcl matchingAcl : filterResult.matchingAcls()) {
                                ApiError aclError = new ApiError(Errors.forCode(matchingAcl.errorCode()),
                                    matchingAcl.errorMessage());
                                AclBinding aclBinding = DeleteAclsResponse.aclBinding(matchingAcl);
                                filterResults.add(new FilterResult(aclBinding, aclError.exception()));
                            }
                            future.complete(new FilterResults(filterResults));
                        }
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return new DeleteAclsResult(new HashMap<>(futures));
    }

    @Override
    public DescribeConfigsResult describeConfigs(Collection<ConfigResource> configResources, final DescribeConfigsOptions options) {
        // Partition the requested config resources based on which broker they must be sent to with the
        // null broker being used for config resources which can be obtained from any broker
        final Map<Integer, Map<ConfigResource, KafkaFutureImpl<Config>>> nodeFutures = new HashMap<>(configResources.size());

        for (ConfigResource resource : configResources) {
            Integer broker = nodeFor(resource);
            nodeFutures.compute(broker, (key, value) -> {
                if (value == null) {
                    value = new HashMap<>();
                }
                value.put(resource, new KafkaFutureImpl<>());
                return value;
            });
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, Map<ConfigResource, KafkaFutureImpl<Config>>> entry : nodeFutures.entrySet()) {
            final Integer node = entry.getKey();
            Map<ConfigResource, KafkaFutureImpl<Config>> unified = entry.getValue();

            runnable.call(new Call("describeConfigs", calcDeadlineMs(now, options.timeoutMs()),
                node != null ? new ConstantNodeIdProvider(node, true) : new LeastLoadedBrokerOrActiveKController()) {

                @Override
                DescribeConfigsRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeConfigsRequest.Builder(new DescribeConfigsRequestData()
                        .setResources(unified.keySet().stream()
                            .map(config ->
                                new DescribeConfigsRequestData.DescribeConfigsResource()
                                    .setResourceName(config.name())
                                    .setResourceType(config.type().id())
                                    .setConfigurationKeys(null))
                            .collect(Collectors.toList()))
                        .setIncludeSynonyms(options.includeSynonyms())
                        .setIncludeDocumentation(options.includeDocumentation()));
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeConfigsResponse response = (DescribeConfigsResponse) abstractResponse;
                    for (Map.Entry<ConfigResource, DescribeConfigsResponseData.DescribeConfigsResult> entry : response.resultMap().entrySet()) {
                        ConfigResource configResource = entry.getKey();
                        DescribeConfigsResponseData.DescribeConfigsResult describeConfigsResult = entry.getValue();
                        KafkaFutureImpl<Config> future = unified.get(configResource);
                        if (future == null) {
                            if (node != null) {
                                log.warn("The config {} in the response from node {} is not in the request",
                                        configResource, node);
                            } else {
                                log.warn("The config {} in the response from the least loaded broker is not in the request",
                                        configResource);
                            }
                        } else {
                            if (describeConfigsResult.errorCode() != Errors.NONE.code()) {
                                future.completeExceptionally(Errors.forCode(describeConfigsResult.errorCode())
                                        .exception(describeConfigsResult.errorMessage()));
                            } else {
                                future.complete(describeConfigResult(describeConfigsResult));
                            }
                        }
                    }
                    completeUnrealizedFutures(
                        unified.entrySet().stream(),
                        configResource -> "The node response did not contain a result for config resource " + configResource);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(unified.values(), throwable);
                }
            }, now);
        }

        return new DescribeConfigsResult(
            nodeFutures.entrySet()
                .stream()
                .flatMap(x -> x.getValue().entrySet().stream())
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (oldValue, newValue) -> {
                        // Duplicate keys should not occur, throw an exception to signal this issue
                        throw new IllegalStateException(String.format("Duplicate key for values: %s and %s", oldValue, newValue));
                    },
                    HashMap::new
                ))
        );
    }

    private Config describeConfigResult(DescribeConfigsResponseData.DescribeConfigsResult describeConfigsResult) {
        return new Config(describeConfigsResult.configs().stream().map(config -> new ConfigEntry(
                config.name(),
                config.value(),
                DescribeConfigsResponse.ConfigSource.forId(config.configSource()).source(),
                config.isSensitive(),
                config.readOnly(),
                (config.synonyms().stream().map(synonym -> new ConfigEntry.ConfigSynonym(synonym.name(), synonym.value(),
                        DescribeConfigsResponse.ConfigSource.forId(synonym.source()).source()))).collect(Collectors.toList()),
                DescribeConfigsResponse.ConfigType.forId(config.configType()).type(),
                config.documentation()
        )).collect(Collectors.toList()));
    }

    private ConfigEntry.ConfigSource configSource(DescribeConfigsResponse.ConfigSource source) {
        ConfigEntry.ConfigSource configSource;
        switch (source) {
            case TOPIC_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
                break;
            case DYNAMIC_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG;
                break;
            case DYNAMIC_DEFAULT_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG;
                break;
            case STATIC_BROKER_CONFIG:
                configSource = ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG;
                break;
            case DYNAMIC_BROKER_LOGGER_CONFIG:
                configSource = ConfigEntry.ConfigSource.DYNAMIC_BROKER_LOGGER_CONFIG;
                break;
            case DEFAULT_CONFIG:
                configSource = ConfigEntry.ConfigSource.DEFAULT_CONFIG;
                break;
            default:
                throw new IllegalArgumentException("Unexpected config source " + source);
        }
        return configSource;
    }

    @Override
    public AlterConfigsResult incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                      final AlterConfigsOptions options) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> allFutures = new HashMap<>();
        // BROKER_LOGGER requests always go to a specific, constant broker or controller node.
        //
        // BROKER resource changes for a specific (non-default) resource go to either that specific
        // node (if using bootstrap.servers), or directly to the active controller (if using
        // bootstrap.controllers)
        //
        // All other requests go to the least loaded broker (if using bootstrap.servers) or the
        // active controller (if using bootstrap.controllers)
        final Collection<ConfigResource> unifiedRequestResources = new ArrayList<>();

        for (ConfigResource resource : configs.keySet()) {
            Integer node = nodeFor(resource);
            if (metadataManager.usingBootstrapControllers()) {
                if (!resource.type().equals(ConfigResource.Type.BROKER_LOGGER)) {
                    node = null;
                }
            }
            if (node != null) {
                NodeProvider nodeProvider = new ConstantNodeIdProvider(node, true);
                allFutures.putAll(incrementalAlterConfigs(configs, options, Collections.singleton(resource), nodeProvider));
            } else
                unifiedRequestResources.add(resource);
        }
        if (!unifiedRequestResources.isEmpty())
            allFutures.putAll(incrementalAlterConfigs(configs, options, unifiedRequestResources, new LeastLoadedBrokerOrActiveKController()));

        return new AlterConfigsResult(new HashMap<>(allFutures));
    }

    private Map<ConfigResource, KafkaFutureImpl<Void>> incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>> configs,
                                                                               final AlterConfigsOptions options,
                                                                               Collection<ConfigResource> resources,
                                                                               NodeProvider nodeProvider) {
        final Map<ConfigResource, KafkaFutureImpl<Void>> futures = new HashMap<>();
        for (ConfigResource resource : resources)
            futures.put(resource, new KafkaFutureImpl<>());

        final long now = time.milliseconds();
        runnable.call(new Call("incrementalAlterConfigs", calcDeadlineMs(now, options.timeoutMs()), nodeProvider) {

            @Override
            public IncrementalAlterConfigsRequest.Builder createRequest(int timeoutMs) {
                return new IncrementalAlterConfigsRequest.Builder(resources, configs, options.shouldValidateOnly());
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                IncrementalAlterConfigsResponse response = (IncrementalAlterConfigsResponse) abstractResponse;
                Map<ConfigResource, ApiError> errors = IncrementalAlterConfigsResponse.fromResponseData(response.data());
                for (Map.Entry<ConfigResource, KafkaFutureImpl<Void>> entry : futures.entrySet()) {
                    KafkaFutureImpl<Void> future = entry.getValue();
                    ApiException exception = errors.get(entry.getKey()).exception();
                    if (exception != null) {
                        future.completeExceptionally(exception);
                    } else {
                        future.complete(null);
                    }
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        }, now);
        return futures;
    }

    @Override
    public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment, final AlterReplicaLogDirsOptions options) {
        final Map<TopicPartitionReplica, KafkaFutureImpl<Void>> futures = new HashMap<>(replicaAssignment.size());

        for (TopicPartitionReplica replica : replicaAssignment.keySet())
            futures.put(replica, new KafkaFutureImpl<>());

        Map<Integer, AlterReplicaLogDirsRequestData> replicaAssignmentByBroker = new HashMap<>();
        for (Map.Entry<TopicPartitionReplica, String> entry: replicaAssignment.entrySet()) {
            TopicPartitionReplica replica = entry.getKey();
            String logDir = entry.getValue();
            int brokerId = replica.brokerId();
            AlterReplicaLogDirsRequestData value = replicaAssignmentByBroker.computeIfAbsent(brokerId,
                key -> new AlterReplicaLogDirsRequestData());
            AlterReplicaLogDir alterReplicaLogDir = value.dirs().find(logDir);
            if (alterReplicaLogDir == null) {
                alterReplicaLogDir = new AlterReplicaLogDir();
                alterReplicaLogDir.setPath(logDir);
                value.dirs().add(alterReplicaLogDir);
            }
            AlterReplicaLogDirTopic alterReplicaLogDirTopic = alterReplicaLogDir.topics().find(replica.topic());
            if (alterReplicaLogDirTopic == null) {
                alterReplicaLogDirTopic = new AlterReplicaLogDirTopic().setName(replica.topic());
                alterReplicaLogDir.topics().add(alterReplicaLogDirTopic);
            }
            alterReplicaLogDirTopic.partitions().add(replica.partition());
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, AlterReplicaLogDirsRequestData> entry: replicaAssignmentByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final AlterReplicaLogDirsRequestData assignment = entry.getValue();

            runnable.call(new Call("alterReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public AlterReplicaLogDirsRequest.Builder createRequest(int timeoutMs) {
                    return new AlterReplicaLogDirsRequest.Builder(assignment);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    AlterReplicaLogDirsResponse response = (AlterReplicaLogDirsResponse) abstractResponse;
                    for (AlterReplicaLogDirTopicResult topicResult: response.data().results()) {
                        for (AlterReplicaLogDirPartitionResult partitionResult: topicResult.partitions()) {
                            TopicPartitionReplica replica = new TopicPartitionReplica(
                                    topicResult.topicName(), partitionResult.partitionIndex(), brokerId);
                            KafkaFutureImpl<Void> future = futures.get(replica);
                            if (future == null) {
                                log.warn("The partition {} in the response from broker {} is not in the request",
                                        new TopicPartition(topicResult.topicName(), partitionResult.partitionIndex()),
                                        brokerId);
                            } else if (partitionResult.errorCode() == Errors.NONE.code()) {
                                future.complete(null);
                            } else {
                                future.completeExceptionally(Errors.forCode(partitionResult.errorCode()).exception());
                            }
                        }
                    }
                    // The server should send back a response for every replica. But do a sanity check anyway.
                    completeUnrealizedFutures(
                        futures.entrySet().stream().filter(entry -> entry.getKey().brokerId() == brokerId),
                        replica -> "The response from broker " + brokerId +
                                " did not contain a result for replica " + replica);
                }
                @Override
                void handleFailure(Throwable throwable) {
                    // Only completes the futures of brokerId
                    completeAllExceptionally(
                        futures.entrySet().stream()
                            .filter(entry -> entry.getKey().brokerId() == brokerId)
                            .map(Map.Entry::getValue),
                        throwable);
                }
            }, now);
        }

        return new AlterReplicaLogDirsResult(new HashMap<>(futures));
    }

    @Override
    public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
        final Map<Integer, KafkaFutureImpl<Map<String, LogDirDescription>>> futures = new HashMap<>(brokers.size());

        final long now = time.milliseconds();
        for (final Integer brokerId : brokers) {
            KafkaFutureImpl<Map<String, LogDirDescription>> future = new KafkaFutureImpl<>();
            futures.put(brokerId, future);

            runnable.call(new Call("describeLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public DescribeLogDirsRequest.Builder createRequest(int timeoutMs) {
                    // Query selected partitions in all log directories
                    return new DescribeLogDirsRequest.Builder(new DescribeLogDirsRequestData().setTopics(null));
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    Map<String, LogDirDescription> descriptions = logDirDescriptions(response);
                    if (!descriptions.isEmpty()) {
                        future.complete(descriptions);
                    } else {
                        // Up to v3 DescribeLogDirsResponse did not have an error code field, hence it defaults to None
                        Errors error = response.data().errorCode() == Errors.NONE.code()
                                ? Errors.CLUSTER_AUTHORIZATION_FAILED
                                : Errors.forCode(response.data().errorCode());
                        future.completeExceptionally(error.exception());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            }, now);
        }

        return new DescribeLogDirsResult(new HashMap<>(futures));
    }

    private static Map<String, LogDirDescription> logDirDescriptions(DescribeLogDirsResponse response) {
        Map<String, LogDirDescription> result = new HashMap<>(response.data().results().size());
        for (DescribeLogDirsResponseData.DescribeLogDirsResult logDirResult : response.data().results()) {
            Map<TopicPartition, ReplicaInfo> replicaInfoMap = new HashMap<>();
            for (DescribeLogDirsResponseData.DescribeLogDirsTopic t : logDirResult.topics()) {
                for (DescribeLogDirsResponseData.DescribeLogDirsPartition p : t.partitions()) {
                    replicaInfoMap.put(
                            new TopicPartition(t.name(), p.partitionIndex()),
                            new ReplicaInfo(p.partitionSize(), p.offsetLag(), p.isFutureKey()));
                }
            }
            result.put(logDirResult.logDir(), new LogDirDescription(
                    Errors.forCode(logDirResult.errorCode()).exception(),
                    replicaInfoMap,
                    logDirResult.totalBytes(),
                    logDirResult.usableBytes()));
        }
        return result;
    }

    @Override
    public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas, DescribeReplicaLogDirsOptions options) {
        final Map<TopicPartitionReplica, KafkaFutureImpl<DescribeReplicaLogDirsResult.ReplicaLogDirInfo>> futures = new HashMap<>(replicas.size());

        for (TopicPartitionReplica replica : replicas) {
            futures.put(replica, new KafkaFutureImpl<>());
        }

        Map<Integer, DescribeLogDirsRequestData> partitionsByBroker = new HashMap<>();

        for (TopicPartitionReplica replica: replicas) {
            DescribeLogDirsRequestData requestData = partitionsByBroker.computeIfAbsent(replica.brokerId(),
                brokerId -> new DescribeLogDirsRequestData());
            DescribableLogDirTopic describableLogDirTopic = requestData.topics().find(replica.topic());
            if (describableLogDirTopic == null) {
                List<Integer> partitions = new ArrayList<>();
                partitions.add(replica.partition());
                describableLogDirTopic = new DescribableLogDirTopic().setTopic(replica.topic())
                        .setPartitions(partitions);
                requestData.topics().add(describableLogDirTopic);
            } else {
                describableLogDirTopic.partitions().add(replica.partition());
            }
        }

        final long now = time.milliseconds();
        for (Map.Entry<Integer, DescribeLogDirsRequestData> entry: partitionsByBroker.entrySet()) {
            final int brokerId = entry.getKey();
            final DescribeLogDirsRequestData topicPartitions = entry.getValue();
            final Map<TopicPartition, ReplicaLogDirInfo> replicaDirInfoByPartition = new HashMap<>();
            for (DescribableLogDirTopic topicPartition: topicPartitions.topics()) {
                for (Integer partitionId : topicPartition.partitions()) {
                    replicaDirInfoByPartition.put(new TopicPartition(topicPartition.topic(), partitionId), new ReplicaLogDirInfo());
                }
            }

            runnable.call(new Call("describeReplicaLogDirs", calcDeadlineMs(now, options.timeoutMs()),
                new ConstantNodeIdProvider(brokerId)) {

                @Override
                public DescribeLogDirsRequest.Builder createRequest(int timeoutMs) {
                    // Query selected partitions in all log directories
                    return new DescribeLogDirsRequest.Builder(topicPartitions);
                }

                @Override
                public void handleResponse(AbstractResponse abstractResponse) {
                    DescribeLogDirsResponse response = (DescribeLogDirsResponse) abstractResponse;
                    for (Map.Entry<String, LogDirDescription> responseEntry: logDirDescriptions(response).entrySet()) {
                        String logDir = responseEntry.getKey();
                        LogDirDescription logDirInfo = responseEntry.getValue();

                        // No replica info will be provided if the log directory is offline
                        if (logDirInfo.error() instanceof KafkaStorageException)
                            continue;
                        if (logDirInfo.error() != null)
                            handleFailure(new IllegalStateException(
                                "The error " + logDirInfo.error().getClass().getName() + " for log directory " + logDir + " in the response from broker " + brokerId + " is illegal"));

                        for (Map.Entry<TopicPartition, ReplicaInfo> replicaInfoEntry: logDirInfo.replicaInfos().entrySet()) {
                            TopicPartition tp = replicaInfoEntry.getKey();
                            ReplicaInfo replicaInfo = replicaInfoEntry.getValue();
                            ReplicaLogDirInfo replicaLogDirInfo = replicaDirInfoByPartition.get(tp);
                            if (replicaLogDirInfo == null) {
                                log.warn("Server response from broker {} mentioned unknown partition {}", brokerId, tp);
                            } else if (replicaInfo.isFuture()) {
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(replicaLogDirInfo.getCurrentReplicaLogDir(),
                                                                                        replicaLogDirInfo.getCurrentReplicaOffsetLag(),
                                                                                        logDir,
                                                                                        replicaInfo.offsetLag()));
                            } else {
                                replicaDirInfoByPartition.put(tp, new ReplicaLogDirInfo(logDir,
                                                                                        replicaInfo.offsetLag(),
                                                                                        replicaLogDirInfo.getFutureReplicaLogDir(),
                                                                                        replicaLogDirInfo.getFutureReplicaOffsetLag()));
                            }
                        }
                    }

                    for (Map.Entry<TopicPartition, ReplicaLogDirInfo> entry: replicaDirInfoByPartition.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        KafkaFutureImpl<ReplicaLogDirInfo> future = futures.get(new TopicPartitionReplica(tp.topic(), tp.partition(), brokerId));
                        future.complete(entry.getValue());
                    }
                }
                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);
        }

        return new DescribeReplicaLogDirsResult(new HashMap<>(futures));
    }

    @Override
    public CreatePartitionsResult createPartitions(final Map<String, NewPartitions> newPartitions,
                                                   final CreatePartitionsOptions options) {
        final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>(newPartitions.size());
        final CreatePartitionsTopicCollection topics = new CreatePartitionsTopicCollection(newPartitions.size());
        for (Map.Entry<String, NewPartitions> entry : newPartitions.entrySet()) {
            final String topic = entry.getKey();
            final NewPartitions newPartition = entry.getValue();
            List<List<Integer>> newAssignments = newPartition.assignments();
            List<CreatePartitionsAssignment> assignments = newAssignments == null ? null :
                newAssignments.stream()
                    .map(brokerIds -> new CreatePartitionsAssignment().setBrokerIds(brokerIds))
                    .collect(Collectors.toList());
            topics.add(new CreatePartitionsTopic()
                .setName(topic)
                .setCount(newPartition.totalCount())
                .setAssignments(assignments));
            futures.put(topic, new KafkaFutureImpl<>());
        }
        if (!topics.isEmpty()) {
            final long now = time.milliseconds();
            final long deadline = calcDeadlineMs(now, options.timeoutMs());
            final Call call = getCreatePartitionsCall(options, futures, topics,
                Collections.emptyMap(), now, deadline);
            runnable.call(call, now);
        }
        return new CreatePartitionsResult(new HashMap<>(futures));
    }

    private Call getCreatePartitionsCall(final CreatePartitionsOptions options,
                                         final Map<String, KafkaFutureImpl<Void>> futures,
                                         final CreatePartitionsTopicCollection topics,
                                         final Map<String, ThrottlingQuotaExceededException> quotaExceededExceptions,
                                         final long now,
                                         final long deadline) {
        return new Call("createPartitions", deadline, new ControllerNodeProvider()) {
            @Override
            public CreatePartitionsRequest.Builder createRequest(int timeoutMs) {
                return new CreatePartitionsRequest.Builder(
                    new CreatePartitionsRequestData()
                        .setTopics(topics)
                        .setValidateOnly(options.validateOnly())
                        .setTimeoutMs(timeoutMs));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                // Check for controller change
                handleNotControllerError(abstractResponse);
                // Handle server responses for particular topics.
                final CreatePartitionsResponse response = (CreatePartitionsResponse) abstractResponse;
                final CreatePartitionsTopicCollection retryTopics = new CreatePartitionsTopicCollection();
                final Map<String, ThrottlingQuotaExceededException> retryTopicQuotaExceededExceptions = new HashMap<>();
                for (CreatePartitionsTopicResult result : response.data().results()) {
                    KafkaFutureImpl<Void> future = futures.get(result.name());
                    if (future == null) {
                        log.warn("Server response mentioned unknown topic {}", result.name());
                    } else {
                        ApiError error = new ApiError(result.errorCode(), result.errorMessage());
                        if (error.isFailure()) {
                            if (error.is(Errors.THROTTLING_QUOTA_EXCEEDED)) {
                                ThrottlingQuotaExceededException quotaExceededException = new ThrottlingQuotaExceededException(
                                    response.throttleTimeMs(), error.messageWithFallback());
                                if (options.shouldRetryOnQuotaViolation()) {
                                    retryTopics.add(topics.find(result.name()).duplicate());
                                    retryTopicQuotaExceededExceptions.put(result.name(), quotaExceededException);
                                } else {
                                    future.completeExceptionally(quotaExceededException);
                                }
                            } else {
                                future.completeExceptionally(error.exception());
                            }
                        } else {
                            future.complete(null);
                        }
                    }
                }
                // If there are topics to retry, retry them; complete unrealized futures otherwise.
                if (retryTopics.isEmpty()) {
                    // The server should send back a response for every topic. But do a sanity check anyway.
                    completeUnrealizedFutures(futures.entrySet().stream(),
                        topic -> "The controller response did not contain a result for topic " + topic);
                } else {
                    final long now = time.milliseconds();
                    final Call call = getCreatePartitionsCall(options, futures, retryTopics,
                        retryTopicQuotaExceededExceptions, now, deadline);
                    runnable.call(call, now);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                // If there were any topics retries due to a quota exceeded exception, we propagate
                // the initial error back to the caller if the request timed out.
                maybeCompleteQuotaExceededException(options.shouldRetryOnQuotaViolation(),
                    throwable, futures, quotaExceededExceptions, (int) (time.milliseconds() - now));
                // Fail all the other remaining futures

                completeAllExceptionally(futures.values(), throwable);
            }
        };
    }

    @Override
    public DeleteRecordsResult deleteRecords(final Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                             final DeleteRecordsOptions options) {
        PartitionLeaderStrategy.PartitionLeaderFuture<DeletedRecords> future =
            DeleteRecordsHandler.newFuture(recordsToDelete.keySet(), partitionLeaderCache);
        int timeoutMs = defaultApiTimeoutMs;
        if (options.timeoutMs() != null) {
            timeoutMs = options.timeoutMs();
        }
        DeleteRecordsHandler handler = new DeleteRecordsHandler(recordsToDelete, logContext, timeoutMs);
        invokeDriver(handler, future, options.timeoutMs);

        return new DeleteRecordsResult(future.all());
    }

    @Override
    public CreateDelegationTokenResult createDelegationToken(final CreateDelegationTokenOptions options) {
        final KafkaFutureImpl<DelegationToken> delegationTokenFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        List<CreatableRenewers> renewers = new ArrayList<>();
        for (KafkaPrincipal principal : options.renewers()) {
            renewers.add(new CreatableRenewers()
                    .setPrincipalName(principal.getName())
                    .setPrincipalType(principal.getPrincipalType()));
        }
        runnable.call(new Call("createDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            CreateDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                CreateDelegationTokenRequestData data = new CreateDelegationTokenRequestData()
                    .setRenewers(renewers)
                    .setMaxLifetimeMs(options.maxLifetimeMs());
                if (options.owner().isPresent()) {
                    data.setOwnerPrincipalName(options.owner().get().getName());
                    data.setOwnerPrincipalType(options.owner().get().getPrincipalType());
                }
                return new CreateDelegationTokenRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                CreateDelegationTokenResponse response = (CreateDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    delegationTokenFuture.completeExceptionally(response.error().exception());
                } else {
                    CreateDelegationTokenResponseData data = response.data();
                    TokenInformation tokenInfo =  new TokenInformation(data.tokenId(), new KafkaPrincipal(data.principalType(), data.principalName()),
                        new KafkaPrincipal(data.tokenRequesterPrincipalType(), data.tokenRequesterPrincipalName()),
                        options.renewers(), data.issueTimestampMs(), data.maxTimestampMs(), data.expiryTimestampMs());
                    DelegationToken token = new DelegationToken(tokenInfo, data.hmac());
                    delegationTokenFuture.complete(token);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                delegationTokenFuture.completeExceptionally(throwable);
            }
        }, now);

        return new CreateDelegationTokenResult(delegationTokenFuture);
    }

    @Override
    public RenewDelegationTokenResult renewDelegationToken(final byte[] hmac, final RenewDelegationTokenOptions options) {
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("renewDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            RenewDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new RenewDelegationTokenRequest.Builder(
                        new RenewDelegationTokenRequestData()
                        .setHmac(hmac)
                        .setRenewPeriodMs(options.renewTimePeriodMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                RenewDelegationTokenResponse response = (RenewDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new RenewDelegationTokenResult(expiryTimeFuture);
    }

    @Override
    public ExpireDelegationTokenResult expireDelegationToken(final byte[] hmac, final ExpireDelegationTokenOptions options) {
        final KafkaFutureImpl<Long>  expiryTimeFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("expireDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            ExpireDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new ExpireDelegationTokenRequest.Builder(
                        new ExpireDelegationTokenRequestData()
                            .setHmac(hmac)
                            .setExpiryTimePeriodMs(options.expiryTimePeriodMs()));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ExpireDelegationTokenResponse response = (ExpireDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    expiryTimeFuture.completeExceptionally(response.error().exception());
                } else {
                    expiryTimeFuture.complete(response.expiryTimestamp());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                expiryTimeFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ExpireDelegationTokenResult(expiryTimeFuture);
    }

    @Override
    public DescribeDelegationTokenResult describeDelegationToken(final DescribeDelegationTokenOptions options) {
        final KafkaFutureImpl<List<DelegationToken>>  tokensFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("describeDelegationToken", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            DescribeDelegationTokenRequest.Builder createRequest(int timeoutMs) {
                return new DescribeDelegationTokenRequest.Builder(options.owners());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                DescribeDelegationTokenResponse response = (DescribeDelegationTokenResponse) abstractResponse;
                if (response.hasError()) {
                    tokensFuture.completeExceptionally(response.error().exception());
                } else {
                    tokensFuture.complete(response.tokens());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                tokensFuture.completeExceptionally(throwable);
            }
        }, now);

        return new DescribeDelegationTokenResult(tokensFuture);
    }

    private static final class ListGroupsResults {
        private final List<Throwable> errors;
        private final HashMap<String, GroupListing> listings;
        private final HashSet<Node> remaining;
        private final KafkaFutureImpl<Collection<Object>> future;

        ListGroupsResults(Collection<Node> leaders,
                          KafkaFutureImpl<Collection<Object>> future) {
            this.errors = new ArrayList<>();
            this.listings = new HashMap<>();
            this.remaining = new HashSet<>(leaders);
            this.future = future;
            tryComplete();
        }

        synchronized void addError(Throwable throwable, Node node) {
            ApiError error = ApiError.fromThrowable(throwable);
            if (error.message() == null || error.message().isEmpty()) {
                errors.add(error.error().exception("Error listing groups on " + node));
            } else {
                errors.add(error.error().exception("Error listing groups on " + node + ": " + error.message()));
            }
        }

        synchronized void addListing(GroupListing listing) {
            listings.put(listing.groupId(), listing);
        }

        synchronized void tryComplete(Node leader) {
            remaining.remove(leader);
            tryComplete();
        }

        private synchronized void tryComplete() {
            if (remaining.isEmpty()) {
                ArrayList<Object> results = new ArrayList<>(listings.values());
                results.addAll(errors);
                future.complete(results);
            }
        }
    }

    @Override
    public ListGroupsResult listGroups(ListGroupsOptions options) {
        final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl<>();
        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
        runnable.call(new Call("findAllBrokers", deadline, new LeastLoadedNodeProvider()) {
            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(Collections.emptyList())
                    .setAllowAutoTopicCreation(true));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse metadataResponse = (MetadataResponse) abstractResponse;
                Collection<Node> nodes = metadataResponse.brokers();
                if (nodes.isEmpty())
                    throw new StaleMetadataException("Metadata fetch failed due to missing broker list");

                HashSet<Node> allNodes = new HashSet<>(nodes);
                final ListGroupsResults results = new ListGroupsResults(allNodes, all);

                for (final Node node : allNodes) {
                    final long nowList = time.milliseconds();
                    runnable.call(new Call("listGroups", deadline, new ConstantNodeIdProvider(node.id())) {
                        @Override
                        ListGroupsRequest.Builder createRequest(int timeoutMs) {
                            List<String> groupTypes = options.types()
                                .stream()
                                .map(GroupType::toString)
                                .collect(Collectors.toList());
                            List<String> groupStates = options.groupStates()
                                .stream()
                                .map(GroupState::toString)
                                .collect(Collectors.toList());
                            return new ListGroupsRequest.Builder(new ListGroupsRequestData()
                                .setTypesFilter(groupTypes)
                                .setStatesFilter(groupStates)
                            );
                        }

                        private void maybeAddGroup(ListGroupsResponseData.ListedGroup group) {
                            final String groupId = group.groupId();
                            final Optional<GroupType> type;
                            if (group.groupType() == null || group.groupType().isEmpty()) {
                                type = Optional.empty();
                            } else {
                                type = Optional.of(GroupType.parse(group.groupType()));
                            }
                            final String protocolType = group.protocolType();
                            final Optional<GroupState> groupState;
                            if (group.groupState() == null || group.groupState().isEmpty()) {
                                groupState = Optional.empty();
                            } else {
                                groupState = Optional.of(GroupState.parse(group.groupState()));
                            }
                            final GroupListing groupListing = new GroupListing(
                                groupId,
                                type,
                                protocolType,
                                groupState
                            );
                            results.addListing(groupListing);
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            final ListGroupsResponse response = (ListGroupsResponse) abstractResponse;
                            synchronized (results) {
                                Errors error = Errors.forCode(response.data().errorCode());
                                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                                    throw error.exception();
                                } else if (error != Errors.NONE) {
                                    results.addError(error.exception(), node);
                                } else {
                                    for (ListGroupsResponseData.ListedGroup group : response.data().groups()) {
                                        maybeAddGroup(group);
                                    }
                                }
                                results.tryComplete(node);
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            synchronized (results) {
                                results.addError(throwable, node);
                                results.tryComplete(node);
                            }
                        }
                    }, nowList);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                KafkaException exception = new KafkaException("Failed to find brokers to send ListGroups", throwable);
                all.complete(Collections.singletonList(exception));
            }
        }, nowMetadata);

        return new ListGroupsResult(all);
    }

    @Override
    public DescribeConsumerGroupsResult describeConsumerGroups(final Collection<String> groupIds,
                                                               final DescribeConsumerGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> future =
                DescribeConsumerGroupsHandler.newFuture(groupIds);
        DescribeConsumerGroupsHandler handler = new DescribeConsumerGroupsHandler(options.includeAuthorizedOperations(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeConsumerGroupsResult(future.all().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    private static final class ListConsumerGroupsResults {
        private final List<Throwable> errors;
        private final HashMap<String, ConsumerGroupListing> listings;
        private final HashSet<Node> remaining;
        private final KafkaFutureImpl<Collection<Object>> future;

        ListConsumerGroupsResults(Collection<Node> leaders,
                                  KafkaFutureImpl<Collection<Object>> future) {
            this.errors = new ArrayList<>();
            this.listings = new HashMap<>();
            this.remaining = new HashSet<>(leaders);
            this.future = future;
            tryComplete();
        }

        synchronized void addError(Throwable throwable, Node node) {
            ApiError error = ApiError.fromThrowable(throwable);
            if (error.message() == null || error.message().isEmpty()) {
                errors.add(error.error().exception("Error listing groups on " + node));
            } else {
                errors.add(error.error().exception("Error listing groups on " + node + ": " + error.message()));
            }
        }

        synchronized void addListing(ConsumerGroupListing listing) {
            listings.put(listing.groupId(), listing);
        }

        synchronized void tryComplete(Node leader) {
            remaining.remove(leader);
            tryComplete();
        }

        private synchronized void tryComplete() {
            if (remaining.isEmpty()) {
                ArrayList<Object> results = new ArrayList<>(listings.values());
                results.addAll(errors);
                future.complete(results);
            }
        }
    }

    @Override
    public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
        final KafkaFutureImpl<Collection<Object>> all = new KafkaFutureImpl<>();
        final long nowMetadata = time.milliseconds();
        final long deadline = calcDeadlineMs(nowMetadata, options.timeoutMs());
        runnable.call(new Call("findAllBrokers", deadline, new LeastLoadedNodeProvider()) {
            @Override
            MetadataRequest.Builder createRequest(int timeoutMs) {
                return new MetadataRequest.Builder(new MetadataRequestData()
                    .setTopics(Collections.emptyList())
                    .setAllowAutoTopicCreation(true));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                MetadataResponse metadataResponse = (MetadataResponse) abstractResponse;
                Collection<Node> nodes = metadataResponse.brokers();
                if (nodes.isEmpty())
                    throw new StaleMetadataException("Metadata fetch failed due to missing broker list");

                HashSet<Node> allNodes = new HashSet<>(nodes);
                final ListConsumerGroupsResults results = new ListConsumerGroupsResults(allNodes, all);

                for (final Node node : allNodes) {
                    final long nowList = time.milliseconds();
                    runnable.call(new Call("listConsumerGroups", deadline, new ConstantNodeIdProvider(node.id())) {
                        @Override
                        ListGroupsRequest.Builder createRequest(int timeoutMs) {
                            List<String> states = options.groupStates()
                                    .stream()
                                    .map(GroupState::toString)
                                    .collect(Collectors.toList());
                            List<String> groupTypes = options.types()
                                    .stream()
                                    .map(GroupType::toString)
                                    .collect(Collectors.toList());
                            return new ListGroupsRequest.Builder(new ListGroupsRequestData()
                                .setStatesFilter(states)
                                .setTypesFilter(groupTypes)
                            );
                        }

                        private void maybeAddConsumerGroup(ListGroupsResponseData.ListedGroup group) {
                            String protocolType = group.protocolType();
                            if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                                final String groupId = group.groupId();
                                final Optional<GroupState> groupState = group.groupState().isEmpty()
                                        ? Optional.empty()
                                        : Optional.of(GroupState.parse(group.groupState()));
                                final Optional<GroupType> type = group.groupType().isEmpty()
                                        ? Optional.empty()
                                        : Optional.of(GroupType.parse(group.groupType()));
                                final ConsumerGroupListing groupListing = new ConsumerGroupListing(
                                        groupId,
                                        groupState,
                                        type,
                                        protocolType.isEmpty()
                                    );
                                results.addListing(groupListing);
                            }
                        }

                        @Override
                        void handleResponse(AbstractResponse abstractResponse) {
                            final ListGroupsResponse response = (ListGroupsResponse) abstractResponse;
                            synchronized (results) {
                                Errors error = Errors.forCode(response.data().errorCode());
                                if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS || error == Errors.COORDINATOR_NOT_AVAILABLE) {
                                    throw error.exception();
                                } else if (error != Errors.NONE) {
                                    results.addError(error.exception(), node);
                                } else {
                                    for (ListGroupsResponseData.ListedGroup group : response.data().groups()) {
                                        maybeAddConsumerGroup(group);
                                    }
                                }
                                results.tryComplete(node);
                            }
                        }

                        @Override
                        void handleFailure(Throwable throwable) {
                            synchronized (results) {
                                results.addError(throwable, node);
                                results.tryComplete(node);
                            }
                        }
                    }, nowList);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                KafkaException exception = new KafkaException("Failed to find brokers to send ListGroups", throwable);
                all.complete(Collections.singletonList(exception));
            }
        }, nowMetadata);

        return new ListConsumerGroupsResult(all);
    }

    @Override
    public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(Map<String, ListConsumerGroupOffsetsSpec> groupSpecs,
                                                                   ListConsumerGroupOffsetsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, OffsetAndMetadata>> future =
                ListConsumerGroupOffsetsHandler.newFuture(groupSpecs.keySet());
        ListConsumerGroupOffsetsHandler handler =
            new ListConsumerGroupOffsetsHandler(groupSpecs, options.requireStable(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new ListConsumerGroupOffsetsResult(future.all());
    }

    @Override
    public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds, DeleteConsumerGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Void> future =
                DeleteConsumerGroupsHandler.newFuture(groupIds);
        DeleteConsumerGroupsHandler handler = new DeleteConsumerGroupsHandler(logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DeleteConsumerGroupsResult(future.all().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    @Override
    public DeleteConsumerGroupOffsetsResult deleteConsumerGroupOffsets(
            String groupId,
            Set<TopicPartition> partitions,
            DeleteConsumerGroupOffsetsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> future =
                DeleteConsumerGroupOffsetsHandler.newFuture(groupId);
        DeleteConsumerGroupOffsetsHandler handler = new DeleteConsumerGroupOffsetsHandler(groupId, partitions, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DeleteConsumerGroupOffsetsResult(future.get(CoordinatorKey.byGroupId(groupId)), partitions);
    }

    @Override
    public DescribeShareGroupsResult describeShareGroups(final Collection<String> groupIds,
                                                         final DescribeShareGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, ShareGroupDescription> future =
                DescribeShareGroupsHandler.newFuture(groupIds);
        DescribeShareGroupsHandler handler = new DescribeShareGroupsHandler(options.includeAuthorizedOperations(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeShareGroupsResult(future.all().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    @Override
    public DescribeClassicGroupsResult describeClassicGroups(final Collection<String> groupIds,
                                                             final DescribeClassicGroupsOptions options) {
        SimpleAdminApiFuture<CoordinatorKey, ClassicGroupDescription> future =
            DescribeClassicGroupsHandler.newFuture(groupIds);
        DescribeClassicGroupsHandler handler = new DescribeClassicGroupsHandler(options.includeAuthorizedOperations(), logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeClassicGroupsResult(future.all().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().idValue, Map.Entry::getValue)));
    }

    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    @Override
    public ElectLeadersResult electLeaders(
            final ElectionType electionType,
            final Set<TopicPartition> topicPartitions,
            ElectLeadersOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, Optional<Throwable>>> electionFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        runnable.call(new Call("electLeaders", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {

            @Override
            public ElectLeadersRequest.Builder createRequest(int timeoutMs) {
                return new ElectLeadersRequest.Builder(electionType, topicPartitions, timeoutMs);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                ElectLeadersResponse response = (ElectLeadersResponse) abstractResponse;
                Map<TopicPartition, Optional<Throwable>> result = ElectLeadersResponse.electLeadersResult(response.data());

                // For version == 0 then errorCode would be 0 which maps to Errors.NONE
                Errors error = Errors.forCode(response.data().errorCode());
                if (error != Errors.NONE) {
                    electionFuture.completeExceptionally(error.exception());
                    return;
                }

                electionFuture.complete(result);
            }

            @Override
            void handleFailure(Throwable throwable) {
                electionFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ElectLeadersResult(electionFuture);
    }

    @Override
    public AlterPartitionReassignmentsResult alterPartitionReassignments(
            Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments,
            AlterPartitionReassignmentsOptions options) {
        final Map<TopicPartition, KafkaFutureImpl<Void>> futures = new HashMap<>();
        final Map<String, Map<Integer, Optional<NewPartitionReassignment>>> topicsToReassignments = new TreeMap<>();
        for (Map.Entry<TopicPartition, Optional<NewPartitionReassignment>> entry : reassignments.entrySet()) {
            String topic = entry.getKey().topic();
            int partition = entry.getKey().partition();
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Optional<NewPartitionReassignment> reassignment = entry.getValue();
            KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
            futures.put(topicPartition, future);

            if (topicNameIsUnrepresentable(topic)) {
                future.completeExceptionally(new InvalidTopicException("The given topic name '" +
                        topic + "' cannot be represented in a request."));
            } else if (topicPartition.partition() < 0) {
                future.completeExceptionally(new InvalidTopicException("The given partition index " +
                        topicPartition.partition() + " is not valid."));
            } else {
                Map<Integer, Optional<NewPartitionReassignment>> partitionReassignments =
                        topicsToReassignments.get(topicPartition.topic());
                if (partitionReassignments == null) {
                    partitionReassignments = new TreeMap<>();
                    topicsToReassignments.put(topic, partitionReassignments);
                }

                partitionReassignments.put(partition, reassignment);
            }
        }

        final long now = time.milliseconds();
        Call call = new Call("alterPartitionReassignments", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider(true)) {

            @Override
            public AlterPartitionReassignmentsRequest.Builder createRequest(int timeoutMs) {
                AlterPartitionReassignmentsRequestData data =
                        new AlterPartitionReassignmentsRequestData();
                for (Map.Entry<String, Map<Integer, Optional<NewPartitionReassignment>>> entry :
                        topicsToReassignments.entrySet()) {
                    String topicName = entry.getKey();
                    Map<Integer, Optional<NewPartitionReassignment>> partitionsToReassignments = entry.getValue();

                    List<ReassignablePartition> reassignablePartitions = new ArrayList<>();
                    for (Map.Entry<Integer, Optional<NewPartitionReassignment>> partitionEntry :
                            partitionsToReassignments.entrySet()) {
                        int partitionIndex = partitionEntry.getKey();
                        Optional<NewPartitionReassignment> reassignment = partitionEntry.getValue();

                        ReassignablePartition reassignablePartition = new ReassignablePartition()
                                .setPartitionIndex(partitionIndex)
                                .setReplicas(reassignment.map(NewPartitionReassignment::targetReplicas).orElse(null));
                        reassignablePartitions.add(reassignablePartition);
                    }

                    ReassignableTopic reassignableTopic = new ReassignableTopic()
                            .setName(topicName)
                            .setPartitions(reassignablePartitions);
                    data.topics().add(reassignableTopic);
                }
                data.setTimeoutMs(timeoutMs);
                return new AlterPartitionReassignmentsRequest.Builder(data);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterPartitionReassignmentsResponse response = (AlterPartitionReassignmentsResponse) abstractResponse;
                Map<TopicPartition, ApiException> errors = new HashMap<>();
                int receivedResponsesCount = 0;

                Errors topLevelError = Errors.forCode(response.data().errorCode());
                switch (topLevelError) {
                    case NONE:
                        receivedResponsesCount += validateTopicResponses(response.data().responses(), errors);
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(topLevelError);
                        break;
                    default:
                        for (ReassignableTopicResponse topicResponse : response.data().responses()) {
                            String topicName = topicResponse.name();
                            for (ReassignablePartitionResponse partition : topicResponse.partitions()) {
                                errors.put(
                                        new TopicPartition(topicName, partition.partitionIndex()),
                                        new ApiError(topLevelError, response.data().errorMessage()).exception()
                                );
                                receivedResponsesCount += 1;
                            }
                        }
                        break;
                }

                assertResponseCountMatch(errors, receivedResponsesCount);
                for (Map.Entry<TopicPartition, ApiException> entry : errors.entrySet()) {
                    ApiException exception = entry.getValue();
                    if (exception == null)
                        futures.get(entry.getKey()).complete(null);
                    else
                        futures.get(entry.getKey()).completeExceptionally(exception);
                }
            }

            private void assertResponseCountMatch(Map<TopicPartition, ApiException> errors, int receivedResponsesCount) {
                int expectedResponsesCount = topicsToReassignments.values().stream().mapToInt(Map::size).sum();
                if (errors.values().stream().noneMatch(Objects::nonNull) && receivedResponsesCount != expectedResponsesCount) {
                    String quantifier = receivedResponsesCount > expectedResponsesCount ? "many" : "less";
                    throw new UnknownServerException("The server returned too " + quantifier + " results." +
                        "Expected " + expectedResponsesCount + " but received " + receivedResponsesCount);
                }
            }

            private int validateTopicResponses(List<ReassignableTopicResponse> topicResponses,
                                               Map<TopicPartition, ApiException> errors) {
                int receivedResponsesCount = 0;

                for (ReassignableTopicResponse topicResponse : topicResponses) {
                    String topicName = topicResponse.name();
                    for (ReassignablePartitionResponse partResponse : topicResponse.partitions()) {
                        Errors partitionError = Errors.forCode(partResponse.errorCode());

                        TopicPartition tp = new TopicPartition(topicName, partResponse.partitionIndex());
                        if (partitionError == Errors.NONE) {
                            errors.put(tp, null);
                        } else {
                            errors.put(tp, new ApiError(partitionError, partResponse.errorMessage()).exception());
                        }
                        receivedResponsesCount += 1;
                    }
                }

                return receivedResponsesCount;
            }

            @Override
            void handleFailure(Throwable throwable) {
                for (KafkaFutureImpl<Void> future : futures.values()) {
                    future.completeExceptionally(throwable);
                }
            }
        };
        if (!topicsToReassignments.isEmpty()) {
            runnable.call(call, now);
        }
        return new AlterPartitionReassignmentsResult(new HashMap<>(futures));
    }

    @Override
    public ListPartitionReassignmentsResult listPartitionReassignments(Optional<Set<TopicPartition>> partitions,
                                                                       ListPartitionReassignmentsOptions options) {
        final KafkaFutureImpl<Map<TopicPartition, PartitionReassignment>> partitionReassignmentsFuture = new KafkaFutureImpl<>();
        if (partitions.isPresent()) {
            for (TopicPartition tp : partitions.get()) {
                String topic = tp.topic();
                int partition = tp.partition();
                if (topicNameIsUnrepresentable(topic)) {
                    partitionReassignmentsFuture.completeExceptionally(new InvalidTopicException("The given topic name '"
                            + topic + "' cannot be represented in a request."));
                } else if (partition < 0) {
                    partitionReassignmentsFuture.completeExceptionally(new InvalidTopicException("The given partition index " +
                            partition + " is not valid."));
                }
                if (partitionReassignmentsFuture.isCompletedExceptionally())
                    return new ListPartitionReassignmentsResult(partitionReassignmentsFuture);
            }
        }
        final long now = time.milliseconds();
        runnable.call(new Call("listPartitionReassignments", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider(true)) {

            @Override
            ListPartitionReassignmentsRequest.Builder createRequest(int timeoutMs) {
                ListPartitionReassignmentsRequestData listData = new ListPartitionReassignmentsRequestData();
                listData.setTimeoutMs(timeoutMs);

                if (partitions.isPresent()) {
                    Map<String, ListPartitionReassignmentsTopics> reassignmentTopicByTopicName = new HashMap<>();

                    for (TopicPartition tp : partitions.get()) {
                        if (!reassignmentTopicByTopicName.containsKey(tp.topic()))
                            reassignmentTopicByTopicName.put(tp.topic(), new ListPartitionReassignmentsTopics().setName(tp.topic()));

                        reassignmentTopicByTopicName.get(tp.topic()).partitionIndexes().add(tp.partition());
                    }

                    listData.setTopics(new ArrayList<>(reassignmentTopicByTopicName.values()));
                }
                return new ListPartitionReassignmentsRequest.Builder(listData);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ListPartitionReassignmentsResponse response = (ListPartitionReassignmentsResponse) abstractResponse;
                Errors error = Errors.forCode(response.data().errorCode());
                switch (error) {
                    case NONE:
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(error);
                        break;
                    default:
                        partitionReassignmentsFuture.completeExceptionally(new ApiError(error, response.data().errorMessage()).exception());
                        break;
                }
                Map<TopicPartition, PartitionReassignment> reassignmentMap = new HashMap<>();

                for (OngoingTopicReassignment topicReassignment : response.data().topics()) {
                    String topicName = topicReassignment.name();
                    for (OngoingPartitionReassignment partitionReassignment : topicReassignment.partitions()) {
                        reassignmentMap.put(
                            new TopicPartition(topicName, partitionReassignment.partitionIndex()),
                            new PartitionReassignment(partitionReassignment.replicas(), partitionReassignment.addingReplicas(), partitionReassignment.removingReplicas())
                        );
                    }
                }

                partitionReassignmentsFuture.complete(reassignmentMap);
            }

            @Override
            void handleFailure(Throwable throwable) {
                partitionReassignmentsFuture.completeExceptionally(throwable);
            }
        }, now);

        return new ListPartitionReassignmentsResult(partitionReassignmentsFuture);
    }

    private void handleNotControllerError(AbstractResponse response) throws ApiException {
        if (response.errorCounts().containsKey(Errors.NOT_CONTROLLER)) {
            handleNotControllerError(Errors.NOT_CONTROLLER);
        }
    }

    private void handleNotControllerError(Errors error) throws ApiException {
        metadataManager.clearController();
        metadataManager.requestUpdate();
        throw error.exception();
    }

    /**
     * Returns the broker id pertaining to the given resource, or null if the resource is not associated
     * with a particular broker.
     */
    private Integer nodeFor(ConfigResource resource) {
        if ((resource.type() == ConfigResource.Type.BROKER && !resource.isDefault())
                || resource.type() == ConfigResource.Type.BROKER_LOGGER) {
            return Integer.valueOf(resource.name());
        } else {
            return null;
        }
    }

    private KafkaFutureImpl<List<MemberIdentity>> getMembersFromGroup(String groupId, String reason) {
        KafkaFutureImpl<List<MemberIdentity>> future = new KafkaFutureImpl<>();

        describeConsumerGroups(Collections.singleton(groupId)).describedGroups().get(groupId).whenComplete((res, ex) -> {
            if (ex != null) {
                future.completeExceptionally(new KafkaException("Encounter exception when trying to get members from group: " + groupId, ex));
            } else {
                List<MemberIdentity> membersToRemove = res.members().stream().map(member ->
                    member.groupInstanceId().map(id -> new MemberIdentity().setGroupInstanceId(id))
                    .orElseGet(() -> new MemberIdentity().setMemberId(member.consumerId()))
                    .setReason(reason)
                ).collect(Collectors.toList());

                future.complete(membersToRemove);
            }
        });

        return future;
    }

    @Override
    public void registerMetricForSubscription(KafkaMetric metric) {
        if (clientTelemetryReporter.isPresent()) {
            ClientTelemetryReporter reporter = clientTelemetryReporter.get();
            reporter.metricChange(metric);
        }
    }

    @Override
    public void unregisterMetricFromSubscription(KafkaMetric metric) {
        if (clientTelemetryReporter.isPresent()) {
            ClientTelemetryReporter reporter = clientTelemetryReporter.get();
            reporter.metricRemoval(metric);
        }
    }

    @Override
    public RemoveMembersFromConsumerGroupResult removeMembersFromConsumerGroup(String groupId,
                                                                               RemoveMembersFromConsumerGroupOptions options) {
        String reason = options.reason() == null || options.reason().isEmpty() ?
            DEFAULT_LEAVE_GROUP_REASON : JoinGroupRequest.maybeTruncateReason(options.reason());

        final SimpleAdminApiFuture<CoordinatorKey, Map<MemberIdentity, Errors>> adminFuture =
                RemoveMembersFromConsumerGroupHandler.newFuture(groupId);

        KafkaFutureImpl<List<MemberIdentity>> memFuture;
        if (options.removeAll()) {
            memFuture = getMembersFromGroup(groupId, reason);
        } else {
            memFuture = new KafkaFutureImpl<>();
            memFuture.complete(options.members().stream()
                    .map(m -> m.toMemberIdentity().setReason(reason))
                    .collect(Collectors.toList()));
        }

        memFuture.whenComplete((members, ex) -> {
            if (ex != null) {
                adminFuture.completeExceptionally(Collections.singletonMap(CoordinatorKey.byGroupId(groupId), ex));
            } else {
                RemoveMembersFromConsumerGroupHandler handler = new RemoveMembersFromConsumerGroupHandler(groupId, members, logContext);
                invokeDriver(handler, adminFuture, options.timeoutMs());
            }
        });

        return new RemoveMembersFromConsumerGroupResult(adminFuture.get(CoordinatorKey.byGroupId(groupId)), options.members());
    }

    @Override
    public AlterConsumerGroupOffsetsResult alterConsumerGroupOffsets(
        String groupId,
        Map<TopicPartition, OffsetAndMetadata> offsets,
        AlterConsumerGroupOffsetsOptions options
    ) {
        SimpleAdminApiFuture<CoordinatorKey, Map<TopicPartition, Errors>> future =
                AlterConsumerGroupOffsetsHandler.newFuture(groupId);
        AlterConsumerGroupOffsetsHandler handler = new AlterConsumerGroupOffsetsHandler(groupId, offsets, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new AlterConsumerGroupOffsetsResult(future.get(CoordinatorKey.byGroupId(groupId)));
    }

    @Override
    public ListOffsetsResult listOffsets(Map<TopicPartition, OffsetSpec> topicPartitionOffsets,
                                         ListOffsetsOptions options) {
        PartitionLeaderStrategy.PartitionLeaderFuture<ListOffsetsResultInfo> future =
            ListOffsetsHandler.newFuture(topicPartitionOffsets.keySet(), partitionLeaderCache);
        Map<TopicPartition, Long> offsetQueriesByPartition = topicPartitionOffsets.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> getOffsetFromSpec(e.getValue())));
        ListOffsetsHandler handler = new ListOffsetsHandler(offsetQueriesByPartition, options, logContext, defaultApiTimeoutMs);
        invokeDriver(handler, future, options.timeoutMs);
        return new ListOffsetsResult(future.all());
    }

    @Override
    public DescribeClientQuotasResult describeClientQuotas(ClientQuotaFilter filter, DescribeClientQuotasOptions options) {
        KafkaFutureImpl<Map<ClientQuotaEntity, Map<String, Double>>> future = new KafkaFutureImpl<>();

        final long now = time.milliseconds();
        runnable.call(new Call("describeClientQuotas", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                DescribeClientQuotasRequest.Builder createRequest(int timeoutMs) {
                    return new DescribeClientQuotasRequest.Builder(filter);
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    DescribeClientQuotasResponse response = (DescribeClientQuotasResponse) abstractResponse;
                    response.complete(future);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            }, now);

        return new DescribeClientQuotasResult(future);
    }

    @Override
    public AlterClientQuotasResult alterClientQuotas(Collection<ClientQuotaAlteration> entries, AlterClientQuotasOptions options) {
        Map<ClientQuotaEntity, KafkaFutureImpl<Void>> futures = new HashMap<>(entries.size());
        for (ClientQuotaAlteration entry : entries) {
            futures.put(entry.entity(), new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        runnable.call(new Call("alterClientQuotas", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

                @Override
                AlterClientQuotasRequest.Builder createRequest(int timeoutMs) {
                    return new AlterClientQuotasRequest.Builder(entries, options.validateOnly());
                }

                @Override
                void handleResponse(AbstractResponse abstractResponse) {
                    AlterClientQuotasResponse response = (AlterClientQuotasResponse) abstractResponse;
                    response.complete(futures);
                }

                @Override
                void handleFailure(Throwable throwable) {
                    completeAllExceptionally(futures.values(), throwable);
                }
            }, now);

        return new AlterClientQuotasResult(Collections.unmodifiableMap(futures));
    }

    @Override
    public DescribeUserScramCredentialsResult describeUserScramCredentials(List<String> users, DescribeUserScramCredentialsOptions options) {
        final KafkaFutureImpl<DescribeUserScramCredentialsResponseData> dataFuture = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        Call call = new Call("describeUserScramCredentials", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {
            @Override
            public DescribeUserScramCredentialsRequest.Builder createRequest(final int timeoutMs) {
                final DescribeUserScramCredentialsRequestData requestData = new DescribeUserScramCredentialsRequestData();

                if (users != null && !users.isEmpty()) {
                    final List<UserName> userNames = new ArrayList<>(users.size());

                    for (final String user : users) {
                        if (user != null) {
                            userNames.add(new UserName().setName(user));
                        }
                    }

                    requestData.setUsers(userNames);
                }

                return new DescribeUserScramCredentialsRequest.Builder(requestData);
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                DescribeUserScramCredentialsResponse response = (DescribeUserScramCredentialsResponse) abstractResponse;
                DescribeUserScramCredentialsResponseData data = response.data();
                short messageLevelErrorCode = data.errorCode();
                if (messageLevelErrorCode != Errors.NONE.code()) {
                    dataFuture.completeExceptionally(Errors.forCode(messageLevelErrorCode).exception(data.errorMessage()));
                } else {
                    dataFuture.complete(data);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                dataFuture.completeExceptionally(throwable);
            }
        };
        runnable.call(call, now);
        return new DescribeUserScramCredentialsResult(dataFuture);
    }

    @Override
    public AlterUserScramCredentialsResult alterUserScramCredentials(List<UserScramCredentialAlteration> alterations,
                                                                     AlterUserScramCredentialsOptions options) {
        final long now = time.milliseconds();
        final Map<String, KafkaFutureImpl<Void>> futures = new HashMap<>();
        for (UserScramCredentialAlteration alteration: alterations) {
            futures.put(alteration.user(), new KafkaFutureImpl<>());
        }
        final Map<String, Exception> userIllegalAlterationExceptions = new HashMap<>();
        // We need to keep track of users with deletions of an unknown SCRAM mechanism
        final String usernameMustNotBeEmptyMsg = "Username must not be empty";
        String passwordMustNotBeEmptyMsg = "Password must not be empty";
        final String unknownScramMechanismMsg = "Unknown SCRAM mechanism";
        alterations.stream().filter(a -> a instanceof UserScramCredentialDeletion).forEach(alteration -> {
            final String user = alteration.user();
            if (user == null || user.isEmpty()) {
                userIllegalAlterationExceptions.put(alteration.user(), new UnacceptableCredentialException(usernameMustNotBeEmptyMsg));
            } else {
                UserScramCredentialDeletion deletion = (UserScramCredentialDeletion) alteration;
                ScramMechanism mechanism = deletion.mechanism();
                if (mechanism == null || mechanism == ScramMechanism.UNKNOWN) {
                    userIllegalAlterationExceptions.put(user, new UnsupportedSaslMechanismException(unknownScramMechanismMsg));
                }
            }
        });
        // Creating an upsertion may throw InvalidKeyException or NoSuchAlgorithmException,
        // so keep track of which users are affected by such a failure so we can fail all their alterations later
        final Map<String, Map<ScramMechanism, AlterUserScramCredentialsRequestData.ScramCredentialUpsertion>> userInsertions = new HashMap<>();
        alterations.stream().filter(a -> a instanceof UserScramCredentialUpsertion)
                .filter(alteration -> !userIllegalAlterationExceptions.containsKey(alteration.user()))
                .forEach(alteration -> {
                    final String user = alteration.user();
                    if (user == null || user.isEmpty()) {
                        userIllegalAlterationExceptions.put(alteration.user(), new UnacceptableCredentialException(usernameMustNotBeEmptyMsg));
                    } else {
                        UserScramCredentialUpsertion upsertion = (UserScramCredentialUpsertion) alteration;
                        try {
                            byte[] password = upsertion.password();
                            if (password == null || password.length == 0) {
                                userIllegalAlterationExceptions.put(user, new UnacceptableCredentialException(passwordMustNotBeEmptyMsg));
                            } else {
                                ScramMechanism mechanism = upsertion.credentialInfo().mechanism();
                                if (mechanism == null || mechanism == ScramMechanism.UNKNOWN) {
                                    userIllegalAlterationExceptions.put(user, new UnsupportedSaslMechanismException(unknownScramMechanismMsg));
                                } else {
                                    userInsertions.putIfAbsent(user, new HashMap<>());
                                    userInsertions.get(user).put(mechanism, getScramCredentialUpsertion(upsertion));
                                }
                            }
                        } catch (NoSuchAlgorithmException e) {
                            // we might overwrite an exception from a previous alteration, but we don't really care
                            // since we just need to mark this user as having at least one illegal alteration
                            // and make an exception instance available for completing the corresponding future exceptionally
                            userIllegalAlterationExceptions.put(user, new UnsupportedSaslMechanismException(unknownScramMechanismMsg));
                        } catch (InvalidKeyException e) {
                            // generally shouldn't happen since we deal with the empty password case above,
                            // but we still need to catch/handle it
                            userIllegalAlterationExceptions.put(user, new UnacceptableCredentialException(e.getMessage(), e));
                        }
                    }
                });

        // submit alterations only for users that do not have an illegal alteration as identified above
        Call call = new Call("alterUserScramCredentials", calcDeadlineMs(now, options.timeoutMs()),
                new ControllerNodeProvider()) {
            @Override
            public AlterUserScramCredentialsRequest.Builder createRequest(int timeoutMs) {
                return new AlterUserScramCredentialsRequest.Builder(
                        new AlterUserScramCredentialsRequestData().setUpsertions(alterations.stream()
                                .filter(a -> a instanceof UserScramCredentialUpsertion)
                                .filter(a -> !userIllegalAlterationExceptions.containsKey(a.user()))
                                .map(a -> userInsertions.get(a.user()).get(((UserScramCredentialUpsertion) a).credentialInfo().mechanism()))
                                .collect(Collectors.toList()))
                        .setDeletions(alterations.stream()
                                .filter(a -> a instanceof UserScramCredentialDeletion)
                                .filter(a -> !userIllegalAlterationExceptions.containsKey(a.user()))
                                .map(d -> getScramCredentialDeletion((UserScramCredentialDeletion) d))
                                .collect(Collectors.toList())));
            }

            @Override
            public void handleResponse(AbstractResponse abstractResponse) {
                AlterUserScramCredentialsResponse response = (AlterUserScramCredentialsResponse) abstractResponse;
                // Check for controller change
                for (Errors error : response.errorCounts().keySet()) {
                    if (error == Errors.NOT_CONTROLLER) {
                        handleNotControllerError(error);
                    }
                }
                /* Now that we have the results for the ones we sent,
                 * fail any users that have an illegal alteration as identified above.
                 * Be sure to do this after the NOT_CONTROLLER error check above
                 * so that all errors are consistent in that case.
                 */
                userIllegalAlterationExceptions.entrySet().stream().forEach(entry ->
                    futures.get(entry.getKey()).completeExceptionally(entry.getValue())
                );
                response.data().results().forEach(result -> {
                    KafkaFutureImpl<Void> future = futures.get(result.user());
                    if (future == null) {
                        log.warn("Server response mentioned unknown user {}", result.user());
                    } else {
                        Errors error = Errors.forCode(result.errorCode());
                        if (error != Errors.NONE) {
                            future.completeExceptionally(error.exception(result.errorMessage()));
                        } else {
                            future.complete(null);
                        }
                    }
                });
                completeUnrealizedFutures(
                    futures.entrySet().stream(),
                    user -> "The broker response did not contain a result for user " + user);
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(futures.values(), throwable);
            }
        };
        runnable.call(call, now);
        return new AlterUserScramCredentialsResult(new HashMap<>(futures));
    }

    private static AlterUserScramCredentialsRequestData.ScramCredentialUpsertion getScramCredentialUpsertion(UserScramCredentialUpsertion u) throws InvalidKeyException, NoSuchAlgorithmException {
        AlterUserScramCredentialsRequestData.ScramCredentialUpsertion retval = new AlterUserScramCredentialsRequestData.ScramCredentialUpsertion();
        return retval.setName(u.user())
                .setMechanism(u.credentialInfo().mechanism().type())
                .setIterations(u.credentialInfo().iterations())
                .setSalt(u.salt())
                .setSaltedPassword(getSaltedPassword(u.credentialInfo().mechanism(), u.password(), u.salt(), u.credentialInfo().iterations()));
    }

    private static AlterUserScramCredentialsRequestData.ScramCredentialDeletion getScramCredentialDeletion(UserScramCredentialDeletion d) {
        return new AlterUserScramCredentialsRequestData.ScramCredentialDeletion().setName(d.user()).setMechanism(d.mechanism().type());
    }

    private static byte[] getSaltedPassword(ScramMechanism publicScramMechanism, byte[] password, byte[] salt, int iterations) throws NoSuchAlgorithmException, InvalidKeyException {
        return new ScramFormatter(org.apache.kafka.common.security.scram.internals.ScramMechanism.forMechanismName(publicScramMechanism.mechanismName()))
                .hi(password, salt, iterations);
    }

    @Override
    public DescribeFeaturesResult describeFeatures(final DescribeFeaturesOptions options) {
        final KafkaFutureImpl<FeatureMetadata> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        final Call call = new Call(
            "describeFeatures", calcDeadlineMs(now, options.timeoutMs()), new LeastLoadedBrokerOrActiveKController()) {

            private FeatureMetadata createFeatureMetadata(final ApiVersionsResponse response) {
                final Map<String, FinalizedVersionRange> finalizedFeatures = new HashMap<>();
                for (final FinalizedFeatureKey key : response.data().finalizedFeatures().valuesSet()) {
                    finalizedFeatures.put(key.name(), new FinalizedVersionRange(key.minVersionLevel(), key.maxVersionLevel()));
                }

                Optional<Long> finalizedFeaturesEpoch;
                if (response.data().finalizedFeaturesEpoch() >= 0L) {
                    finalizedFeaturesEpoch = Optional.of(response.data().finalizedFeaturesEpoch());
                } else {
                    finalizedFeaturesEpoch = Optional.empty();
                }

                final Map<String, SupportedVersionRange> supportedFeatures = new HashMap<>();
                for (final SupportedFeatureKey key : response.data().supportedFeatures().valuesSet()) {
                    supportedFeatures.put(key.name(), new SupportedVersionRange(key.minVersion(), key.maxVersion()));
                }

                return new FeatureMetadata(finalizedFeatures, finalizedFeaturesEpoch, supportedFeatures);
            }

            @Override
            ApiVersionsRequest.Builder createRequest(int timeoutMs) {
                return new ApiVersionsRequest.Builder();
            }

            @Override
            void handleResponse(AbstractResponse response) {
                final ApiVersionsResponse apiVersionsResponse = (ApiVersionsResponse) response;
                if (apiVersionsResponse.data().errorCode() == Errors.NONE.code()) {
                    future.complete(createFeatureMetadata(apiVersionsResponse));
                } else {
                    future.completeExceptionally(Errors.forCode(apiVersionsResponse.data().errorCode()).exception());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(Collections.singletonList(future), throwable);
            }
        };

        runnable.call(call, now);
        return new DescribeFeaturesResult(future);
    }

    @Override
    public UpdateFeaturesResult updateFeatures(final Map<String, FeatureUpdate> featureUpdates,
                                               final UpdateFeaturesOptions options) {
        if (featureUpdates.isEmpty()) {
            throw new IllegalArgumentException("Feature updates can not be null or empty.");
        }

        final Map<String, KafkaFutureImpl<Void>> updateFutures = new HashMap<>();
        for (final Map.Entry<String, FeatureUpdate> entry : featureUpdates.entrySet()) {
            final String feature = entry.getKey();
            if (Utils.isBlank(feature)) {
                throw new IllegalArgumentException("Provided feature can not be empty.");
            }
            updateFutures.put(entry.getKey(), new KafkaFutureImpl<>());
        }

        final long now = time.milliseconds();
        final Call call = new Call("updateFeatures", calcDeadlineMs(now, options.timeoutMs()),
            new ControllerNodeProvider(true)) {

            @Override
            UpdateFeaturesRequest.Builder createRequest(int timeoutMs) {
                final UpdateFeaturesRequestData.FeatureUpdateKeyCollection featureUpdatesRequestData
                    = new UpdateFeaturesRequestData.FeatureUpdateKeyCollection();
                for (Map.Entry<String, FeatureUpdate> entry : featureUpdates.entrySet()) {
                    final String feature = entry.getKey();
                    final FeatureUpdate update = entry.getValue();
                    final UpdateFeaturesRequestData.FeatureUpdateKey requestItem =
                        new UpdateFeaturesRequestData.FeatureUpdateKey();
                    requestItem.setFeature(feature);
                    requestItem.setMaxVersionLevel(update.maxVersionLevel());
                    requestItem.setUpgradeType(update.upgradeType().code());
                    featureUpdatesRequestData.add(requestItem);
                }
                return new UpdateFeaturesRequest.Builder(
                    new UpdateFeaturesRequestData()
                        .setTimeoutMs(timeoutMs)
                        .setValidateOnly(options.validateOnly())
                        .setFeatureUpdates(featureUpdatesRequestData));
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final UpdateFeaturesResponse response =
                    (UpdateFeaturesResponse) abstractResponse;

                ApiError topLevelError = response.topLevelError();
                switch (topLevelError.error()) {
                    case NONE:
                        // For V2 and above, None responses will just have a top level NONE error -- mark all the futures as completed.
                        if (response.data().results().isEmpty()) {
                            for (final KafkaFutureImpl<Void> future : updateFutures.values()) {
                                future.complete(null);
                            }
                        } else {
                            for (final UpdatableFeatureResult result : response.data().results()) {
                                final KafkaFutureImpl<Void> future = updateFutures.get(result.feature());
                                if (future == null) {
                                    log.warn("Server response mentioned unknown feature {}", result.feature());
                                } else {
                                    final Errors error = Errors.forCode(result.errorCode());
                                    if (error == Errors.NONE) {
                                        future.complete(null);
                                    } else {
                                        future.completeExceptionally(error.exception(result.errorMessage()));
                                    }
                                }
                            }
                            // The server should send back a response for every feature, but we do a sanity check anyway.
                            completeUnrealizedFutures(updateFutures.entrySet().stream(),
                                    feature -> "The controller response did not contain a result for feature " + feature);
                        }
                        break;
                    case NOT_CONTROLLER:
                        handleNotControllerError(topLevelError.error());
                        break;
                    default:
                        for (final Map.Entry<String, KafkaFutureImpl<Void>> entry : updateFutures.entrySet()) {
                            entry.getValue().completeExceptionally(topLevelError.exception());
                        }
                        break;
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                completeAllExceptionally(updateFutures.values(), throwable);
            }
        };

        runnable.call(call, now);
        return new UpdateFeaturesResult(new HashMap<>(updateFutures));
    }

    @Override
    public DescribeMetadataQuorumResult describeMetadataQuorum(DescribeMetadataQuorumOptions options) {
        NodeProvider provider = new LeastLoadedBrokerOrActiveKController();

        final KafkaFutureImpl<QuorumInfo> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        final Call call = new Call(
                "describeMetadataQuorum", calcDeadlineMs(now, options.timeoutMs()), provider) {

            private QuorumInfo.ReplicaState translateReplicaState(DescribeQuorumResponseData.ReplicaState replica) {
                return new QuorumInfo.ReplicaState(
                        replica.replicaId(),
                        replica.replicaDirectoryId() == null ? Uuid.ZERO_UUID : replica.replicaDirectoryId(),
                        replica.logEndOffset(),
                        replica.lastFetchTimestamp() == -1 ? OptionalLong.empty() : OptionalLong.of(replica.lastFetchTimestamp()),
                        replica.lastCaughtUpTimestamp() == -1 ? OptionalLong.empty() : OptionalLong.of(replica.lastCaughtUpTimestamp()));
            }

            private QuorumInfo createQuorumResult(final DescribeQuorumResponseData.PartitionData partition, DescribeQuorumResponseData.NodeCollection nodeCollection) {
                List<QuorumInfo.ReplicaState> voters = partition.currentVoters().stream()
                    .map(this::translateReplicaState)
                    .collect(Collectors.toList());

                List<QuorumInfo.ReplicaState> observers = partition.observers().stream()
                    .map(this::translateReplicaState)
                    .collect(Collectors.toList());

                Map<Integer, QuorumInfo.Node> nodes = nodeCollection.stream().map(n -> {
                    List<RaftVoterEndpoint> endpoints = n.listeners().stream()
                        .map(l -> new RaftVoterEndpoint(l.name(), l.host(), l.port()))
                        .collect(Collectors.toList());

                    return new QuorumInfo.Node(n.nodeId(), endpoints);
                }).collect(Collectors.toMap(QuorumInfo.Node::nodeId, Function.identity()));

                return new QuorumInfo(
                    partition.leaderId(),
                    partition.leaderEpoch(),
                    partition.highWatermark(),
                    voters,
                    observers,
                    nodes
                );
            }

            @Override
            DescribeQuorumRequest.Builder createRequest(int timeoutMs) {
                return new Builder(DescribeQuorumRequest.singletonRequest(
                        new TopicPartition(CLUSTER_METADATA_TOPIC_NAME, CLUSTER_METADATA_TOPIC_PARTITION.partition())));
            }

            @Override
            void handleResponse(AbstractResponse response) {
                final DescribeQuorumResponse quorumResponse = (DescribeQuorumResponse) response;
                if (quorumResponse.data().errorCode() != Errors.NONE.code()) {
                    throw Errors.forCode(quorumResponse.data().errorCode()).exception(quorumResponse.data().errorMessage());
                }
                if (quorumResponse.data().topics().size() != 1) {
                    String msg = String.format("DescribeMetadataQuorum received %d topics when 1 was expected",
                            quorumResponse.data().topics().size());
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                DescribeQuorumResponseData.TopicData topic = quorumResponse.data().topics().get(0);
                if (!topic.topicName().equals(CLUSTER_METADATA_TOPIC_NAME)) {
                    String msg = String.format("DescribeMetadataQuorum received a topic with name %s when %s was expected",
                            topic.topicName(), CLUSTER_METADATA_TOPIC_NAME);
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                if (topic.partitions().size() != 1) {
                    String msg = String.format("DescribeMetadataQuorum received a topic %s with %d partitions when 1 was expected",
                            topic.topicName(), topic.partitions().size());
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                DescribeQuorumResponseData.PartitionData partition = topic.partitions().get(0);
                if (partition.partitionIndex() != CLUSTER_METADATA_TOPIC_PARTITION.partition()) {
                    String msg = String.format("DescribeMetadataQuorum received a single partition with index %d when %d was expected",
                            partition.partitionIndex(), CLUSTER_METADATA_TOPIC_PARTITION.partition());
                    log.debug(msg);
                    throw new UnknownServerException(msg);
                }
                if (partition.errorCode() != Errors.NONE.code()) {
                    throw Errors.forCode(partition.errorCode()).exception(partition.errorMessage());
                }
                future.complete(createQuorumResult(partition, quorumResponse.data().nodes()));
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        };

        runnable.call(call, now);
        return new DescribeMetadataQuorumResult(future);
    }

    @Override
    public UnregisterBrokerResult unregisterBroker(int brokerId, UnregisterBrokerOptions options) {
        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        final Call call = new Call("unregisterBroker", calcDeadlineMs(now, options.timeoutMs()),
                new LeastLoadedNodeProvider()) {

            @Override
            UnregisterBrokerRequest.Builder createRequest(int timeoutMs) {
                UnregisterBrokerRequestData data =
                        new UnregisterBrokerRequestData().setBrokerId(brokerId);
                return new UnregisterBrokerRequest.Builder(data);
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                final UnregisterBrokerResponse response =
                        (UnregisterBrokerResponse) abstractResponse;
                Errors error = Errors.forCode(response.data().errorCode());
                switch (error) {
                    case NONE:
                        future.complete(null);
                        break;
                    case REQUEST_TIMED_OUT:
                        throw error.exception();
                    default:
                        log.error("Unregister broker request for broker ID {} failed: {}",
                            brokerId, error.message());
                        future.completeExceptionally(error.exception());
                        break;
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        };
        runnable.call(call, now);
        return new UnregisterBrokerResult(future);
    }

    @Override
    public DescribeProducersResult describeProducers(Collection<TopicPartition> topicPartitions, DescribeProducersOptions options) {
        PartitionLeaderStrategy.PartitionLeaderFuture<DescribeProducersResult.PartitionProducerState> future =
            DescribeProducersHandler.newFuture(topicPartitions, partitionLeaderCache);
        DescribeProducersHandler handler = new DescribeProducersHandler(options, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeProducersResult(future.all());
    }

    @Override
    public DescribeTransactionsResult describeTransactions(Collection<String> transactionalIds, DescribeTransactionsOptions options) {
        AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, TransactionDescription> future =
            DescribeTransactionsHandler.newFuture(transactionalIds);
        DescribeTransactionsHandler handler = new DescribeTransactionsHandler(logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new DescribeTransactionsResult(future.all());
    }

    @Override
    public AbortTransactionResult abortTransaction(AbortTransactionSpec spec, AbortTransactionOptions options) {
        PartitionLeaderStrategy.PartitionLeaderFuture<Void> future =
            AbortTransactionHandler.newFuture(Collections.singleton(spec.topicPartition()), partitionLeaderCache);
        AbortTransactionHandler handler = new AbortTransactionHandler(spec, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new AbortTransactionResult(future.all());
    }

    @Override
    public ListTransactionsResult listTransactions(ListTransactionsOptions options) {
        AllBrokersStrategy.AllBrokersFuture<Collection<TransactionListing>> future =
            ListTransactionsHandler.newFuture();
        ListTransactionsHandler handler = new ListTransactionsHandler(options, logContext);
        invokeDriver(handler, future, options.timeoutMs);
        return new ListTransactionsResult(future.all());
    }

    @Override
    public FenceProducersResult fenceProducers(Collection<String> transactionalIds, FenceProducersOptions options) {
        AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ProducerIdAndEpoch> future =
            FenceProducersHandler.newFuture(transactionalIds);
        FenceProducersHandler handler = new FenceProducersHandler(options, logContext, requestTimeoutMs);
        invokeDriver(handler, future, options.timeoutMs);
        return new FenceProducersResult(future.all());
    }

    @Override
    public ListClientMetricsResourcesResult listClientMetricsResources(ListClientMetricsResourcesOptions options) {
        final long now = time.milliseconds();
        final KafkaFutureImpl<Collection<ClientMetricsResourceListing>> future = new KafkaFutureImpl<>();
        runnable.call(new Call("listClientMetricsResources", calcDeadlineMs(now, options.timeoutMs()),
            new LeastLoadedNodeProvider()) {

            @Override
            ListClientMetricsResourcesRequest.Builder createRequest(int timeoutMs) {
                return new ListClientMetricsResourcesRequest.Builder(new ListClientMetricsResourcesRequestData());
            }

            @Override
            void handleResponse(AbstractResponse abstractResponse) {
                ListClientMetricsResourcesResponse response = (ListClientMetricsResourcesResponse) abstractResponse;
                if (response.error().isFailure()) {
                    future.completeExceptionally(response.error().exception());
                } else {
                    future.complete(response.clientMetricsResources());
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        }, now);
        return new ListClientMetricsResourcesResult(future);
    }

    @Override
    public AddRaftVoterResult addRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        Set<RaftVoterEndpoint> endpoints,
        AddRaftVoterOptions options
    ) {
        NodeProvider provider = new LeastLoadedBrokerOrActiveKController();

        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        final Call call = new Call(
                "addRaftVoter", calcDeadlineMs(now, options.timeoutMs()), provider) {

            @Override
            AddRaftVoterRequest.Builder createRequest(int timeoutMs) {
                AddRaftVoterRequestData.ListenerCollection listeners =
                    new AddRaftVoterRequestData.ListenerCollection();
                endpoints.forEach(endpoint ->
                    listeners.add(new AddRaftVoterRequestData.Listener().
                        setName(endpoint.name()).
                        setHost(endpoint.host()).
                        setPort(endpoint.port())));
                return new AddRaftVoterRequest.Builder(
                   new AddRaftVoterRequestData().
                       setClusterId(options.clusterId().orElse(null)).
                       setTimeoutMs(timeoutMs).
                       setVoterId(voterId) .
                       setVoterDirectoryId(voterDirectoryId).
                       setListeners(listeners));
            }

            @Override
            void handleResponse(AbstractResponse response) {
                AddRaftVoterResponse addResponse = (AddRaftVoterResponse) response;
                if (addResponse.data().errorCode() != Errors.NONE.code()) {
                    ApiError error = new ApiError(
                        addResponse.data().errorCode(),
                        addResponse.data().errorMessage());
                    future.completeExceptionally(error.exception());
                } else {
                    future.complete(null);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        };
        runnable.call(call, now);
        return new AddRaftVoterResult(future);
    }

    @Override
    public RemoveRaftVoterResult removeRaftVoter(
        int voterId,
        Uuid voterDirectoryId,
        RemoveRaftVoterOptions options
    ) {
        NodeProvider provider = new LeastLoadedBrokerOrActiveKController();

        final KafkaFutureImpl<Void> future = new KafkaFutureImpl<>();
        final long now = time.milliseconds();
        final Call call = new Call(
                "removeRaftVoter", calcDeadlineMs(now, options.timeoutMs()), provider) {

            @Override
            RemoveRaftVoterRequest.Builder createRequest(int timeoutMs) {
                return new RemoveRaftVoterRequest.Builder(
                    new RemoveRaftVoterRequestData().
                        setClusterId(options.clusterId().orElse(null)).
                        setVoterId(voterId) .
                        setVoterDirectoryId(voterDirectoryId));
            }

            @Override
            void handleResponse(AbstractResponse response) {
                RemoveRaftVoterResponse addResponse = (RemoveRaftVoterResponse) response;
                if (addResponse.data().errorCode() != Errors.NONE.code()) {
                    ApiError error = new ApiError(
                            addResponse.data().errorCode(),
                            addResponse.data().errorMessage());
                    future.completeExceptionally(error.exception());
                } else {
                    future.complete(null);
                }
            }

            @Override
            void handleFailure(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        };
        runnable.call(call, now);
        return new RemoveRaftVoterResult(future);
    }

    @Override
    public Uuid clientInstanceId(Duration timeout) {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("The timeout cannot be negative.");
        }

        if (clientTelemetryReporter.isEmpty()) {
            throw new IllegalStateException("Telemetry is not enabled. Set config `" + AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG + "` to `true`.");

        }

        if (clientInstanceId != null) {
            return clientInstanceId;
        }

        clientInstanceId = ClientTelemetryUtils.fetchClientInstanceId(clientTelemetryReporter.get(), timeout);
        return clientInstanceId;
    }

    private <K, V> void invokeDriver(
        AdminApiHandler<K, V> handler,
        AdminApiFuture<K, V> future,
        Integer timeoutMs
    ) {
        long currentTimeMs = time.milliseconds();
        long deadlineMs = calcDeadlineMs(currentTimeMs, timeoutMs);

        AdminApiDriver<K, V> driver = new AdminApiDriver<>(
            handler,
            future,
            deadlineMs,
            retryBackoffMs,
            retryBackoffMaxMs,
            logContext
        );

        maybeSendRequests(driver, currentTimeMs);
    }

    private <K, V> void maybeSendRequests(AdminApiDriver<K, V> driver, long currentTimeMs) {
        for (AdminApiDriver.RequestSpec<K> spec : driver.poll()) {
            runnable.call(newCall(driver, spec), currentTimeMs);
        }
    }

    private <K, V> Call newCall(AdminApiDriver<K, V> driver, AdminApiDriver.RequestSpec<K> spec) {
        NodeProvider nodeProvider = spec.scope.destinationBrokerId().isPresent() ?
            new ConstantNodeIdProvider(spec.scope.destinationBrokerId().getAsInt()) :
            new LeastLoadedNodeProvider();
        return new Call(spec.name, spec.nextAllowedTryMs, spec.tries, spec.deadlineMs, nodeProvider) {
            @Override
            AbstractRequest.Builder<?> createRequest(int timeoutMs) {
                return spec.request;
            }

            @Override
            void handleResponse(AbstractResponse response) {
                long currentTimeMs = time.milliseconds();
                driver.onResponse(currentTimeMs, spec, response, this.curNode());
                maybeSendRequests(driver, currentTimeMs);
            }

            @Override
            void handleFailure(Throwable throwable) {
                long currentTimeMs = time.milliseconds();
                driver.onFailure(currentTimeMs, spec, throwable);
                maybeSendRequests(driver, currentTimeMs);
            }

            @Override
            void maybeRetry(long currentTimeMs, Throwable throwable) {
                if (throwable instanceof DisconnectException) {
                    // Disconnects are a special case. We want to give the driver a chance
                    // to retry lookup rather than getting stuck on a node which is down.
                    // For example, if a partition leader shuts down after our metadata query,
                    // then we might get a disconnect. We want to try to find the new partition
                    // leader rather than retrying on the same node.
                    driver.onFailure(currentTimeMs, spec, throwable);
                    maybeSendRequests(driver, currentTimeMs);
                } else {
                    super.maybeRetry(currentTimeMs, throwable);
                }
            }
        };
    }

    private static long getOffsetFromSpec(OffsetSpec offsetSpec) {
        if (offsetSpec instanceof TimestampSpec) {
            return ((TimestampSpec) offsetSpec).timestamp();
        } else if (offsetSpec instanceof OffsetSpec.EarliestSpec) {
            return ListOffsetsRequest.EARLIEST_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.MaxTimestampSpec) {
            return ListOffsetsRequest.MAX_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.EarliestLocalSpec) {
            return ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
        } else if (offsetSpec instanceof OffsetSpec.LatestTieredSpec) {
            return ListOffsetsRequest.LATEST_TIERED_TIMESTAMP;
        }
        return ListOffsetsRequest.LATEST_TIMESTAMP;
    }

    /**
     * Get a sub level error when the request is in batch. If given key was not found,
     * return an {@link IllegalArgumentException}.
     */
    static <K> Throwable getSubLevelError(Map<K, Errors> subLevelErrors, K subKey, String keyNotFoundMsg) {
        if (!subLevelErrors.containsKey(subKey)) {
            return new IllegalArgumentException(keyNotFoundMsg);
        } else {
            return subLevelErrors.get(subKey).exception();
        }
    }
}
