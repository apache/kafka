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

package org.apache.kafka.controller;

import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenRequestData;
import org.apache.kafka.common.message.CreateDelegationTokenResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ExpireDelegationTokenRequestData;
import org.apache.kafka.common.message.ExpireDelegationTokenResponseData;
import org.apache.kafka.common.message.ListPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.ListPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.RenewDelegationTokenRequestData;
import org.apache.kafka.common.message.RenewDelegationTokenResponseData;
import org.apache.kafka.common.message.UpdateFeaturesRequestData;
import org.apache.kafka.common.message.UpdateFeaturesResponseData;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.BrokerRegistrationReply;
import org.apache.kafka.metadata.FinalizedControllerFeatures;
import org.apache.kafka.metadata.authorizer.AclMutator;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public interface Controller extends AclMutator, AutoCloseable {
    /**
     * Change partition information.
     *
     * @param context       The controller request context.
     * @param request       The AlterPartitionRequest data.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<AlterPartitionResponseData> alterPartition(
        ControllerRequestContext context,
        AlterPartitionRequestData request
    );

    /**
     * Alter the user SCRAM credentials.
     *
     * @param context       The controller request context.
     * @param request       The AlterUserScramCredentialsRequest data.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<AlterUserScramCredentialsResponseData> alterUserScramCredentials(
        ControllerRequestContext context,
        AlterUserScramCredentialsRequestData request
    );

    /**
     * Create a DelegationToken for a specified user.
     *
     * @param context       The controller request context.
     * @param request       The CreateDelegationTokenRequest data.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<CreateDelegationTokenResponseData> createDelegationToken(
        ControllerRequestContext context,
        CreateDelegationTokenRequestData request
    );

    /**
     * Renew an existing DelegationToken for a specific TokenID.
     *
     * @param context       The controller request context.
     * @param request       The RenewDelegationTokenRequest data.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<RenewDelegationTokenResponseData> renewDelegationToken(
        ControllerRequestContext context,
        RenewDelegationTokenRequestData request
    );

    /**
     * Expire an existing DelegationToken for a specific TokenID.
     *
     * @param context       The controller request context.
     * @param request       The ExpireDelegationTokenRequest data.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<ExpireDelegationTokenResponseData> expireDelegationToken(
        ControllerRequestContext context,
        ExpireDelegationTokenRequestData request
    );

    /**
     * Create a batch of topics.
     *
     * @param context       The controller request context.
     * @param request       The CreateTopicsRequest data.
     * @param describable   The topics which we have DESCRIBE permission on.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<CreateTopicsResponseData> createTopics(
        ControllerRequestContext context,
        CreateTopicsRequestData request,
        Set<String> describable
    );

    /**
     * Unregister a broker.
     *
     * @param context       The controller request context.
     * @param brokerId      The broker id to unregister.
     *
     * @return              A future that is completed successfully when the broker is
     *                      unregistered.
     */
    CompletableFuture<Void> unregisterBroker(
        ControllerRequestContext context,
        int brokerId
    );

    /**
     * Find the ids for topic names.
     *
     * @param context       The controller request context.
     * @param topicNames    The topic names to resolve.
     * @return              A future yielding a map from topic name to id.
     */
    CompletableFuture<Map<String, ResultOrError<Uuid>>> findTopicIds(
        ControllerRequestContext context,
        Collection<String> topicNames
    );

    /**
     * Find the ids for all topic names. Note that this function should only be used for
     * integration tests.
     *
     * @param context       The controller request context.
     * @return              A future yielding a map from topic name to id.
     */
    CompletableFuture<Map<String, Uuid>> findAllTopicIds(
        ControllerRequestContext context
    );

    /**
     * Find the names for topic ids.
     *
     * @param context       The controller request context.
     * @param topicIds      The topic ids to resolve.
     * @return              A future yielding a map from topic id to name.
     */
    CompletableFuture<Map<Uuid, ResultOrError<String>>> findTopicNames(
        ControllerRequestContext context,
        Collection<Uuid> topicIds
    );

    /**
     * Delete a batch of topics.
     *
     * @param context       The controller request context.
     * @param topicIds      The IDs of the topics to delete.
     *
     * @return              A future yielding the response.
     */
    CompletableFuture<Map<Uuid, ApiError>> deleteTopics(
        ControllerRequestContext context,
        Collection<Uuid> topicIds
    );

    /**
     * Describe the current configuration of various resources.
     *
     * @param context       The controller request context.
     * @param resources     A map from resources to the collection of config keys that we
     *                      want to describe for each.  If the collection is empty, then
     *                      all configuration keys will be described.
     *
     * @return              A future yielding a map from config resources to results.
     */
    CompletableFuture<Map<ConfigResource, ResultOrError<Map<String, String>>>> describeConfigs(
        ControllerRequestContext context,
        Map<ConfigResource, Collection<String>> resources
    );

    /**
     * Elect new partition leaders.
     *
     * @param context       The controller request context.
     * @param request       The request.
     *
     * @return              A future yielding the elect leaders response.
     */
    CompletableFuture<ElectLeadersResponseData> electLeaders(
        ControllerRequestContext context,
        ElectLeadersRequestData request
    );

    /**
     * Get the current finalized feature ranges for each feature.
     *
     * @param context       The controller request context.
     *
     * @return              A future yielding the feature ranges.
     */
    CompletableFuture<FinalizedControllerFeatures> finalizedFeatures(
        ControllerRequestContext context
    );

    /**
     * Perform some incremental configuration changes.
     *
     * @param context       The controller request context.
     * @param configChanges The changes.
     * @param validateOnly  True if we should validate the changes but not apply them.
     *
     * @return              A future yielding a map from config resources to error results.
     */
    CompletableFuture<Map<ConfigResource, ApiError>> incrementalAlterConfigs(
        ControllerRequestContext context,
        Map<ConfigResource, Map<String, Map.Entry<AlterConfigOp.OpType, String>>> configChanges,
        boolean validateOnly
    );

    /**
     * Start or stop some partition reassignments.
     *
     * @param context       The controller request context.
     * @param request       The alter partition reassignments request.
     *
     * @return              A future yielding the results.
     */
    CompletableFuture<AlterPartitionReassignmentsResponseData> alterPartitionReassignments(
        ControllerRequestContext context,
        AlterPartitionReassignmentsRequestData request
    );

    /**
     * List ongoing partition reassignments.
     *
     * @param context       The controller request context.
     * @param request       The list partition reassignments request.
     *
     * @return              A future yielding the results.
     */
    CompletableFuture<ListPartitionReassignmentsResponseData> listPartitionReassignments(
        ControllerRequestContext context,
        ListPartitionReassignmentsRequestData request
    );

    /**
     * Perform some configuration changes using the legacy API.
     *
     * @param context       The controller request context.
     * @param newConfigs    The new configuration maps to apply.
     * @param validateOnly  True if we should validate the changes but not apply them.
     *
     * @return              A future yielding a map from config resources to error results.
     */
    CompletableFuture<Map<ConfigResource, ApiError>> legacyAlterConfigs(
        ControllerRequestContext context,
        Map<ConfigResource, Map<String, String>> newConfigs,
        boolean validateOnly
    );

    /**
     * Process a heartbeat from a broker.
     *
     * @param context       The controller request context.
     * @param request      The broker heartbeat request.
     *
     * @return             A future yielding the broker heartbeat reply.
     */
    CompletableFuture<BrokerHeartbeatReply> processBrokerHeartbeat(
        ControllerRequestContext context,
        BrokerHeartbeatRequestData request
    );

    /**
     * Attempt to register the given broker.
     *
     * @param context       The controller request context.
     * @param request      The registration request.
     *
     * @return             A future yielding the broker registration reply.
     */
    CompletableFuture<BrokerRegistrationReply> registerBroker(
        ControllerRequestContext context,
        BrokerRegistrationRequestData request
    );

    /**
     * Wait for the given number of brokers to be registered and unfenced.
     * This is for testing.
     *
     * @param minBrokers    The minimum number of brokers to wait for.
     * @return              A future which is completed when the given number of brokers
     *                      is reached.
     */
    CompletableFuture<Void> waitForReadyBrokers(int minBrokers);

    /**
     * Perform some client quota changes
     *
     * @param context           The controller request context.
     * @param quotaAlterations  The list of quotas to alter
     * @param validateOnly      True if we should validate the changes but not apply them.
     *
     * @return                  A future yielding a map of quota entities to error results.
     */
    CompletableFuture<Map<ClientQuotaEntity, ApiError>> alterClientQuotas(
        ControllerRequestContext context,
        Collection<ClientQuotaAlteration> quotaAlterations,
        boolean validateOnly
    );

    /**
     * Describe current quotas setting
     *
     * @param context           The controller request context.
     * @param quotaFilters      The list of quotas to filter
     *
     * @return                  A future yielding a map of quota entities to quota results.
     */
    CompletableFuture<Map<ClientQuotaEntity, Map<String, Double>>> describeClientQuotas(
            ControllerRequestContext context,
            ClientQuotaFilter quotaFilters
    );

    /**
     * Allocate a block of producer IDs for transactional and idempotent producers
     *
     * @param context   The controller request context.
     * @param request   The allocate producer IDs request
     *
     * @return          A future which yields a new producer ID block as a response
     */
    CompletableFuture<AllocateProducerIdsResponseData> allocateProducerIds(
        ControllerRequestContext context,
        AllocateProducerIdsRequestData request
    );

    /**
     * Update a set of feature flags
     *
     * @param context   The controller request context.
     * @param request   The update features request
     *
     * @return          A future which yields the result of the action
     */
    CompletableFuture<UpdateFeaturesResponseData> updateFeatures(
        ControllerRequestContext context,
        UpdateFeaturesRequestData request
    );

    /**
     * Create partitions on certain topics.
     *
     * @param topics        The list of topics to create partitions for.
     * @param validateOnly  If true, the request is validated, but no partitions will be created.
     *
     * @return              A future yielding per-topic results.
     */
    CompletableFuture<List<CreatePartitionsTopicResult>> createPartitions(
        ControllerRequestContext context,
        List<CreatePartitionsTopic> topics,
        boolean validateOnly
    );

    /**
     * Attempt to register the given controller.
     *
     * @param context       The controller request context.
     * @param request       The registration request.
     *
     * @return              A future yielding the broker registration reply.
     */
    CompletableFuture<Void> registerController(
        ControllerRequestContext context,
        ControllerRegistrationRequestData request
    );

    /**
     * Assign replicas to directories.
     *
     * @param context       The controller request context.
     * @param request       The assign replicas to dirs request.
     *
     * @return              A future yielding the results.
     */
    CompletableFuture<AssignReplicasToDirsResponseData> assignReplicasToDirs(
        ControllerRequestContext context,
        AssignReplicasToDirsRequestData request
    );

    /**
     * Begin shutting down, but don't block.  You must still call close to clean up all
     * resources.
     */
    void beginShutdown();

    /**
     * If this controller is active, this is the non-negative controller epoch.
     * Otherwise, this is -1.
     */
    int curClaimEpoch();

    /**
     * Returns true if this controller is currently active.
     */
    default boolean isActive() {
        return curClaimEpoch() != -1;
    }

    /**
     * Blocks until we have shut down and freed all resources.
     */
    void close() throws InterruptedException;
}
