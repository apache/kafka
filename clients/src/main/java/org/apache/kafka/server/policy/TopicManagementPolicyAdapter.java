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

package org.apache.kafka.server.policy;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;

import java.util.Map;

/**
 * Adapter for the deprecated
 * {@link CreateTopicPolicy} and {@link AlterConfigPolicy} so they can be used as
 * a {@link TopicManagementPolicy}.
 * @deprecated
 */
@Deprecated
public class TopicManagementPolicyAdapter implements TopicManagementPolicy {

    private final CreateTopicPolicy createTopicPolicy;
    private final AlterConfigPolicy alterConfigPolicy;

    public TopicManagementPolicyAdapter(CreateTopicPolicy createTopicPolicy, AlterConfigPolicy alterConfigPolicy) {
        this.createTopicPolicy = createTopicPolicy;
        this.alterConfigPolicy = alterConfigPolicy;
    }

    @Override
    public void validateCreateTopic(CreateTopicRequest request, ClusterState clusterState)
            throws PolicyViolationException {
        RequestTopicState state = request.requestedState();
        int numPartitions = state.numPartitions();
        CreateTopicPolicy.RequestMetadata meta = new CreateTopicPolicy.RequestMetadata(request.topic(),
                state.generatedReplicaAssignments() ? numPartitions : null,
                state.generatedReplicaAssignments() ? state.replicationFactor() : null,
                state.generatedReplicaAssignments() ? null : state.replicasAssignments(),
                state.configs());
        createTopicPolicy.validate(meta);
    }

    @Override
    public void validateAlterTopic(AlterTopicRequest request, ClusterState clusterState)
            throws PolicyViolationException {
        // Are the configs being changed?
        Map<String, String> oldConfigs = clusterState.topicState(request.topic()).configs();
        Map<String, String> newConfigs = request.requestedState().configs();
        if (!oldConfigs.equals(newConfigs)) {
            AlterConfigPolicy.RequestMetadata meta = new AlterConfigPolicy.RequestMetadata(
                    new ConfigResource(ConfigResource.Type.TOPIC, request.topic()),
                    newConfigs);
            alterConfigPolicy.validate(meta);
        }
    }

    @Override
    public void validateDeleteTopic(DeleteTopicRequest request, ClusterState clusterState)
            throws PolicyViolationException {
        // no legacy policy exists
    }

    @Override
    public void validateDeleteRecords(DeleteRecordsRequest request, ClusterState clusterState)
            throws PolicyViolationException {
        // no legacy policy exists
    }

    @Override
    public void close() throws Exception {
        try {
            createTopicPolicy.close();
        } finally {
            alterConfigPolicy.close();
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        createTopicPolicy.configure(configs);
        alterConfigPolicy.configure(configs);
    }
}
