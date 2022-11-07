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

package org.apache.kafka.connect.mirror.clients.admin;

import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ForwardingAdmin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Customised ForwardingAdmin for testing only.
 * The class create/alter topics, partitions and ACLs in Kafka then store metadata in {@link FakeLocalMetadataStore}.
 * */

public class FakeForwardingAdminWithLocalMetadata extends ForwardingAdmin {
    private static final Logger log = LoggerFactory.getLogger(FakeForwardingAdminWithLocalMetadata.class);
    private final long timeout = 1000L;

    public FakeForwardingAdminWithLocalMetadata(Map<String, Object> configs) {
        super(configs);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        CreateTopicsResult createTopicsResult = super.createTopics(newTopics, options);
        newTopics.forEach(newTopic -> {
            try {
                log.info("Add topic '{}' to cluster and metadata store", newTopic);
                // Wait for topic to be created before edit the fake local store
                createTopicsResult.values().get(newTopic.name()).get(timeout, TimeUnit.MILLISECONDS);
                FakeLocalMetadataStore.addTopicToLocalMetadataStore(newTopic);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    log.warn("Topic '{}' already exists. Update the local metadata store if absent", newTopic.name());
                    FakeLocalMetadataStore.addTopicToLocalMetadataStore(newTopic);
                } else
                    log.error(e.getMessage());
            }
        });
        return createTopicsResult;
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        CreatePartitionsResult createPartitionsResult = super.createPartitions(newPartitions, options);
        newPartitions.forEach((topic, newPartition) -> {
            try {
                // Wait for topic partition to be created before edit the fake local store
                createPartitionsResult.values().get(topic).get(timeout, TimeUnit.MILLISECONDS);
                FakeLocalMetadataStore.updatePartitionCount(topic, newPartition.totalCount());
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error(e.getMessage());
            }
        });
        return createPartitionsResult;
    }

    @Deprecated
    @Override
    public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
        AlterConfigsResult alterConfigsResult = super.alterConfigs(configs, options);
        configs.forEach((configResource, newConfigs) -> {
            try {
                if (configResource.type() == ConfigResource.Type.TOPIC) {
                    // Wait for config to be altered before edit the fake local store
                    alterConfigsResult.values().get(configResource).get(timeout, TimeUnit.MILLISECONDS);
                    FakeLocalMetadataStore.updateTopicConfig(configResource.name(), newConfigs);
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.error(e.getMessage());
            }
        });
        return alterConfigsResult;
    }


    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        CreateAclsResult aclsResult = super.createAcls(acls, options);
        try {
            // Wait for acls to be created before edit the fake local store
            aclsResult.all().get(timeout, TimeUnit.MILLISECONDS);
            acls.forEach(aclBinding -> {
                FakeLocalMetadataStore.addACLs(aclBinding.entry().principal(), aclBinding);
            });
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error(e.getMessage());
        }
        return aclsResult;
    }
}
