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
import org.apache.kafka.common.errors.TopicExistsException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/** Customised ForwardingAdmin for testing only.
 * The class create/alter topics, partitions and ACLs in Kafka then store metadata in {@link FakeLocalMetadataStore}.
 * */

public class FakeForwardingAdminWithLocalMetadata extends ForwardingAdmin {
    private static final Logger log = LoggerFactory.getLogger(FakeForwardingAdminWithLocalMetadata.class);

    public FakeForwardingAdminWithLocalMetadata(Map<String, Object> configs) {
        super(configs);
    }

    @Override
    public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
        CreateTopicsResult createTopicsResult = super.createTopics(newTopics, options);
        newTopics.forEach(newTopic -> createTopicsResult.values().get(newTopic.name()).whenComplete((ignored, error) -> {
            if (error == null) {
                FakeLocalMetadataStore.addTopicToLocalMetadataStore(newTopic);
            } else if (error.getCause() instanceof TopicExistsException) {
                log.warn("Topic '{}' already exists. Update the local metadata store if absent", newTopic.name());
                FakeLocalMetadataStore.addTopicToLocalMetadataStore(newTopic);
            } else {
                log.error("Unable to intercept admin client operation", error);
            }
        }));
        return createTopicsResult;
    }

    @Override
    public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions, CreatePartitionsOptions options) {
        CreatePartitionsResult createPartitionsResult = super.createPartitions(newPartitions, options);
        newPartitions.forEach((topic, newPartition) -> createPartitionsResult.values().get(topic).whenComplete((ignored, error) -> {
            if (error == null) {
                FakeLocalMetadataStore.updatePartitionCount(topic, newPartition.totalCount());
            } else {
                log.error("Unable to intercept admin client operation", error);
            }
        }));
        return createPartitionsResult;
    }

    @Override
    public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
        CreateAclsResult aclsResult = super.createAcls(acls, options);
        aclsResult.values().forEach((aclBinding, future) -> future.whenComplete((ignored, error) -> {
            if (error == null) {
                FakeLocalMetadataStore.addACLs(aclBinding.entry().principal(), aclBinding);
            } else {
                log.error("Unable to intercept admin client operation", error);
            }
        }));
        return aclsResult;
    }
}
