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

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.message.CreateAclsResponseData;
import org.apache.kafka.common.message.DeleteAclsResponseData;
import org.apache.kafka.common.message.DescribeAclsResponseData;
import org.apache.kafka.common.message.DescribeConfigsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData.CredentialInfo;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData;
import org.apache.kafka.common.message.IncrementalAlterConfigsResponseData.AlterConfigsResourceResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;
import org.apache.kafka.common.requests.AlterClientQuotasResponse;
import org.apache.kafka.common.requests.AlterUserScramCredentialsResponse;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.CreateAclsResponse;
import org.apache.kafka.common.requests.DeleteAclsResponse;
import org.apache.kafka.common.requests.DescribeAclsResponse;
import org.apache.kafka.common.requests.DescribeClientQuotasResponse;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.DescribeUserScramCredentialsResponse;
import org.apache.kafka.common.requests.IncrementalAlterConfigsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaAdminClientConfigTest extends KafkaAdminClientTestBase {

    @Test
    public void testDescribeAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we get back ACL1 and ACL2.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(new DescribeAclsResponseData()
                .setResources(DescribeAclsResponse.aclsResources(asList(ACL1, ACL2))), ApiKeys.DESCRIBE_ACLS.latestVersion()));
            assertCollectionIs(env.adminClient().describeAcls(FILTER1).values().get(), ACL1, ACL2);

            // Test a call where we get back no results.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(new DescribeAclsResponseData(),
                    ApiKeys.DESCRIBE_ACLS.latestVersion()));
            assertTrue(env.adminClient().describeAcls(FILTER2).values().get().isEmpty());

            // Test a call where we get back an error.
            env.kafkaClient().prepareResponse(new DescribeAclsResponse(new DescribeAclsResponseData()
                .setErrorCode(Errors.SECURITY_DISABLED.code())
                .setErrorMessage("Security is disabled"), ApiKeys.DESCRIBE_ACLS.latestVersion()));
            TestUtils.assertFutureThrows(SecurityDisabledException.class, env.adminClient().describeAcls(FILTER2).values());

            // Test a call where we supply an invalid filter.
            TestUtils.assertFutureThrows(InvalidRequestException.class, env.adminClient().describeAcls(UNKNOWN_FILTER).values());
        }
    }

    @Test
    public void testCreateAclsToController() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "dummy")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
                    new CreateAclsResponseData.AclCreationResult()
                            .setErrorCode(Errors.NOT_CONTROLLER.code())
                            .setErrorMessage("not controller")))));
            // should retry the describe cluster to update the metadata
            env.kafkaClient().prepareResponse(
                    prepareDescribeClusterResponse(0,
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            2,
                            MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                            true)
            );

            // Test a call where we successfully create two ACLs.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
                    new CreateAclsResponseData.AclCreationResult()))));

            CreateAclsResult results = env.adminClient().createAcls(asList(ACL1));
            assertCollectionIs(results.values().keySet(), ACL1);
            for (KafkaFuture<Void> future : results.values().values())
                future.get();
            results.all().get();
        }
    }

    @Test
    public void testDeleteAclsToController() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "dummy")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                    .setThrottleTimeMs(0)
                    .setFilterResults(asList(new DeleteAclsResponseData.DeleteAclsFilterResult()
                                    .setErrorCode(Errors.NOT_CONTROLLER.code())
                                    .setErrorMessage("not controller"))),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            // should retry the describe cluster to update the metadata
            env.kafkaClient().prepareResponse(
                    prepareDescribeClusterResponse(0,
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            2,
                            MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                            true)
            );
            // Test a call where there are no errors.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                    .setThrottleTimeMs(0)
                    .setFilterResults(asList(
                            new DeleteAclsResponseData.DeleteAclsFilterResult()
                                    .setMatchingAcls(singletonList(DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE))))),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            DeleteAclsResult results = env.adminClient().deleteAcls(asList(FILTER1));
            Collection<AclBinding> deleted = results.all().get();
            assertCollectionIs(deleted, ACL1);
        }
    }

    @Test
    public void testCreateAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where we successfully create two ACLs.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
                new CreateAclsResponseData.AclCreationResult(),
                new CreateAclsResponseData.AclCreationResult()))));
            CreateAclsResult results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            for (KafkaFuture<Void> future : results.values().values())
                future.get();
            results.all().get();

            // Test a call where we fail to create one ACL.
            env.kafkaClient().prepareResponse(new CreateAclsResponse(new CreateAclsResponseData().setResults(asList(
                new CreateAclsResponseData.AclCreationResult()
                    .setErrorCode(Errors.SECURITY_DISABLED.code())
                    .setErrorMessage("Security is disabled"),
                new CreateAclsResponseData.AclCreationResult()))));
            results = env.adminClient().createAcls(asList(ACL1, ACL2));
            assertCollectionIs(results.values().keySet(), ACL1, ACL2);
            TestUtils.assertFutureThrows(SecurityDisabledException.class, results.values().get(ACL1));
            results.values().get(ACL2).get();
            TestUtils.assertFutureThrows(SecurityDisabledException.class, results.all());
        }
    }

    @Test
    public void testDeleteAcls() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            // Test a call where one filter has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(asList(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(asList(
                            DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE),
                            DeleteAclsResponse.matchingAcl(ACL2, ApiError.NONE))),
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setErrorCode(Errors.SECURITY_DISABLED.code())
                        .setErrorMessage("No security"))),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            DeleteAclsResult results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Map<AclBindingFilter, KafkaFuture<FilterResults>> filterResults = results.values();
            FilterResults filter1Results = filterResults.get(FILTER1).get();
            assertNull(filter1Results.values().get(0).exception());
            assertEquals(ACL1, filter1Results.values().get(0).binding());
            assertNull(filter1Results.values().get(1).exception());
            assertEquals(ACL2, filter1Results.values().get(1).binding());
            TestUtils.assertFutureThrows(SecurityDisabledException.class, filterResults.get(FILTER2));
            TestUtils.assertFutureThrows(SecurityDisabledException.class, results.all());

            // Test a call where one deletion result has an error.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(asList(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(asList(
                            DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE),
                            new DeleteAclsResponseData.DeleteAclsMatchingAcl()
                                .setErrorCode(Errors.SECURITY_DISABLED.code())
                                .setErrorMessage("No security")
                                .setPermissionType(AclPermissionType.ALLOW.code())
                                .setOperation(AclOperation.ALTER.code())
                                .setResourceType(ResourceType.CLUSTER.code())
                                .setPatternType(FILTER2.patternFilter().patternType().code()))),
                    new DeleteAclsResponseData.DeleteAclsFilterResult())),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            assertTrue(results.values().get(FILTER2).get().values().isEmpty());
            TestUtils.assertFutureThrows(SecurityDisabledException.class, results.all());

            // Test a call where there are no errors.
            env.kafkaClient().prepareResponse(new DeleteAclsResponse(new DeleteAclsResponseData()
                .setThrottleTimeMs(0)
                .setFilterResults(asList(
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(singletonList(DeleteAclsResponse.matchingAcl(ACL1, ApiError.NONE))),
                    new DeleteAclsResponseData.DeleteAclsFilterResult()
                        .setMatchingAcls(singletonList(DeleteAclsResponse.matchingAcl(ACL2, ApiError.NONE))))),
                    ApiKeys.DELETE_ACLS.latestVersion()));
            results = env.adminClient().deleteAcls(asList(FILTER1, FILTER2));
            Collection<AclBinding> deleted = results.all().get();
            assertCollectionIs(deleted, ACL1, ACL2);
        }
    }

    @Test
    public void testDescribeBrokerConfigs() throws Exception {
        ConfigResource broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource broker1Resource = new ConfigResource(ConfigResource.Type.BROKER, "1");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(new DescribeConfigsResponse(
                    new DescribeConfigsResponseData().setResults(singletonList(new DescribeConfigsResponseData.DescribeConfigsResult()
                            .setResourceName(broker0Resource.name()).setResourceType(broker0Resource.type().id()).setErrorCode(Errors.NONE.code())
                            .setConfigs(emptyList())))), env.cluster().nodeById(0));
            env.kafkaClient().prepareResponseFrom(new DescribeConfigsResponse(
                    new DescribeConfigsResponseData().setResults(singletonList(new DescribeConfigsResponseData.DescribeConfigsResult()
                            .setResourceName(broker1Resource.name()).setResourceType(broker1Resource.type().id()).setErrorCode(Errors.NONE.code())
                            .setConfigs(emptyList())))), env.cluster().nodeById(1));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    broker0Resource,
                    broker1Resource)).values();
            assertEquals(Set.of(broker0Resource, broker1Resource), result.keySet());
            result.get(broker0Resource).get();
            result.get(broker1Resource).get();
        }
    }

    @Test
    public void testDescribeBrokerAndLogConfigs() throws Exception {
        ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "0");
        ConfigResource brokerLoggerResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, "0");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponseFrom(new DescribeConfigsResponse(
                new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                    .setResourceName(brokerResource.name()).setResourceType(brokerResource.type().id()).setErrorCode(Errors.NONE.code())
                    .setConfigs(emptyList()),
                    new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setResourceName(brokerLoggerResource.name()).setResourceType(brokerLoggerResource.type().id()).setErrorCode(Errors.NONE.code())
                        .setConfigs(emptyList())))), env.cluster().nodeById(0));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    brokerResource,
                    brokerLoggerResource)).values();
            assertEquals(Set.of(brokerResource, brokerLoggerResource), result.keySet());
            result.get(brokerResource).get();
            result.get(brokerLoggerResource).get();
        }
    }

    @Test
    public void testDescribeConfigsPartialResponse() {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
        ConfigResource topic2 = new ConfigResource(ConfigResource.Type.TOPIC, "topic2");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(
                    new DescribeConfigsResponseData().setResults(singletonList(new DescribeConfigsResponseData.DescribeConfigsResult()
                            .setResourceName(topic.name()).setResourceType(topic.type().id()).setErrorCode(Errors.NONE.code())
                            .setConfigs(emptyList())))));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                    topic,
                    topic2)).values();
            assertEquals(Set.of(topic, topic2), result.keySet());
            result.get(topic);
            TestUtils.assertFutureThrows(ApiException.class, result.get(topic2));
        }
    }

    @Test
    public void testDescribeConfigsUnrequested() throws Exception {
        ConfigResource topic = new ConfigResource(ConfigResource.Type.TOPIC, "topic");
        ConfigResource unrequested = new ConfigResource(ConfigResource.Type.TOPIC, "unrequested");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(
                new DescribeConfigsResponseData().setResults(asList(new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setResourceName(topic.name()).setResourceType(topic.type().id()).setErrorCode(Errors.NONE.code())
                        .setConfigs(emptyList()),
                    new DescribeConfigsResponseData.DescribeConfigsResult()
                            .setResourceName(unrequested.name()).setResourceType(unrequested.type().id()).setErrorCode(Errors.NONE.code())
                            .setConfigs(emptyList())))));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(singletonList(
                    topic)).values();
            assertEquals(Set.of(topic), result.keySet());
            assertNotNull(result.get(topic).get());
            assertNull(result.get(unrequested));
        }
    }

    @Test
    public void testDescribeClientMetricsConfigs() throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "sub1");
        ConfigResource resource1 = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "sub2");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(
                new DescribeConfigsResponseData().setResults(asList(
                    new DescribeConfigsResponseData.DescribeConfigsResult()
                    .setResourceName(resource.name()).setResourceType(resource.type().id()).setErrorCode(Errors.NONE.code())
                    .setConfigs(emptyList()),
                    new DescribeConfigsResponseData.DescribeConfigsResult()
                    .setResourceName(resource1.name()).setResourceType(resource1.type().id()).setErrorCode(Errors.NONE.code())
                    .setConfigs(emptyList())))));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                resource,
                resource1)).values();
            assertEquals(Set.of(resource, resource1), result.keySet());
            assertNotNull(result.get(resource).get());
            assertNotNull(result.get(resource1).get());
        }
    }

    @Test
    public void testDescribeConsumerGroupConfigs() throws Exception {
        ConfigResource resource1 = new ConfigResource(ConfigResource.Type.GROUP, "group1");
        ConfigResource resource2 = new ConfigResource(ConfigResource.Type.GROUP, "group2");
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(new DescribeConfigsResponse(
                new DescribeConfigsResponseData().setResults(asList(
                    new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setResourceName(resource1.name())
                        .setResourceType(resource1.type().id())
                        .setErrorCode(Errors.NONE.code())
                        .setConfigs(emptyList()),
                    new DescribeConfigsResponseData.DescribeConfigsResult()
                        .setResourceName(resource2.name())
                        .setResourceType(resource2.type().id())
                        .setErrorCode(Errors.NONE.code())
                        .setConfigs(emptyList())))));
            Map<ConfigResource, KafkaFuture<Config>> result = env.adminClient().describeConfigs(asList(
                resource1,
                resource2)).values();
            assertEquals(Set.of(resource1, resource2), result.keySet());
            assertNotNull(result.get(resource1).get());
            assertNotNull(result.get(resource2).get());
        }
    }

    @Test
    public void testIncrementalAlterConfigs()  throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //test error scenarios
            IncrementalAlterConfigsResponseData responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.CLUSTER_AUTHORIZATION_FAILED.code())
                    .setErrorMessage("authorization error"));

            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("metric1")
                    .setResourceType(ConfigResource.Type.CLIENT_METRICS.id())
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Subscription is not allowed"));

            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("topic1")
                    .setResourceType(ConfigResource.Type.TOPIC.id())
                    .setErrorCode(Errors.INVALID_REQUEST.code())
                    .setErrorMessage("Config value append is not allowed for config"));

            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("group1")
                    .setResourceType(ConfigResource.Type.GROUP.id())
                    .setErrorCode(Errors.INVALID_CONFIG.code())
                    .setErrorMessage("Unknown group config name: group.initial.rebalance.delay.ms"));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "topic1");
            ConfigResource metricResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, "metric1");
            ConfigResource groupResource = new ConfigResource(ConfigResource.Type.GROUP, "group1");

            AlterConfigOp alterConfigOp1 = new AlterConfigOp(
                    new ConfigEntry("log.segment.bytes", "1073741"),
                    AlterConfigOp.OpType.SET);

            AlterConfigOp alterConfigOp2 = new AlterConfigOp(
                    new ConfigEntry("compression.type", "gzip"),
                    AlterConfigOp.OpType.APPEND);

            AlterConfigOp alterConfigOp3 = new AlterConfigOp(
                    new ConfigEntry("interval.ms", "1000"),
                    AlterConfigOp.OpType.APPEND);

            AlterConfigOp alterConfigOp4 = new AlterConfigOp(
                    new ConfigEntry("group.initial.rebalance.delay.ms", "1000"),
                    AlterConfigOp.OpType.SET);

            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(brokerResource, singletonList(alterConfigOp1));
            configs.put(topicResource, singletonList(alterConfigOp2));
            configs.put(metricResource, singletonList(alterConfigOp3));
            configs.put(groupResource, singletonList(alterConfigOp4));

            AlterConfigsResult result = env.adminClient().incrementalAlterConfigs(configs);
            TestUtils.assertFutureThrows(ClusterAuthorizationException.class, result.values().get(brokerResource));
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.values().get(topicResource));
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.values().get(metricResource));
            TestUtils.assertFutureThrows(InvalidConfigurationException.class, result.values().get(groupResource));

            // Test a call where there are no errors.
            responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("metric1")
                    .setResourceType(ConfigResource.Type.CLIENT_METRICS.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("group1")
                    .setResourceType(ConfigResource.Type.GROUP.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));

            final Map<ConfigResource, Collection<AlterConfigOp>> successConfig = new HashMap<>();
            successConfig.put(brokerResource, singletonList(alterConfigOp1));
            successConfig.put(metricResource, singletonList(alterConfigOp3));
            successConfig.put(groupResource, singletonList(alterConfigOp4));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));
            env.adminClient().incrementalAlterConfigs(successConfig).all().get();
        }
    }

    @Test
    public void testIncrementalAlterConfigsToController()  throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, "dummy")) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            //test NOT_CONTROLLER error scenarios
            IncrementalAlterConfigsResponseData responseData =  new IncrementalAlterConfigsResponseData();
            responseData.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.NOT_CONTROLLER.code())
                    .setErrorMessage("not controller"));

            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData));

            // should retry the describe cluster to update the metadata
            env.kafkaClient().prepareResponse(
                    prepareDescribeClusterResponse(0,
                            env.cluster().nodes(),
                            env.cluster().clusterResource().clusterId(),
                            2,
                            MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED,
                            true)
            );

            IncrementalAlterConfigsResponseData responseData2 =  new IncrementalAlterConfigsResponseData();
            responseData2.responses().add(new AlterConfigsResourceResponse()
                    .setResourceName("")
                    .setResourceType(ConfigResource.Type.BROKER.id())
                    .setErrorCode(Errors.NONE.code())
                    .setErrorMessage(ApiError.NONE.message()));

            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");

            AlterConfigOp alterConfigOp1 = new AlterConfigOp(
                    new ConfigEntry("log.segment.bytes", "1073741"),
                    AlterConfigOp.OpType.SET);

            final Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
            configs.put(brokerResource, singletonList(alterConfigOp1));
            env.kafkaClient().prepareResponse(new IncrementalAlterConfigsResponse(responseData2));
            env.adminClient().incrementalAlterConfigs(configs).all().get();
        }
    }

    @Test
    public void testDescribeClientQuotas() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String value = "value";

            Map<ClientQuotaEntity, Map<String, Double>> responseData = new HashMap<>();
            ClientQuotaEntity entity1 = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1", ClientQuotaEntity.CLIENT_ID, value);
            ClientQuotaEntity entity2 = newClientQuotaEntity(ClientQuotaEntity.USER, "user-2", ClientQuotaEntity.CLIENT_ID, value);
            responseData.put(entity1, Collections.singletonMap("consumer_byte_rate", 10000.0));
            responseData.put(entity2, Collections.singletonMap("producer_byte_rate", 20000.0));

            env.kafkaClient().prepareResponse(DescribeClientQuotasResponse.fromQuotaEntities(responseData, 0));

            ClientQuotaFilter filter = ClientQuotaFilter.contains(singletonList(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, value)));

            DescribeClientQuotasResult result = env.adminClient().describeClientQuotas(filter);
            Map<ClientQuotaEntity, Map<String, Double>> resultData = result.entities().get();
            assertEquals(2, resultData.size());
            assertTrue(resultData.containsKey(entity1));
            Map<String, Double> config1 = resultData.get(entity1);
            assertEquals(1, config1.size());
            assertEquals(10000.0, config1.get("consumer_byte_rate"), 1e-6);
            assertTrue(resultData.containsKey(entity2));
            Map<String, Double> config2 = resultData.get(entity2);
            assertEquals(1, config2.size());
            assertEquals(20000.0, config2.get("producer_byte_rate"), 1e-6);
        }
    }

    @Test
    public void testEqualsOfClientQuotaFilterComponent() {
        assertEquals(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));

        assertEquals(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));

        // match = null is different from match = Empty
        assertNotEquals(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));

        assertEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"));

        assertNotEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));

        assertNotEquals(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, "user"),
            ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));
    }

    @Test
    public void testAlterClientQuotas() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            ClientQuotaEntity goodEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-1");
            ClientQuotaEntity unauthorizedEntity = newClientQuotaEntity(ClientQuotaEntity.USER, "user-0");
            ClientQuotaEntity invalidEntity = newClientQuotaEntity("", "user-0");

            Map<ClientQuotaEntity, ApiError> responseData = new HashMap<>(2);
            responseData.put(goodEntity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Authorization failed"));
            responseData.put(unauthorizedEntity, new ApiError(Errors.CLUSTER_AUTHORIZATION_FAILED, "Authorization failed"));
            responseData.put(invalidEntity, new ApiError(Errors.INVALID_REQUEST, "Invalid quota entity"));

            env.kafkaClient().prepareResponse(AlterClientQuotasResponse.fromQuotaEntities(responseData, 0));

            List<ClientQuotaAlteration> entries = new ArrayList<>(3);
            entries.add(new ClientQuotaAlteration(goodEntity, singleton(new ClientQuotaAlteration.Op("consumer_byte_rate", 10000.0))));
            entries.add(new ClientQuotaAlteration(unauthorizedEntity, singleton(new ClientQuotaAlteration.Op("producer_byte_rate", 10000.0))));
            entries.add(new ClientQuotaAlteration(invalidEntity, singleton(new ClientQuotaAlteration.Op("producer_byte_rate", 100.0))));

            AlterClientQuotasResult result = env.adminClient().alterClientQuotas(entries);
            result.values().get(goodEntity);
            TestUtils.assertFutureThrows(ClusterAuthorizationException.class, result.values().get(unauthorizedEntity));
            TestUtils.assertFutureThrows(InvalidRequestException.class, result.values().get(invalidEntity));

            // ensure immutable
            assertThrows(UnsupportedOperationException.class, () -> result.values().put(newClientQuotaEntity(ClientQuotaEntity.USER, "user-3"), null));
        }
    }

    @Test
    public void testDescribeUserScramCredentials() throws Exception {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String user0Name = "user0";
            final ScramMechanism user0ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;
            final int user0Iterations0 = 4096;
            final ScramMechanism user0ScramMechanism1 = ScramMechanism.SCRAM_SHA_512;
            final int user0Iterations1 = 8192;

            final CredentialInfo user0CredentialInfo0 = new CredentialInfo();
            user0CredentialInfo0.setMechanism(user0ScramMechanism0.type());
            user0CredentialInfo0.setIterations(user0Iterations0);
            final CredentialInfo user0CredentialInfo1 = new CredentialInfo();
            user0CredentialInfo1.setMechanism(user0ScramMechanism1.type());
            user0CredentialInfo1.setIterations(user0Iterations1);

            final String user1Name = "user1";
            final ScramMechanism user1ScramMechanism = ScramMechanism.SCRAM_SHA_256;
            final int user1Iterations = 4096;

            final CredentialInfo user1CredentialInfo = new CredentialInfo();
            user1CredentialInfo.setMechanism(user1ScramMechanism.type());
            user1CredentialInfo.setIterations(user1Iterations);

            final DescribeUserScramCredentialsResponseData responseData = new DescribeUserScramCredentialsResponseData();
            responseData.setResults(asList(
                    new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                            .setUser(user0Name)
                            .setCredentialInfos(asList(user0CredentialInfo0, user0CredentialInfo1)),
                    new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult()
                            .setUser(user1Name)
                            .setCredentialInfos(singletonList(user1CredentialInfo))));
            final DescribeUserScramCredentialsResponse response = new DescribeUserScramCredentialsResponse(responseData);

            final Set<String> usersRequestedSet = new HashSet<>();
            usersRequestedSet.add(user0Name);
            usersRequestedSet.add(user1Name);

            for (final List<String> users : asList(null, new ArrayList<String>(), asList(user0Name, null, user1Name))) {
                env.kafkaClient().prepareResponse(response);

                final DescribeUserScramCredentialsResult result = env.adminClient().describeUserScramCredentials(users);
                final Map<String, UserScramCredentialsDescription> descriptionResults = result.all().get();
                final KafkaFuture<UserScramCredentialsDescription> user0DescriptionFuture = result.description(user0Name);
                final KafkaFuture<UserScramCredentialsDescription> user1DescriptionFuture = result.description(user1Name);

                final Set<String> usersDescribedFromUsersSet = new HashSet<>(result.users().get());
                assertEquals(usersRequestedSet, usersDescribedFromUsersSet);

                final Set<String> usersDescribedFromMapKeySet = descriptionResults.keySet();
                assertEquals(usersRequestedSet, usersDescribedFromMapKeySet);

                final UserScramCredentialsDescription userScramCredentialsDescription0 = descriptionResults.get(user0Name);
                assertEquals(user0Name, userScramCredentialsDescription0.name());
                assertEquals(2, userScramCredentialsDescription0.credentialInfos().size());
                assertEquals(user0ScramMechanism0, userScramCredentialsDescription0.credentialInfos().get(0).mechanism());
                assertEquals(user0Iterations0, userScramCredentialsDescription0.credentialInfos().get(0).iterations());
                assertEquals(user0ScramMechanism1, userScramCredentialsDescription0.credentialInfos().get(1).mechanism());
                assertEquals(user0Iterations1, userScramCredentialsDescription0.credentialInfos().get(1).iterations());
                assertEquals(userScramCredentialsDescription0, user0DescriptionFuture.get());

                final UserScramCredentialsDescription userScramCredentialsDescription1 = descriptionResults.get(user1Name);
                assertEquals(user1Name, userScramCredentialsDescription1.name());
                assertEquals(1, userScramCredentialsDescription1.credentialInfos().size());
                assertEquals(user1ScramMechanism, userScramCredentialsDescription1.credentialInfos().get(0).mechanism());
                assertEquals(user1Iterations, userScramCredentialsDescription1.credentialInfos().get(0).iterations());
                assertEquals(userScramCredentialsDescription1, user1DescriptionFuture.get());
            }
        }
    }

    @Test
    public void testAlterUserScramCredentialsUnknownMechanism() throws ExecutionException, InterruptedException {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String user0Name = "user0";
            ScramMechanism user0ScramMechanism0 = ScramMechanism.UNKNOWN;

            final String user1Name = "user1";
            ScramMechanism user1ScramMechanism0 = ScramMechanism.UNKNOWN;

            final String user2Name = "user2";
            ScramMechanism user2ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;

            AlterUserScramCredentialsResponseData responseData = new AlterUserScramCredentialsResponseData();
            responseData.setResults(singletonList(
                    new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult().setUser(user2Name)));

            env.kafkaClient().prepareResponse(new AlterUserScramCredentialsResponse(responseData));

            AlterUserScramCredentialsResult result = env.adminClient().alterUserScramCredentials(asList(
                    new UserScramCredentialDeletion(user0Name, user0ScramMechanism0),
                    new UserScramCredentialUpsertion(user1Name, new ScramCredentialInfo(user1ScramMechanism0, 8192), "password"),
                    new UserScramCredentialUpsertion(user2Name, new ScramCredentialInfo(user2ScramMechanism0, 4096), "password")));
            Map<String, KafkaFuture<Void>> resultData = result.values();
            assertEquals(3, resultData.size());
            Stream.of(user0Name, user1Name).forEach(u -> {
                assertTrue(resultData.containsKey(u));
                assertThrows(Exception.class, () -> resultData.get(u).get(), "Expected request for user " + u + " to complete exceptionally, but it did not");
            });
            assertTrue(resultData.containsKey(user2Name));
            resultData.get(user2Name).get();

            assertThrows(Exception.class, () -> result.all().get(), "Expected 'result.all().get()' to throw an exception since at least one user failed, but it did not");
        }
    }

    @Test
    public void testAlterUserScramCredentials() {
        try (AdminClientUnitTestEnv env = mockClientEnv()) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());

            final String user0Name = "user0";
            ScramMechanism user0ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;
            ScramMechanism user0ScramMechanism1 = ScramMechanism.SCRAM_SHA_512;
            final String user1Name = "user1";
            ScramMechanism user1ScramMechanism0 = ScramMechanism.SCRAM_SHA_256;
            final String user2Name = "user2";
            ScramMechanism user2ScramMechanism0 = ScramMechanism.SCRAM_SHA_512;
            AlterUserScramCredentialsResponseData responseData = new AlterUserScramCredentialsResponseData();
            responseData.setResults(Stream.of(user0Name, user1Name, user2Name).map(u ->
                    new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                    .setUser(u).setErrorCode(Errors.NONE.code())).collect(Collectors.toList()));

            env.kafkaClient().prepareResponse(new AlterUserScramCredentialsResponse(responseData));

            AlterUserScramCredentialsResult result = env.adminClient().alterUserScramCredentials(asList(
                    new UserScramCredentialDeletion(user0Name, user0ScramMechanism0),
                    new UserScramCredentialUpsertion(user0Name, new ScramCredentialInfo(user0ScramMechanism1, 8192), "password"),
                    new UserScramCredentialUpsertion(user1Name, new ScramCredentialInfo(user1ScramMechanism0, 8192), "password"),
                    new UserScramCredentialDeletion(user2Name, user2ScramMechanism0)));
            Map<String, KafkaFuture<Void>> resultData = result.values();
            assertEquals(3, resultData.size());
            Stream.of(user0Name, user1Name, user2Name).forEach(u -> {
                assertTrue(resultData.containsKey(u));
                assertFalse(resultData.get(u).isCompletedExceptionally());
            });
        }
    }

    private ClientQuotaEntity newClientQuotaEntity(String... args) {
        assertEquals(0, args.length % 2);

        Map<String, String> entityMap = new HashMap<>(args.length / 2);
        for (int index = 0; index < args.length; index += 2) {
            entityMap.put(args[index], args[index + 1]);
        }
        return new ClientQuotaEntity(entityMap);
    }

    @SafeVarargs
    private static <T> void assertCollectionIs(Collection<T> collection, T... elements) {
        for (T element : elements) {
            assertTrue(collection.contains(element), "Did not find " + element);
        }
        assertEquals(elements.length, collection.size(), "There are unexpected extra elements in the collection.");
    }
}
