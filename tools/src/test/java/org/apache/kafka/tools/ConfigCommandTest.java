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


package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.AdminClientUnitTestEnv;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.common.requests.DescribeConfigsResponse;
import org.apache.kafka.common.requests.Resource;
import org.apache.kafka.common.requests.ResourceType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMockBuilder;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;


public class ConfigCommandTest {

    private static AdminClientUnitTestEnv mockClientEnv(String... configVals) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        nodes.put(0, new Node(0, "localhost", 8121));
        nodes.put(1, new Node(1, "localhost", 8122));
        nodes.put(2, new Node(2, "localhost", 8123));
        Cluster cluster = new Cluster("mockClusterId", nodes.values(),
                Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));
        return new AdminClientUnitTestEnv(cluster, configVals);
    }

    @Test
    public void testDescribeBrokerConfigs() throws Exception {
        Map<Resource, DescribeConfigsResponse.Config> configs = new HashMap<>();
        configs.put(
                new Resource(ResourceType.BROKER, "0"),
                new DescribeConfigsResponse.Config(
                        ApiError.NONE,
                        Collections.singletonList(
                                new DescribeConfigsResponse.ConfigEntry("ssl.truststore.type", "JKS", false, true, true))));
        DescribeConfigsResponse response = new DescribeConfigsResponse(1000, configs);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {

            ConfigCommand cmd = createMockBuilder(ConfigCommand.class).addMockedMethod("createAdminClient", Properties.class).createMock();
            expect(cmd.createAdminClient(anyObject(Properties.class))).andReturn(env.adminClient());
            replay(cmd);

            env.kafkaClient().setNode(env.cluster().controller());
            env.kafkaClient().prepareResponse(response);
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());

            String hostPort = String.format("%s:%s", env.cluster().controller().host(), env.cluster().controller().port());
            cmd.doRun(Arrays.asList("--bootstrap-servers", hostPort, "--entity-type", "brokers", "--entity-name", Integer.toString(env.cluster().controller().id()), "--describe").toArray(new String[]{}));
            verify(cmd);
        }
    }

    @Test
    public void testDescribeTopicConfigs() throws Exception {
        Map<Resource, DescribeConfigsResponse.Config> configs = new HashMap<>();
        configs.put(
                new Resource(ResourceType.TOPIC, "topicA"),
                new DescribeConfigsResponse.Config(
                        ApiError.NONE,
                        Collections.singletonList(
                                new DescribeConfigsResponse.ConfigEntry("compression.type", "snappy", false, true, true))));
        DescribeConfigsResponse response = new DescribeConfigsResponse(1000, configs);

        try (AdminClientUnitTestEnv env = mockClientEnv()) {

            ConfigCommand cmd = createMockBuilder(ConfigCommand.class).addMockedMethod("createAdminClient", Properties.class).createMock();
            expect(cmd.createAdminClient(anyObject(Properties.class))).andReturn(env.adminClient());
            replay(cmd);

            env.kafkaClient().setNode(env.cluster().controller());
            env.kafkaClient().prepareResponse(response);
            env.kafkaClient().prepareMetadataUpdate(env.cluster(), Collections.<String>emptySet());

            String hostPort = String.format("%s:%s", env.cluster().controller().host(), env.cluster().controller().port());
            cmd.doRun(Arrays.asList("--bootstrap-servers", hostPort, "--entity-type", "topics", "--entity-name", "topicA", "--describe").toArray(new String[]{}));
            verify(cmd);
        }
    }
}
