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
package org.apache.kafka.tools.consumergroup;

import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.tools.consumergroup.ConsumerGroupCommand.ConsumerGroupService;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.collection.JavaConverters;

import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.apache.kafka.tools.ToolsTestUtils.TEST_WITH_PARAMETERIZED_QUORUM_NAME;

public class AuthorizerIntegrationTest extends kafka.api.AuthorizerIntegrationTest {
    @ParameterizedTest(name = TEST_WITH_PARAMETERIZED_QUORUM_NAME)
    @ValueSource(strings = {"zk", "kraft"})
    public void testDescribeGroupCliWithGroupDescribe(String quorum) throws Exception {
        createTopicWithBrokerPrincipal(topic());
        addAndVerifyAcls(set(new AccessControlEntry(ClientPrincipal().toString(), AclEntry.WildcardHost(), DESCRIBE, ALLOW)), groupResource());
        addAndVerifyAcls(set(new AccessControlEntry(ClientPrincipal().toString(), AclEntry.WildcardHost(), DESCRIBE, ALLOW)), topicResource());

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group()};
        ConsumerGroupCommandOptions opts = new ConsumerGroupCommandOptions(cgcArgs);
        ConsumerGroupService consumerGroupService = new ConsumerGroupService(opts, Collections.emptyMap());
        consumerGroupService.describeGroups();
        consumerGroupService.close();
    }

    private void createTopicWithBrokerPrincipal(String topic) {
        // Note the principal builder implementation maps all connections on the
        // inter-broker listener to the broker principal.
        createTopic(
            topic,
            1,
            1,
            new Properties(),
            interBrokerListenerName(),
            new Properties()
        );
    }

    @SuppressWarnings({"deprecation", "unchecked"})
    private static <T> scala.collection.immutable.Set<T> set(final T...set) {
        return JavaConverters.asScalaSet(new HashSet<>(asList(set))).toSet();
    }
}
