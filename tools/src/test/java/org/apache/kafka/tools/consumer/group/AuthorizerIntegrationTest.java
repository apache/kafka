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
package org.apache.kafka.tools.consumer.group;

import kafka.api.AbstractAuthorizerIntegrationTest;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.errors.GroupIdNotFoundException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import scala.jdk.javaapi.CollectionConverters;

import static org.apache.kafka.common.acl.AclOperation.DESCRIBE;
import static org.apache.kafka.common.acl.AclPermissionType.ALLOW;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

public class AuthorizerIntegrationTest extends AbstractAuthorizerIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {"kraft"})
    public void testDescribeGroupCliWithGroupDescribe(String quorum) throws Exception {
        addAndVerifyAcls(CollectionConverters.asScala(Collections.singleton(new AccessControlEntry(ClientPrincipal().toString(), "*", DESCRIBE, ALLOW))).toSet(), groupResource());

        String[] cgcArgs = new String[]{"--bootstrap-server", bootstrapServers(listenerName()), "--describe", "--group", group()};
        ConsumerGroupCommandOptions opts = ConsumerGroupCommandOptions.fromArgs(cgcArgs);
        try (ConsumerGroupCommand.ConsumerGroupService consumerGroupService = new ConsumerGroupCommand.ConsumerGroupService(opts, Collections.emptyMap())) {
            consumerGroupService.describeGroups();
            fail("Non-existent group should throw an exception");
        } catch (ExecutionException e) {
            assertInstanceOf(GroupIdNotFoundException.class, e.getCause(),
                "Non-existent group should throw GroupIdNotFoundException");
        }
    }
}
