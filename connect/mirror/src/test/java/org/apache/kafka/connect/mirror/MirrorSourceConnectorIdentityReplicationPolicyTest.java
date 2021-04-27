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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorSourceConnectorIdentityReplicationPolicyTest extends MirrorSourceConnectorTest {

    @Override
    protected ReplicationPolicy replicationPolicy() {
        return new IdentityReplicationPolicy();
    }

    @Test
    public void testCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                replicationPolicy(), x -> true, x -> true);
        // LegacyReplicationPolicy can prevent cycles only for heartbeats topics.
        assertTrue(connector.shouldReplicateTopic("target.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("target.source.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("source.target.topic1"), "should allow cycles");
        assertTrue(connector.shouldReplicateTopic("topic1"), "should allow anything else");
        assertTrue(connector.shouldReplicateTopic("source.topic1"), "should allow anything else");

        assertFalse(connector.shouldReplicateTopic("target.heartbeats"), "should not allow cycles for heartbeats");
        assertFalse(connector.shouldReplicateTopic("target.source.heartbeats"), "should not allow cycles for heartbeats");
        assertFalse(connector.shouldReplicateTopic("source.target.heartbeats"), "should not allow cycles for heartbeats");
        assertTrue(connector.shouldReplicateTopic("heartbeats"), "should allow anything else for heartbeats");
        assertTrue(connector.shouldReplicateTopic("source.heartbeats"), "should allow anything else for heartbeats");
    }

    @Test
    public void testAclTransformation() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
                replicationPolicy(), x -> true, x -> true);
        AclBinding allowAllAclBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
                new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW));
        AclBinding processedAllowAllAclBinding = connector.targetAclBinding(allowAllAclBinding);
        String expectedRemoteTopicName = allowAllAclBinding.pattern().name();
        assertTrue(processedAllowAllAclBinding.pattern().name().equals(expectedRemoteTopicName),
                "should change topic name");
        assertTrue(processedAllowAllAclBinding.entry().operation() == AclOperation.READ,
                "should change ALL to READ");
        assertTrue(processedAllowAllAclBinding.entry().permissionType() == AclPermissionType.ALLOW,
                "should not change ALLOW");

        AclBinding denyAllAclBinding = new AclBinding(
                new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
                new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.DENY));
        AclBinding processedDenyAllAclBinding = connector.targetAclBinding(denyAllAclBinding);
        assertTrue(processedDenyAllAclBinding.entry().operation() == AclOperation.ALL,
                "should not change ALL");
        assertTrue(processedDenyAllAclBinding.entry().permissionType() == AclPermissionType.DENY,
                "should not change DENY");
    }

    @Test
    public void testRefreshTopicPartitions() throws Exception {
        testRefreshTopicPartitions("topic");
    }
}
