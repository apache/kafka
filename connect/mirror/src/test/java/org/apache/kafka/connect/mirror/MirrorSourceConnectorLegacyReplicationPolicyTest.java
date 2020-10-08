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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MirrorSourceConnectorLegacyReplicationPolicyTest extends MirrorSourceConnectorTest {

    @Override
    protected ReplicationPolicy replicationPolicy() {
        return new LegacyReplicationPolicy();
    }

    @Test
    public void testCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new LegacyReplicationPolicy(), x -> true, x -> true);
        // LegacyReplicationPolicy can prevent cycles only for heartbeats topics.
        assertTrue("should allow cycles", connector.shouldReplicateTopic("target.topic1"));
        assertTrue("should allow cycles", connector.shouldReplicateTopic("target.source.topic1"));
        assertTrue("should allow cycles", connector.shouldReplicateTopic("source.target.topic1"));
        assertTrue("should allow anything else", connector.shouldReplicateTopic("topic1"));
        assertTrue("should allow anything else", connector.shouldReplicateTopic("source.topic1"));

        assertFalse("should not allow cycles for heartbeats", connector.shouldReplicateTopic("target.heartbeats"));
        assertFalse("should not allow cycles for heartbeats", connector.shouldReplicateTopic("target.source.heartbeats"));
        assertFalse("should not allow cycles for heartbeats", connector.shouldReplicateTopic("source.target.heartbeats"));
        assertTrue("should allow anything else for heartbeats", connector.shouldReplicateTopic("heartbeats"));
        assertTrue("should allow anything else for heartbeats", connector.shouldReplicateTopic("source.heartbeats"));
    }

    @Test
    public void testAclTransformation() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new LegacyReplicationPolicy(), x -> true, x -> true);
        AclBinding allowAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW));
        AclBinding processedAllowAllAclBinding = connector.targetAclBinding(allowAllAclBinding);
        String expectedRemoteTopicName = allowAllAclBinding.pattern().name();
        assertTrue("should change topic name",
            processedAllowAllAclBinding.pattern().name().equals(expectedRemoteTopicName));
        assertTrue("should change ALL to READ", processedAllowAllAclBinding.entry().operation() == AclOperation.READ);
        assertTrue("should not change ALLOW",
            processedAllowAllAclBinding.entry().permissionType() == AclPermissionType.ALLOW);

        AclBinding denyAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.DENY));
        AclBinding processedDenyAllAclBinding = connector.targetAclBinding(denyAllAclBinding);
        assertTrue("should not change ALL", processedDenyAllAclBinding.entry().operation() == AclOperation.ALL);
        assertTrue("should not change DENY",
            processedDenyAllAclBinding.entry().permissionType() == AclPermissionType.DENY);
    }

    @Test
    public void testRefreshTopicPartitions() throws Exception {
        testRefreshTopicPartitions("topic");
    }
}
