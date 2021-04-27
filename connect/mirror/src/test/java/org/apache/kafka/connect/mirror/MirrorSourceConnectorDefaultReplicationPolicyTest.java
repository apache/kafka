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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorSourceConnectorDefaultReplicationPolicyTest extends MirrorSourceConnectorTest {
    @Override
    protected ReplicationPolicy replicationPolicy() {
        return new DefaultReplicationPolicy();
    }

    @Test
    public void testNoCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            replicationPolicy(), x -> true, x -> true);
        assertFalse(connector.shouldReplicateTopic("target.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("target.source.topic1"), "should not allow cycles");
        assertFalse(connector.shouldReplicateTopic("source.target.topic1"), "should not allow cycles");
        assertTrue(connector.shouldReplicateTopic("topic1"), "should allow anything else");
        assertTrue(connector.shouldReplicateTopic("source.topic1"), "should allow anything else");
    }

    @Test
    public void testAclTransformation() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            replicationPolicy(), x -> true, x -> true);
        AclBinding allowAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW));
        AclBinding processedAllowAllAclBinding = connector.targetAclBinding(allowAllAclBinding);
        String expectedRemoteTopicName = "source" + DefaultReplicationPolicy.SEPARATOR_DEFAULT
            + allowAllAclBinding.pattern().name();
        assertEquals(expectedRemoteTopicName, processedAllowAllAclBinding.pattern().name(), "should change topic name");
        assertEquals(processedAllowAllAclBinding.entry().operation(), AclOperation.READ, "should change ALL to READ");
        assertEquals(processedAllowAllAclBinding.entry().permissionType(), AclPermissionType.ALLOW, "should not change ALLOW");

        AclBinding denyAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.DENY));
        AclBinding processedDenyAllAclBinding = connector.targetAclBinding(denyAllAclBinding);
        assertEquals(processedDenyAllAclBinding.entry().operation(), AclOperation.ALL, "should not change ALL");
        assertEquals(processedDenyAllAclBinding.entry().permissionType(), AclPermissionType.DENY, "should not change DENY");
    }

    @Test
    public void testRefreshTopicPartitions() throws Exception {
        testRefreshTopicPartitions("source.topic");
    }
}
