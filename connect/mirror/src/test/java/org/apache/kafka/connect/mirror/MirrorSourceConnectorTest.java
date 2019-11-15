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
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;

public class MirrorSourceConnectorTest {

    @Test
    public void testReplicatesHeartbeatsByDefault() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"), 
            new DefaultReplicationPolicy(), new DefaultTopicFilter(), new DefaultConfigPropertyFilter());
        assertTrue("should replicate heartbeats", connector.shouldReplicateTopic("heartbeats"));
        assertTrue("should replicate upstream heartbeats", connector.shouldReplicateTopic("us-west.heartbeats"));
    }

    @Test
    public void testReplicatesHeartbeatsDespiteFilter() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> false, new DefaultConfigPropertyFilter());
        assertTrue("should replicate heartbeats", connector.shouldReplicateTopic("heartbeats"));
        assertTrue("should replicate upstream heartbeats", connector.shouldReplicateTopic("us-west.heartbeats"));
    }

    @Test
    public void testNoCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, x -> true);
        assertFalse("should not allow cycles", connector.shouldReplicateTopic("target.topic1"));
        assertFalse("should not allow cycles", connector.shouldReplicateTopic("target.source.topic1"));
        assertFalse("should not allow cycles", connector.shouldReplicateTopic("source.target.topic1"));
        assertTrue("should allow anything else", connector.shouldReplicateTopic("topic1"));
        assertTrue("should allow anything else", connector.shouldReplicateTopic("source.topic1"));
    }

    @Test
    public void testAclFiltering() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, x -> true);
        assertFalse("should not replicate ALLOW WRITE", connector.shouldReplicateAcl(
            new AclBinding(new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.WRITE, AclPermissionType.ALLOW))));
        assertTrue("should replicate ALLOW ALL", connector.shouldReplicateAcl(
            new AclBinding(new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW))));
    }

    @Test
    public void testAclTransformation() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, x -> true);
        AclBinding allowAllAclBinding = new AclBinding(
            new ResourcePattern(ResourceType.TOPIC, "test_topic", PatternType.LITERAL),
            new AccessControlEntry("kafka", "", AclOperation.ALL, AclPermissionType.ALLOW));
        AclBinding processedAllowAllAclBinding = connector.targetAclBinding(allowAllAclBinding);
        String expectedRemoteTopicName = "source" + DefaultReplicationPolicy.SEPARATOR_DEFAULT
            + allowAllAclBinding.pattern().name();
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
    public void testConfigPropertyFiltering() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true, new DefaultConfigPropertyFilter());
        ArrayList<ConfigEntry> entries = new ArrayList<>();
        entries.add(new ConfigEntry("name-1", "value-1"));
        entries.add(new ConfigEntry("min.insync.replicas", "2"));
        Config config = new Config(entries);
        Config targetConfig = connector.targetConfig(config);
        assertTrue("should replicate properties", targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("name-1")));
        assertFalse("should not replicate blacklisted properties", targetConfig.entries().stream()
            .anyMatch(x -> x.name().equals("min.insync.replicas")));
    }
}
