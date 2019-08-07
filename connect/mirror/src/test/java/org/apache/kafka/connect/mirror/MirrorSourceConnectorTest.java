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

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class MirrorSourceConnectorTest {

    @Test
    public void testReplicatesHeartbeatsByDefault() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"), 
            new DefaultReplicationPolicy(), new DefaultTopicFilter());
        assertTrue("should replicate heartbeats", connector.shouldReplicateTopic("heartbeats"));
        assertTrue("should replicate upstream heartbeats", connector.shouldReplicateTopic("us-west.heartbeats"));
    }

    @Test
    public void testReplicatesHeartbeatsDespiteFilter() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> false);
        assertTrue("should replicate heartbeats", connector.shouldReplicateTopic("heartbeats"));
        assertTrue("should replicate upstream heartbeats", connector.shouldReplicateTopic("us-west.heartbeats"));
    }

    @Test
    public void testNoCycles() {
        MirrorSourceConnector connector = new MirrorSourceConnector(new SourceAndTarget("source", "target"),
            new DefaultReplicationPolicy(), x -> true);
        assertFalse("should not allow cycles", connector.shouldReplicateTopic("target.topic1"));
        assertFalse("should not allow cycles", connector.shouldReplicateTopic("target.source.topic1"));
        assertFalse("should not allow cycles", connector.shouldReplicateTopic("source.target.topic1"));
        assertTrue("should allow anything else", connector.shouldReplicateTopic("topic1"));
        assertTrue("should allow anything else", connector.shouldReplicateTopic("source.topic1"));
    }

}
