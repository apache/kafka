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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MirrorHeartbeatTaskTest {

    @Test
    public void testPollCreatesRecords() throws InterruptedException {
        MirrorHeartbeatTask heartbeatTask = new MirrorHeartbeatTask();
        heartbeatTask.start(TestUtils.makeProps("source.cluster.alias", "testSource",
                "target.cluster.alias", "testTarget"));
        List<SourceRecord> records = heartbeatTask.poll();
        assertEquals(1, records.size());
        Map<String, ?> sourcePartition = records.iterator().next().sourcePartition();
        assertEquals(sourcePartition.get(Heartbeat.SOURCE_CLUSTER_ALIAS_KEY), "testSource",
                "sourcePartition's " + Heartbeat.SOURCE_CLUSTER_ALIAS_KEY + " record was not created");
        assertEquals(sourcePartition.get(Heartbeat.TARGET_CLUSTER_ALIAS_KEY), "testTarget",
                "sourcePartition's " + Heartbeat.TARGET_CLUSTER_ALIAS_KEY + " record was not created");
    }
}
