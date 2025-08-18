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

package org.apache.kafka.controller;

import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.OptionalInt;

import static org.apache.kafka.controller.MockRaftClientListener.COMMIT;
import static org.apache.kafka.controller.MockRaftClientListener.LAST_COMMITTED_OFFSET;
import static org.apache.kafka.controller.MockRaftClientListener.SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


@Timeout(value = 40)
public class MockRaftClientTest {

    /**
     * Test creating a MockRaftClient and closing it.
     */
    @Test
    public void testCreateAndClose() throws Exception {
        try (
            MockRaftClientTestEnv env = new MockRaftClientTestEnv.Builder(1).
                buildWithMockListeners()
        ) {
            env.close();
            assertNull(env.firstError.get());
        }
    }

    /**
     * Test that the raft client will claim leadership.
     */
    @Test
    public void testClaimsLeadership() throws Exception {
        try (
            MockRaftClientTestEnv env = new MockRaftClientTestEnv.Builder(1).
                    buildWithMockListeners()
        ) {
            assertEquals(new LeaderAndEpoch(OptionalInt.of(0), 1), env.waitForLeader());
            env.close();
            assertNull(env.firstError.get());
        }
    }

    /**
     * Test that we can pass leadership back and forth between raft clients.
     */
    @Test
    public void testPassLeadership() throws Exception {
        try (
            MockRaftClientTestEnv env = new MockRaftClientTestEnv.Builder(3).
                    buildWithMockListeners()
        ) {
            LeaderAndEpoch first = env.waitForLeader();
            LeaderAndEpoch cur = first;
            do {
                int currentLeaderId = cur.leaderId().orElseThrow(() ->
                    new AssertionError("Current leader is undefined")
                );
                env.raftClients().get(currentLeaderId).resign(cur.epoch());

                LeaderAndEpoch next = env.waitForLeader();
                while (next.epoch() == cur.epoch()) {
                    Thread.sleep(1);
                    next = env.waitForLeader();
                }
                long expectedNextEpoch = cur.epoch() + 2;
                assertEquals(expectedNextEpoch, next.epoch(), "Expected next epoch to be " + expectedNextEpoch +
                    ", but found  " + next);
                cur = next;
            } while (cur.leaderId().equals(first.leaderId()));
            env.close();
            assertNull(env.firstError.get());
        }
    }

    private static void waitForLastCommittedOffset(long targetOffset,
                MockRaftClient raftClient) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(20000, 3, () -> {
            MockRaftClientListener listener = (MockRaftClientListener) raftClient.listeners().get(0);
            long highestOffset = -1;
            for (String event : listener.serializedEvents()) {
                if (event.startsWith(LAST_COMMITTED_OFFSET)) {
                    long offset = Long.parseLong(
                        event.substring(LAST_COMMITTED_OFFSET.length() + 1));
                    if (offset < highestOffset) {
                        throw new RuntimeException("Invalid offset: " + offset +
                            " is less than the previous offset of " + highestOffset);
                    }
                    highestOffset = offset;
                }
            }
            if (highestOffset < targetOffset) {
                throw new RuntimeException("Offset for raft client " +
                    raftClient.nodeId() + " only reached " + highestOffset);
            }
        });
    }

    /**
     * Test that all the raft clients see all the commits.
     */
    @Test
    public void testCommits() throws Exception {
        try (
            MockRaftClientTestEnv env = new MockRaftClientTestEnv.Builder(3).
                    buildWithMockListeners()
        ) {
            LeaderAndEpoch leaderInfo = env.waitForLeader();
            int leaderId = leaderInfo.leaderId().orElseThrow(() ->
                new AssertionError("Current leader is undefined")
            );

            MockRaftClient activeRaftClient = env.raftClients().get(leaderId);
            int epoch = activeRaftClient.leaderAndEpoch().epoch();
            List<ApiMessageAndVersion> messages = List.of(
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(0), (short) 0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(1), (short) 0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(2), (short) 0));
            assertEquals(3, activeRaftClient.prepareAppend(epoch, messages));

            activeRaftClient.schedulePreparedAppend();
            for (MockRaftClient raftClient : env.raftClients()) {
                waitForLastCommittedOffset(3, raftClient);
            }

            List<MockRaftClientListener> listeners = env.raftClients().stream().
                map(m -> (MockRaftClientListener) m.listeners().get(0)).
                toList();
            env.close();
            for (MockRaftClientListener listener : listeners) {
                List<String> events = listener.serializedEvents();
                assertEquals(SHUTDOWN, events.get(events.size() - 1));
                int foundIndex = 0;
                for (String event : events) {
                    if (event.startsWith(COMMIT)) {
                        assertEquals(messages.get(foundIndex).message().toString(),
                            event.substring(COMMIT.length() + 1));
                        foundIndex++;
                    }
                }
                assertEquals(messages.size(), foundIndex);
            }
        }
    }
}
