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

package org.apache.kafka.metalog;

import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalInt;

import static org.apache.kafka.metalog.MockMetaLogManagerListener.COMMIT;
import static org.apache.kafka.metalog.MockMetaLogManagerListener.LAST_COMMITTED_OFFSET;
import static org.apache.kafka.metalog.MockMetaLogManagerListener.SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


@Timeout(value = 40)
public class LocalLogManagerTest {

    /**
     * Test creating a LocalLogManager and closing it.
     */
    @Test
    public void testCreateAndClose() throws Exception {
        try (
            LocalLogManagerTestEnv env = new LocalLogManagerTestEnv.Builder(1).
                buildWithMockListeners()
        ) {
            env.close();
            assertNull(env.firstError.get());
        }
    }

    /**
     * Test that the local log manager will claim leadership.
     */
    @Test
    public void testClaimsLeadership() throws Exception {
        try (
            LocalLogManagerTestEnv env = new LocalLogManagerTestEnv.Builder(1).
                    buildWithMockListeners()
        ) {
            assertEquals(new LeaderAndEpoch(OptionalInt.of(0), 1), env.waitForLeader());
            env.close();
            assertNull(env.firstError.get());
        }
    }

    /**
     * Test that we can pass leadership back and forth between log managers.
     */
    @Test
    public void testPassLeadership() throws Exception {
        try (
            LocalLogManagerTestEnv env = new LocalLogManagerTestEnv.Builder(3).
                    buildWithMockListeners()
        ) {
            LeaderAndEpoch first = env.waitForLeader();
            LeaderAndEpoch cur = first;
            do {
                int currentLeaderId = cur.leaderId().orElseThrow(() ->
                    new AssertionError("Current leader is undefined")
                );
                env.logManagers().get(currentLeaderId).resign(cur.epoch());

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
                LocalLogManager logManager) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(20000, 3, () -> {
            MockMetaLogManagerListener listener =
                (MockMetaLogManagerListener) logManager.listeners().get(0);
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
                throw new RuntimeException("Offset for log manager " +
                    logManager.nodeId() + " only reached " + highestOffset);
            }
        });
    }

    /**
     * Test that all the log managers see all the commits.
     */
    @Test
    public void testCommits() throws Exception {
        try (
            LocalLogManagerTestEnv env = new LocalLogManagerTestEnv.Builder(3).
                    buildWithMockListeners()
        ) {
            LeaderAndEpoch leaderInfo = env.waitForLeader();
            int leaderId = leaderInfo.leaderId().orElseThrow(() ->
                new AssertionError("Current leader is undefined")
            );

            LocalLogManager activeLogManager = env.logManagers().get(leaderId);
            int epoch = activeLogManager.leaderAndEpoch().epoch();
            List<ApiMessageAndVersion> messages = Arrays.asList(
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(0), (short) 0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(1), (short) 0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(2), (short) 0));
            assertEquals(3, activeLogManager.prepareAppend(epoch, messages));
            activeLogManager.schedulePreparedAppend();
            for (LocalLogManager logManager : env.logManagers()) {
                waitForLastCommittedOffset(3, logManager);
            }
            List<MockMetaLogManagerListener> listeners = env.logManagers().stream().
                map(m -> (MockMetaLogManagerListener) m.listeners().get(0)).
                toList();
            env.close();
            for (MockMetaLogManagerListener listener : listeners) {
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
