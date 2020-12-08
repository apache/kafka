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
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.metalog.LocalLogManager.LockRegistry;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.kafka.metalog.MockMetaLogManagerListener.COMMIT;
import static org.apache.kafka.metalog.MockMetaLogManagerListener.LAST_COMMITTED_OFFSET;
import static org.apache.kafka.metalog.MockMetaLogManagerListener.SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 40)
public class LocalLogManagerTest {
    private static final Logger log = LoggerFactory.getLogger(LocalLogManagerTest.class);

    /**
     * Test that when a bunch of threads race to take a lock registry lock, only one
     * thread at a time can get it.
     */
    @Test
    public void testLockRegistryWithRacingThreads() throws Exception {
        List<Thread> threads = new ArrayList<>();
        try (LocalLogManagerTestEnv env = LocalLogManagerTestEnv.createWithMockListeners(0)) {
            final Path leaderPath = env.dir().toPath().resolve("leader").toAbsolutePath();
            final LockRegistry registry = new LockRegistry();
            final AtomicInteger threadsWithLock = new AtomicInteger(0);
            try (FileChannel leaderChannel = FileChannel.open(leaderPath,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                for (int i = 0; i < 20; i++) {
                    Thread thread = new Thread(() -> {
                        try {
                            registry.lock(leaderPath.toString(), leaderChannel, () -> {
                                if (threadsWithLock.getAndIncrement() != 0) {
                                    env.firstError().compareAndSet(null, "threadsWithLock " +
                                        "was non-zero when we got the lock.");
                                    return;
                                }
                                try {
                                    Thread.sleep(0, 1000);
                                } catch (InterruptedException e) {
                                }
                                threadsWithLock.decrementAndGet();
                            });
                        } catch (Throwable t) {
                            env.firstError().compareAndSet(null, "unexpected " +
                                t.getClass().getName() + " :" + t.getMessage());
                        }
                    });
                    threads.add(thread);
                    thread.start();
                }
                joinAll(threads);
            }
            assertEquals(null, env.firstError.get());
        } finally {
            joinAll(threads);
        }
    }

    private static void joinAll(List<Thread> threads) throws Exception {
        for (Thread thread : threads) {
            thread.join();
        }
    }

    @Test
    public void testRoundTripLeaderInfo() {
        MetaLogLeader leader = new MetaLogLeader(123, 123456789L);
        ByteBuffer buffer = ByteBuffer.allocate(LocalLogManager.leaderSize(leader));
        buffer.clear();
        LocalLogManager.writeLeader(buffer, leader);
        buffer.flip();
        MetaLogLeader leader2 = LocalLogManager.readLeader(buffer);
        assertEquals(leader, leader2);
    }

    /**
     * Test creating a LocalLogManager and closing it.
     */
    @Test
    public void testCreateAndClose() throws Exception {
        try (LocalLogManagerTestEnv env =
                 LocalLogManagerTestEnv.createWithMockListeners(1)) {
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }

    /**
     * Test that the local log maanger will claim leadership.
     */
    @Test
    public void testClaimsLeadership() throws Exception {
        try (LocalLogManagerTestEnv env =
                 LocalLogManagerTestEnv.createWithMockListeners(1)) {
            assertEquals(new MetaLogLeader(0, 0), env.waitForLeader());
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }

    /**
     * Test that we can pass leadership back and forth between log managers.
     */
    @Test
    public void testPassLeadership() throws Exception {
        try (LocalLogManagerTestEnv env =
                 LocalLogManagerTestEnv.createWithMockListeners(3)) {
            MetaLogLeader first = env.waitForLeader();
            MetaLogLeader cur = first;
            do {
                env.logManagers().get(cur.nodeId()).renounce(cur.epoch());
                MetaLogLeader next = env.waitForLeader();
                while (next.epoch() == cur.epoch()) {
                    Thread.sleep(1);
                    next = env.waitForLeader();
                }
                long expectedNextEpoch = cur.epoch() + 1;
                assertEquals(expectedNextEpoch, next.epoch(), "Expected next epoch to be " + expectedNextEpoch +
                    ", but found  " + next);
                cur = next;
            } while (cur.nodeId() == first.nodeId());
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }

    private static void waitForLastCommittedOffset(long targetOffset,
                LocalLogManager logManager) throws InterruptedException {
        TestUtils.retryOnExceptionWithTimeout(3, 20000, () -> {
            MockMetaLogManagerListener listener =
                (MockMetaLogManagerListener) logManager.listeners().get(0);
            long highestOffset = -1;
            for (String event : listener.serializedEvents()) {
                if (event.startsWith(LAST_COMMITTED_OFFSET)) {
                    long offset = Long.valueOf(
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
        try (LocalLogManagerTestEnv env =
                 LocalLogManagerTestEnv.createWithMockListeners(3)) {
            MetaLogLeader leaderInfo = env.waitForLeader();
            LocalLogManager activeLogManager = env.logManagers().get(leaderInfo.nodeId());
            long epoch = activeLogManager.leader().epoch();
            List<ApiMessageAndVersion> messages = Arrays.asList(
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(0), (short) 0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(1), (short) 0),
                new ApiMessageAndVersion(new RegisterBrokerRecord().setBrokerId(2), (short) 0));
            activeLogManager.scheduleWrite(epoch, messages);
            for (LocalLogManager logManager : env.logManagers()) {
                waitForLastCommittedOffset(2, logManager);
            }
            List<MockMetaLogManagerListener> listeners = env.logManagers().stream().
                map(m -> (MockMetaLogManagerListener) m.listeners().get(0)).
                collect(Collectors.toList());
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
