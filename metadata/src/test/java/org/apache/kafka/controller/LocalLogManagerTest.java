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

import org.apache.kafka.common.metadata.BrokerRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ApiMessageAndVersion;
import org.apache.kafka.controller.LocalLogManager.LeaderInfo;
import org.apache.kafka.controller.LocalLogManager.LockRegistry;
import org.apache.kafka.controller.MockMetaLogManagerListener.CommitHandler;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class LocalLogManagerTest {
    private static final Logger log = LoggerFactory.getLogger(LocalLogManagerTest.class);

    @Rule
    final public Timeout globalTimeout = Timeout.seconds(40);

    /**
     * Test that when a bunch of threads race to take a lock registry lock, only one
     * thread at a time can get it.
     */
    @Test
    public void testLockRegistryWithRacingThreads() throws Exception {
        List<Thread> threads = new ArrayList<>();
        try (LocalLogManagerTestEnv env = new LocalLogManagerTestEnv(0)) {
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
        LeaderInfo info = new LeaderInfo(123, 123456789L);
        ByteBuffer buffer = ByteBuffer.allocate(info.size());
        buffer.clear();
        info.write(buffer);
        buffer.flip();
        LeaderInfo info2 = LeaderInfo.read(buffer);
        assertEquals(info, info2);
    }

    /**
     * Test creating a LocalLogManager and closing it.
     */
    @Test
    public void testCreateAndClose() throws Exception {
        try (LocalLogManagerTestEnv env = new LocalLogManagerTestEnv(1)) {
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }

    /**
     * Test that the local log maanger will claim leadership.
     */
    @Test
    public void testClaimsLeadership() throws Exception {
        try (LocalLogManagerTestEnv env = new LocalLogManagerTestEnv(1)) {
            assertEquals(new LeaderInfo(0, 0), env.waitForLeader());
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }

    /**
     * Test that we can pass leadership back and forth between log managers.
     */
    @Test
    public void testPassLeadership() throws Exception {
        try (LocalLogManagerTestEnv env = new LocalLogManagerTestEnv(3)) {
            LeaderInfo first = env.waitForLeader();
            LeaderInfo cur = first;
            do {
                env.logManagers().get(cur.nodeId()).renounce(cur.epoch());
                LeaderInfo next = env.waitForLeader();
                while (next.epoch() == cur.epoch()) {
                    Thread.sleep(1);
                    next = env.waitForLeader();
                }
                long expectedNextEpoch = cur.epoch() + 1;
                assertEquals("Expected next epoch to be " + expectedNextEpoch +
                    ", but found  " + next, expectedNextEpoch, next.epoch());
                cur = next;
            } while (cur.nodeId() == first.nodeId());
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }

    static class TestCommit {
        final long epoch;
        final long index;
        final ApiMessage message;

        public TestCommit(long epoch, long index, ApiMessage message) {
            this.epoch = epoch;
            this.index = index;
            this.message = message;
        }

        @Override
        public int hashCode() {
            return Objects.hash(epoch, index, message);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TestCommit)) {
                return false;
            }
            TestCommit other = (TestCommit) o;
            return epoch == other.epoch &&
                index == other.index &&
                message.equals(other.message);
        }

        @Override
        public String toString() {
            return "TestCommit(epoch=" + epoch + ", index=" + index +
                ", message=" + message + ")";
        }
    }

    static class CommitChecker implements CommitHandler {
        private final List<TestCommit> commits;
        private final Map<Integer, Integer> cursors = new HashMap<>();

        CommitChecker(List<TestCommit> commits) {
            this.commits = commits;
        }

        @Override
        public synchronized void handle(int nodeId, long epoch, long index,
                                        ApiMessage message) throws Exception {
            int commitNumber = cursors.getOrDefault(nodeId, 0);
            TestCommit newCommit = new TestCommit(epoch, index, message);
            TestCommit expectedCommit = commits.get(commitNumber);
            assertEquals("Unexpected commit on node " + nodeId, expectedCommit, newCommit);
            cursors.put(nodeId, commitNumber + 1);
        }

        synchronized void verifyCursorsHaveReachedCommitNumber(int expectedNumCursors,
                                                               int expectedCommitNumber) {
            if (cursors.size() != expectedNumCursors) {
                throw new RuntimeException("Expected " + expectedNumCursors +
                    " cursors, but there were only " + cursors.size());
            }
            for (Map.Entry<Integer, Integer> entry : cursors.entrySet()) {
                if (entry.getValue() < expectedCommitNumber) {
                    throw new RuntimeException("Node " + entry.getKey() + " has " +
                        "only reached commit number " + entry.getValue() + ", not " +
                        expectedCommitNumber);
                }
            }
        }
    }

    /**
     * Test that all the log managers see all the commits.
     */
    @Test
    public void testCommits() throws Exception {
        CommitChecker checker = new CommitChecker(Arrays.asList(
            new TestCommit(0, 0, new BrokerRecord().setBrokerId(0)),
            new TestCommit(0, 1, new BrokerRecord().setBrokerId(1)),
            new TestCommit(0, 2, new BrokerRecord().setBrokerId(2)),
            new TestCommit(0, 3, new BrokerRecord().setBrokerId(0))
        ));
        final int numManagers = 3;
        try (LocalLogManagerTestEnv env =
                new LocalLogManagerTestEnv(checker, numManagers)) {
            LeaderInfo leaderInfo = env.waitForLeader();
            for (int i = 0; i < checker.commits.size(); i++) {
                TestCommit testCommit = checker.commits.get(i);
                long index = env.logManagers().get(leaderInfo.nodeId()).
                    scheduleWrite(testCommit.epoch,
                        new ApiMessageAndVersion(testCommit.message, (short) 0));
                log.trace("scheduleWrite(epoch=" + testCommit.epoch +
                    ", nodeId=" + leaderInfo.nodeId() + ") = " + index);
            }
            TestUtils.retryOnExceptionWithTimeout(3, 20000, () -> {
                checker.verifyCursorsHaveReachedCommitNumber(numManagers,
                    checker.commits.size());
            });
            env.close();
            assertEquals(null, env.firstError.get());
        }
    }
}
