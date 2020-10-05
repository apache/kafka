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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.controller.LocalLogManager.LeaderInfo;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class LocalLogManagerTestEnv implements AutoCloseable {
    private static final Logger log =
        LoggerFactory.getLogger(LocalLogManagerTestEnv.class);

    /**
     * The first error we encountered during this test, or the empty string if we have
     * not encountered any.
     */
    final AtomicReference<String> firstError = new AtomicReference<>(null);

    /**
     * The test directory, which we will delete once the test is over.
     */
    private final File dir;

    /**
     * A list of log managers.
     */
    private final List<LocalLogManager> logManagers;

    public static LocalLogManagerTestEnv createWithMockListeners(int numManagers) throws Exception {
        return new LocalLogManagerTestEnv(numManagers,
            __ -> new MockMetaLogManagerListener());
    }

    private LocalLogManagerTestEnv(int numManagers,
            Function<Integer, LocalLogManager.Listener> listenerProvider) throws Exception {
        dir = TestUtils.tempDirectory();
        List<LocalLogManager> newLogManagers = new ArrayList<>(numManagers);
        try {
            for (int nodeId = 0; nodeId < numManagers; nodeId++) {
                String prefix = String.format("Manager%d", nodeId);
                newLogManagers.add(new LocalLogManager(
                    new LogContext(prefix + ": "),
                    nodeId,
                    dir.getAbsolutePath(),
                    prefix,
                    listenerProvider.apply(nodeId),
                    50));
            }
        } catch (Throwable t) {
            for (LocalLogManager logManager : newLogManagers) {
                logManager.close();
            }
            throw t;
        }
        this.logManagers = newLogManagers;
    }

    AtomicReference<String> firstError() {
        return firstError;
    }

    File dir() {
        return dir;
    }

    LeaderInfo waitForLeader() throws InterruptedException {
        AtomicReference<LeaderInfo> value = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(3, 20000, () -> {
            LeaderInfo leaderInfo = null;
            for (LocalLogManager logManager : logManagers) {
                long curEpoch = logManager.listener().currentClaim();
                if (curEpoch != -1) {
                    if (leaderInfo != null) {
                        throw new RuntimeException("node " + leaderInfo.nodeId() +
                            " thinks it's the leader, but so does " + logManager.nodeId());
                    }
                    leaderInfo = new LeaderInfo(logManager.nodeId(), curEpoch);
                }
            }
            if (leaderInfo == null) {
                throw new RuntimeException("No leader found.");
            }
            value.set(leaderInfo);
        });
        LeaderInfo result = value.get();
        return result;
    }

    List<LocalLogManager> logManagers() {
        return logManagers;
    }

    @Override
    public void close() throws InterruptedException {
        try {
            for (LocalLogManager logManager : logManagers) {
                logManager.beginShutdown();
            }
            for (LocalLogManager logManager : logManagers) {
                logManager.close();
            }
            Utils.delete(dir);
        } catch (IOException e) {
            log.error("Error deleting {}", dir.getAbsolutePath(), e);
        }
    }
}
