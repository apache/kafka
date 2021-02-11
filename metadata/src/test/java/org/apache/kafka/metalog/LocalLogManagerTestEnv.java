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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metalog.LocalLogManager.SharedLogData;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
     * The shared data for our LocalLogManager instances.
     */
    private final SharedLogData shared;

    /**
     * A list of log managers.
     */
    private final List<LocalLogManager> logManagers;

    public static LocalLogManagerTestEnv createWithMockListeners(int numManagers) throws Exception {
        LocalLogManagerTestEnv testEnv = new LocalLogManagerTestEnv(numManagers);
        try {
            for (LocalLogManager logManager : testEnv.logManagers) {
                logManager.register(new MockMetaLogManagerListener());
            }
        } catch (Exception e) {
            testEnv.close();
            throw e;
        }
        return testEnv;
    }

    public LocalLogManagerTestEnv(int numManagers) throws Exception {
        dir = TestUtils.tempDirectory();
        shared = new SharedLogData();
        List<LocalLogManager> newLogManagers = new ArrayList<>(numManagers);
        try {
            for (int nodeId = 0; nodeId < numManagers; nodeId++) {
                newLogManagers.add(new LocalLogManager(
                    new LogContext(String.format("[LocalLogManager %d] ", nodeId)),
                    nodeId,
                    shared,
                    String.format("LocalLogManager-%d_", nodeId)));
            }
            for (LocalLogManager logManager : newLogManagers) {
                logManager.initialize();
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

    MetaLogLeader waitForLeader() throws InterruptedException {
        AtomicReference<MetaLogLeader> value = new AtomicReference<>(null);
        TestUtils.retryOnExceptionWithTimeout(3, 20000, () -> {
            MetaLogLeader result = null;
            for (LocalLogManager logManager : logManagers) {
                MetaLogLeader leader = logManager.leader();
                if (leader.nodeId() == logManager.nodeId()) {
                    if (result != null) {
                        throw new RuntimeException("node " + leader.nodeId() +
                            " thinks it's the leader, but so does " + result.nodeId());
                    }
                    result = leader;
                }
            }
            if (result == null) {
                throw new RuntimeException("No leader found.");
            }
            value.set(result);
        });
        return value.get();
    }

    public List<LocalLogManager> logManagers() {
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
