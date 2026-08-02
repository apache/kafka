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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task.TaskType;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A/B on the mechanism that makes the KIP-892/1035 OOORE possible: the EOS
 * wipe-on-unclean-close, with transactional state stores (KIP-892) enabled vs
 * disabled.
 *
 * <p>Why this is THE mechanism: the restore
 * {@code OffsetOutOfRangeException} requires a from-scratch restore, i.e. a task
 * that re-initialises with no checkpoint. On the soaks that state is produced by
 * {@code StateManagerUtil.closeStateManager} deleting the entire task directory —
 * RocksDB stores and their KIP-1035 offsets column family included — after an
 * unclean close under exactly-once:
 *
 * <pre>
 *   // 4.3
 *   wipeStateStore = !closeClean &amp;&amp; eosEnabled;
 *   // trunk
 *   wipeStateStore = !closeClean &amp;&amp; eosEnabled
 *       &amp;&amp; (!transactionalStateStoresEnabled || stateMgr.hasCorruptedStores());
 * </pre>
 *
 * <p>{@code StateManagerUtilTest} already covers this with Mockito verification of
 * {@code removeTaskOffsets}. These tests instead assert the **filesystem** effect —
 * the task directory and its contents surviving or not — because that is what
 * actually decides whether the next re-initialisation finds a checkpoint.
 *
 * <p>Note the config default: {@code enable.transactional.statestores} is
 * {@code false} (StreamsConfig, Importance.LOW). So case A below is the
 * out-of-the-box behaviour on trunk, not an exotic configuration.
 */
public class TxnStoreWipeABTest {

    private final Logger logger = LoggerFactory.getLogger(TxnStoreWipeABTest.class);
    private final TaskId taskId = new TaskId(0, 0);

    /** A task directory with a file in it, standing in for a populated RocksDB store. */
    private File populatedTaskDir() throws IOException {
        final File dir = TestUtils.tempDirectory("task-state");
        Files.writeString(new File(dir, "MANIFEST-000001").toPath(), "rocksdb");
        Files.writeString(new File(dir, ".checkpoint").toPath(), "offsets");
        assertTrue(new File(dir, "MANIFEST-000001").exists(), "fixture should start populated");
        return dir;
    }

    private File runUncleanClose(final boolean transactionalStateStoresEnabled,
                                 final boolean hasCorruptedStores) throws IOException {
        final File taskDir = populatedTaskDir();

        final ProcessorStateManager stateManager = mock(ProcessorStateManager.class);
        final StateDirectory stateDirectory = mock(StateDirectory.class);
        when(stateManager.taskId()).thenReturn(taskId);
        when(stateManager.baseDir()).thenReturn(taskDir);
        when(stateManager.hasCorruptedStores()).thenReturn(hasCorruptedStores);
        when(stateDirectory.lock(taskId)).thenReturn(true);

        StateManagerUtil.closeStateManager(
            logger,
            "ab-test:",
            /* closeClean */ false,          // the unclean close the soak injects hourly
            /* eosEnabled */ true,           // exactly_once_v2, as the soaks run
            transactionalStateStoresEnabled,
            stateManager,
            stateDirectory,
            TaskType.ACTIVE);

        return taskDir;
    }

    /**
     * A: transactional stores DISABLED — this is 4.3 always (the config is inert
     * there) and it is also trunk's DEFAULT. The whole task directory is deleted,
     * so the next re-init logs "did not find checkpoint offset" and restores from
     * log-start, where retention can lap it.
     */
    @Test
    public void withoutTransactionalStoresAnUncleanCloseWipesTheTaskDirectory() throws IOException {
        final File taskDir = runUncleanClose(false, false);

        assertFalse(taskDir.exists(),
            "with transactional stores disabled, an unclean EOS close must wipe the task "
                + "directory -- this is what creates the no-checkpoint restore the OOORE needs");
    }

    /**
     * B: transactional stores ENABLED (KIP-892) — the wipe is skipped, the store
     * and its KIP-1035 offsets survive, and the next re-init resumes from its
     * checkpoint instead of replaying from log-start. That is why the trunk soak
     * shows ~1% no-checkpoint re-inits against the 4.3 family's ~50%.
     */
    @Test
    public void withTransactionalStoresAnUncleanCloseKeepsTheTaskDirectory() throws IOException {
        final File taskDir = runUncleanClose(true, false);

        assertTrue(taskDir.exists(),
            "with transactional stores enabled the directory must survive an unclean close");
        assertTrue(new File(taskDir, "MANIFEST-000001").exists(),
            "the store contents must survive, not just the directory");
        assertTrue(new File(taskDir, ".checkpoint").exists(),
            "the checkpoint must survive -- this is what lets the restore RESUME");
    }

    /**
     * B': transactional stores enabled but the stores are already marked corrupted
     * (e.g. by an InvalidOffsetException). The wipe is still required, so KIP-892 is
     * not blanket protection -- once an OOORE has happened, the rebuild is the same.
     */
    @Test
    public void withTransactionalStoresCorruptedStoresAreStillWiped() throws IOException {
        final File taskDir = runUncleanClose(true, true);

        assertFalse(taskDir.exists(),
            "corrupted stores must still be wiped even with transactional stores enabled");
    }
}
