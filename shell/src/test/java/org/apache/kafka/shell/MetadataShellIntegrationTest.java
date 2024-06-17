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

package org.apache.kafka.shell;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import kafka.utils.FileLock;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(120)
@Tag("integration")
public class MetadataShellIntegrationTest {
    private final static Logger LOG = LoggerFactory.getLogger(MetadataShellIntegrationTest.class);

    static class IntegrationEnv implements AutoCloseable {
        File tempDir;
        MetadataShell shell;
        final MockFaultHandler faultHandler;

        IntegrationEnv() throws IOException {
            this.tempDir = Files.createTempDirectory("MetadataShellIntegrationTest").toFile();
            this.shell = null;
            this.faultHandler = new MockFaultHandler("testFailToGetLockOnSnapshotFile");
        }

        @Override
        public void close() {
            if (shell != null) {
                try {
                    shell.close();
                } catch (Throwable e) {
                    LOG.error("Error closing shell", e);
                } finally {
                    shell = null;
                }
            }
            if (tempDir != null) {
                try {
                    Utils.delete(tempDir);
                } catch (Throwable e) {
                    LOG.error("Error deleting tempDir", e);
                } finally {
                    tempDir = null;
                }
            }
            faultHandler.maybeRethrowFirstException();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testLock(boolean canLock) throws Exception {
        try (IntegrationEnv env = new IntegrationEnv()) {
            env.shell = new MetadataShell(null,
                new File(new File(env.tempDir, "__cluster_metadata-0"), "00000000000122906351-0000000226.checkpoint").getAbsolutePath(),
                    env.faultHandler);

            if (canLock) {
                assertEquals(NoSuchFileException.class,
                    assertThrows(ExecutionException.class,
                        () -> env.shell.run(Collections.emptyList())).
                            getCause().getClass());
            } else {
                FileLock fileLock = new FileLock(new File(env.tempDir, ".lock"));
                try {
                    fileLock.lock();
                    assertEquals("Unable to lock " + env.tempDir.getAbsolutePath() +
                        ". Please ensure that no broker or controller process is using this " +
                        "directory before proceeding.",
                        assertThrows(RuntimeException.class,
                            () -> env.shell.run(Collections.emptyList())).
                                getMessage());
                } finally {
                    fileLock.destroy();
                }
            }
        }
    }

    @Test
    public void testParentParent() {
        final File root = File.listRoots()[0];
        assertEquals(root, MetadataShell.parentParent(new File(new File(root, "a"), "b")));
    }

    @Test
    public void testParentParentOfRoot() {
        final File root = File.listRoots()[0];
        assertEquals(root, MetadataShell.parentParent(root));
    }
}
