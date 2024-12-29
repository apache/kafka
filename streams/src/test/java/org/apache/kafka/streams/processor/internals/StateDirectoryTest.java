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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory.TaskDirectory;
import org.apache.kafka.streams.processor.internals.metrics.StreamsMetricsImpl;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.MockKeyValueStore;
import org.apache.kafka.test.TestUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.streams.processor.internals.StateDirectory.PROCESS_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.toTaskDirString;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class StateDirectoryTest {

    private static final Logger log = LoggerFactory.getLogger(StateDirectoryTest.class);
    private final MockTime time = new MockTime();
    private File stateDir;
    private final String applicationId = "applicationId";
    private StreamsConfig config;
    private StateDirectory directory;
    private File appDir;

    private void initializeStateDirectory(final boolean createStateDirectory, final boolean hasNamedTopology) throws IOException {
        stateDir = new File(TestUtils.IO_TMP_DIR, "kafka-" + TestUtils.randomString(5));
        if (!createStateDirectory) {
            cleanup();
        }
        config = new StreamsConfig(new Properties() {
            {
                put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
            }
        });
        directory = new StateDirectory(config, time, createStateDirectory, hasNamedTopology);
        appDir = new File(stateDir, applicationId);
    }

    @BeforeEach
    public void before() throws IOException {
        initializeStateDirectory(true, false);
    }

    @AfterEach
    public void cleanup() throws IOException {
        Utils.delete(stateDir);
    }

    @Test
    public void shouldCreateBaseDirectory() {
        assertTrue(stateDir.exists());
        assertTrue(stateDir.isDirectory());
        assertTrue(appDir.exists());
        assertTrue(appDir.isDirectory());
    }

    @Test
    public void shouldHaveSecurePermissions() {
        assertPermissions(stateDir);
        assertPermissions(appDir);
    }
    
    private void assertPermissions(final File file) {
        final Path path = file.toPath();
        if (path.getFileSystem().supportedFileAttributeViews().contains("posix")) {
            final Set<PosixFilePermission> expectedPermissions = EnumSet.of(
                    PosixFilePermission.OWNER_EXECUTE,
                    PosixFilePermission.GROUP_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.GROUP_EXECUTE,
                    PosixFilePermission.OWNER_READ);
            try {
                final Set<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(path);
                assertThat(expectedPermissions, equalTo(filePermissions));
            } catch (final IOException e) {
                fail("Should create correct files and set correct permissions");
            }
        } else {
            assertThat(file.canRead(), is(true));
            assertThat(file.canWrite(), is(true));
            assertThat(file.canExecute(), is(true));
        }
    }

    @Test
    public void shouldParseUnnamedTaskId() {
        final TaskId task = new TaskId(1, 0);
        assertThat(TaskId.parse(task.toString()), equalTo(task));
    }

    @Test
    public void shouldParseNamedTaskId() {
        final TaskId task = new TaskId(1, 0, "namedTopology");
        assertThat(TaskId.parse(task.toString()), equalTo(task));
    }

    @Test
    public void shouldCreateTaskStateDirectory() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
        assertTrue(taskDirectory.exists());
        assertTrue(taskDirectory.isDirectory());
    }

    @Test
    public void shouldBeTrueIfAlreadyHoldsLock() {
        final TaskId taskId = new TaskId(0, 0);
        directory.getOrCreateDirectoryForTask(taskId);
        directory.lock(taskId);
        try {
            assertTrue(directory.lock(taskId));
        } finally {
            directory.unlock(taskId);
        }
    }

    @Test
    public void shouldBeAbleToUnlockEvenWithoutLocking() {
        final TaskId taskId = new TaskId(0, 0);
        directory.unlock(taskId);
    }

    @Test
    public void shouldReportDirectoryEmpty() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        // when task dir first created, it should be empty
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        // after locking, it should still be empty
        directory.lock(taskId);
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        // after writing checkpoint, it should still be empty
        final OffsetCheckpoint checkpointFile = new OffsetCheckpoint(new File(directory.getOrCreateDirectoryForTask(taskId), CHECKPOINT_FILE_NAME));
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        checkpointFile.write(Collections.singletonMap(new TopicPartition("topic", 0), 0L));
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        // if some store dir is created, it should not be empty
        final File dbDir = new File(new File(directory.getOrCreateDirectoryForTask(taskId), "db"), "store1");

        Files.createDirectories(dbDir.getParentFile().toPath());
        Files.createDirectories(dbDir.getAbsoluteFile().toPath());

        assertFalse(directory.directoryForTaskIsEmpty(taskId));

        // after wiping out the state dir, the dir should show as empty again
        Utils.delete(dbDir.getParentFile());
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        directory.unlock(taskId);
        assertTrue(directory.directoryForTaskIsEmpty(taskId));
    }

    @Test
    public void shouldThrowProcessorStateException() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);

        assertThrows(ProcessorStateException.class, () -> directory.getOrCreateDirectoryForTask(taskId));
    }

    @Test
    public void shouldThrowProcessorStateExceptionIfStateDirOccupied() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        // Replace application's stateDir to regular file
        Utils.delete(appDir);
        Files.createFile(appDir.toPath());

        assertThrows(ProcessorStateException.class, () -> directory.getOrCreateDirectoryForTask(taskId));
    }

    @Test
    public void shouldThrowProcessorStateExceptionIfTestDirOccupied() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        // Replace taskDir to a regular file
        final File taskDir = new File(appDir, toTaskDirString(taskId));
        Utils.delete(taskDir);
        Files.createFile(taskDir.toPath());

        // Error: ProcessorStateException should be thrown.
        assertThrows(ProcessorStateException.class, () -> directory.getOrCreateDirectoryForTask(taskId));
    }

    @Test
    public void shouldNotThrowIfStateDirectoryHasBeenDeleted() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);
        assertThrows(IllegalStateException.class, () -> directory.lock(taskId));
    }

    @Test
    public void shouldLockMultipleTaskDirectories() {
        final TaskId taskId = new TaskId(0, 0);
        final TaskId taskId2 = new TaskId(1, 0);

        assertThat(directory.lock(taskId), is(true));
        assertThat(directory.lock(taskId2), is(true));
        directory.unlock(taskId);
        directory.unlock(taskId2);
    }

    @Test
    public void shouldCleanUpTaskStateDirectoriesThatAreNotCurrentlyLocked() {
        final TaskId task0 = new TaskId(0, 0);
        final TaskId task1 = new TaskId(1, 0);
        final TaskId task2 = new TaskId(2, 0);
        try {
            assertTrue(new File(directory.getOrCreateDirectoryForTask(task0), "store").mkdir());
            assertTrue(new File(directory.getOrCreateDirectoryForTask(task1), "store").mkdir());
            assertTrue(new File(directory.getOrCreateDirectoryForTask(task2), "store").mkdir());

            directory.lock(task0);
            directory.lock(task1);

            final TaskDirectory dir0 = new TaskDirectory(new File(appDir, toTaskDirString(task0)), null);
            final TaskDirectory dir1 = new TaskDirectory(new File(appDir, toTaskDirString(task1)), null);
            final TaskDirectory dir2 = new TaskDirectory(new File(appDir, toTaskDirString(task2)), null);

            List<TaskDirectory> files = directory.listAllTaskDirectories();
            assertEquals(Set.of(dir0, dir1, dir2), new HashSet<>(files));

            files = directory.listNonEmptyTaskDirectories();
            assertEquals(Set.of(dir0, dir1, dir2), new HashSet<>(files));

            time.sleep(5000);
            directory.cleanRemovedTasks(0);

            files = directory.listAllTaskDirectories();
            assertEquals(Set.of(dir0, dir1), new HashSet<>(files));

            files = directory.listNonEmptyTaskDirectories();
            assertEquals(Set.of(dir0, dir1), new HashSet<>(files));
        } finally {
            directory.unlock(task0);
            directory.unlock(task1);
        }
    }

    @Test
    public void shouldCleanupStateDirectoriesWhenLastModifiedIsLessThanNowMinusCleanupDelay() {
        final File dir = directory.getOrCreateDirectoryForTask(new TaskId(2, 0));
        assertTrue(new File(dir, "store").mkdir());

        final int cleanupDelayMs = 60000;
        directory.cleanRemovedTasks(cleanupDelayMs);
        assertTrue(dir.exists());
        assertEquals(1, directory.listAllTaskDirectories().size());
        assertEquals(1, directory.listNonEmptyTaskDirectories().size());

        time.sleep(cleanupDelayMs + 1000);
        directory.cleanRemovedTasks(cleanupDelayMs);
        assertFalse(dir.exists());
        assertEquals(0, directory.listAllTaskDirectories().size());
        assertEquals(0, directory.listNonEmptyTaskDirectories().size());
    }

    @Test
    public void shouldCleanupObsoleteTaskDirectoriesAndDeleteTheDirectoryItself() {
        final File dir = directory.getOrCreateDirectoryForTask(new TaskId(2, 0));
        assertTrue(new File(dir, "store").mkdir());
        assertEquals(1, directory.listAllTaskDirectories().size());
        assertEquals(1, directory.listNonEmptyTaskDirectories().size());

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            time.sleep(5000);
            directory.cleanRemovedTasks(0);
            assertFalse(dir.exists());
            assertEquals(0, directory.listAllTaskDirectories().size());
            assertEquals(0, directory.listNonEmptyTaskDirectories().size());
            assertThat(
                appender.getMessages(),
                hasItem(containsString("Deleting obsolete state directory"))
            );
        }
    }

    @Test
    public void shouldNotRemoveNonTaskDirectoriesAndFiles() {
        final File otherDir = TestUtils.tempDirectory(stateDir.toPath(), "foo");
        directory.cleanRemovedTasks(0);
        assertTrue(otherDir.exists());
    }

    @Test
    public void shouldReturnEmptyArrayForNonPersistentApp() throws IOException {
        initializeStateDirectory(false, false);
        assertTrue(directory.listAllTaskDirectories().isEmpty());
    }

    @Test
    public void shouldReturnEmptyArrayIfStateDirDoesntExist() throws IOException {
        cleanup();
        assertFalse(stateDir.exists());
        assertTrue(directory.listAllTaskDirectories().isEmpty());
    }

    @Test
    public void shouldReturnEmptyArrayIfListFilesReturnsNull() throws IOException {
        stateDir = new File(TestUtils.IO_TMP_DIR, "kafka-" + TestUtils.randomString(5));
        directory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time,
            true,
            false);
        appDir = new File(stateDir, applicationId);

        // make sure the File#listFiles returns null and StateDirectory#listAllTaskDirectories is able to handle null
        Utils.delete(appDir);
        Files.createFile(appDir.toPath());
        assertTrue(Files.exists(appDir.toPath()));
        assertNull(appDir.listFiles());
        assertEquals(0, directory.listAllTaskDirectories().size());
    }

    @Test
    public void shouldOnlyListNonEmptyTaskDirectories() throws IOException {
        TestUtils.tempDirectory(stateDir.toPath(), "foo");
        final TaskDirectory taskDir1 = new TaskDirectory(directory.getOrCreateDirectoryForTask(new TaskId(0, 0)), null);
        final TaskDirectory taskDir2 = new TaskDirectory(directory.getOrCreateDirectoryForTask(new TaskId(0, 1)), null);

        final File storeDir = new File(taskDir1.file(), "store");
        assertTrue(storeDir.mkdir());

        assertThat(Set.of(taskDir1, taskDir2), equalTo(new HashSet<>(directory.listAllTaskDirectories())));
        assertThat(singletonList(taskDir1), equalTo(directory.listNonEmptyTaskDirectories()));

        Utils.delete(taskDir1.file());

        assertThat(singleton(taskDir2), equalTo(new HashSet<>(directory.listAllTaskDirectories())));
        assertThat(emptyList(), equalTo(directory.listNonEmptyTaskDirectories()));
    }

    @Test
    public void shouldCreateDirectoriesIfParentDoesntExist() {
        final File tempDir = TestUtils.tempDirectory();
        final File stateDir = new File(new File(tempDir, "foo"), "state-dir");
        final StateDirectory stateDirectory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time,
            true,
            false);
        final File taskDir = stateDirectory.getOrCreateDirectoryForTask(new TaskId(0, 0));
        assertTrue(stateDir.exists());
        assertTrue(taskDir.exists());
    }

    @Test
    public void shouldNotLockStateDirLockedByAnotherThread() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final Thread thread = new Thread(() -> directory.lock(taskId));
        thread.start();
        thread.join(30000);
        assertFalse(directory.lock(taskId));
    }

    @Test
    public void shouldNotUnLockStateDirLockedByAnotherThread() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final CountDownLatch lockLatch = new CountDownLatch(1);
        final CountDownLatch unlockLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionOnThread = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                directory.lock(taskId);
                lockLatch.countDown();
                unlockLatch.await();
                directory.unlock(taskId);
            } catch (final Exception e) {
                exceptionOnThread.set(e);
            }
        });
        thread.start();
        lockLatch.await(5, TimeUnit.SECONDS);

        assertNull(exceptionOnThread.get(), "should not have had an exception on other thread");
        directory.unlock(taskId);
        assertFalse(directory.lock(taskId));

        unlockLatch.countDown();
        thread.join(30000);

        assertNull(exceptionOnThread.get(), "should not have had an exception on other thread");
        assertTrue(directory.lock(taskId));
    }

    @Test
    public void shouldCleanupAllTaskDirectoriesIncludingGlobalOne() {
        final TaskId id = new TaskId(1, 0);
        directory.getOrCreateDirectoryForTask(id);
        directory.globalStateDir();

        final File dir0 = new File(appDir, id.toString());
        final File globalDir = new File(appDir, "global");
        assertEquals(Set.of(dir0, globalDir), Arrays.stream(
            Objects.requireNonNull(appDir.listFiles())).collect(Collectors.toSet()));

        directory.clean();

        // if appDir is empty, it is deleted in StateDirectory#clean process.
        assertFalse(appDir.exists());
    }

    @Test
    public void shouldNotCreateBaseDirectory() throws IOException {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            initializeStateDirectory(false, false);
            assertThat(stateDir.exists(), is(false));
            assertThat(appDir.exists(), is(false));
            assertThat(appender.getMessages(),
                not(hasItem(containsString("Error changing permissions for the state or base directory"))));
        }
    }

    @Test
    public void shouldNotCreateTaskStateDirectory() throws IOException {
        initializeStateDirectory(false, false);
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
        assertFalse(taskDirectory.exists());
    }

    @Test
    public void shouldNotCreateGlobalStateDirectory() throws IOException {
        initializeStateDirectory(false, false);
        final File globalStateDir = directory.globalStateDir();
        assertFalse(globalStateDir.exists());
    }

    @Test
    public void shouldLockTaskStateDirectoryWhenDirectoryCreationDisabled() throws IOException {
        initializeStateDirectory(false, false);
        final TaskId taskId = new TaskId(0, 0);
        assertTrue(directory.lock(taskId));
    }

    @Test
    public void shouldNotFailWhenCreatingTaskDirectoryInParallel() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final AtomicBoolean passed = new AtomicBoolean(true);

        final CreateTaskDirRunner runner = new CreateTaskDirRunner(directory, taskId, passed);

        final Thread t1 = new Thread(runner);
        final Thread t2 = new Thread(runner);

        t1.start();
        t2.start();

        t1.join(Duration.ofMillis(500L).toMillis());
        t2.join(Duration.ofMillis(500L).toMillis());

        assertNotNull(runner.taskDirectory);
        assertTrue(passed.get());
        assertTrue(runner.taskDirectory.exists());
        assertTrue(runner.taskDirectory.isDirectory());
    }

    @Test
    public void shouldDeleteAppDirWhenCleanUpIfEmpty() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
        final File testFile = new File(taskDirectory, "testFile");
        assertThat(testFile.mkdir(), is(true));
        assertThat(directory.directoryForTaskIsEmpty(taskId), is(false));

        // call StateDirectory#clean
        directory.clean();

        // if appDir is empty, it is deleted in StateDirectory#clean process.
        assertFalse(appDir.exists());
    }

    @Test
    public void shouldNotDeleteAppDirWhenCleanUpIfNotEmpty() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
        final File testFile = new File(taskDirectory, "testFile");
        assertThat(testFile.mkdir(), is(true));
        assertThat(directory.directoryForTaskIsEmpty(taskId), is(false));

        // Create a dummy file in appDir; for this, appDir will not be empty after cleanup.
        final File dummyFile = new File(appDir, "dummy");
        Files.createFile(dummyFile.toPath());

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            // call StateDirectory#clean
            directory.clean();
            assertThat(
                appender.getMessages(),
                hasItem(endsWith(String.format("Failed to delete state store directory of %s for it is not empty", appDir.getAbsolutePath())))
            );
        }
    }

    @Test
    public void shouldLogManualUserCallMessage() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
        final File testFile = new File(taskDirectory, "testFile");
        assertThat(testFile.mkdir(), is(true));
        assertThat(directory.directoryForTaskIsEmpty(taskId), is(false));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            directory.clean();
            assertThat(
                appender.getMessages(),
                hasItem(endsWith("as user calling cleanup."))
            );
        }
    }

    @Test
    public void shouldLogStateDirCleanerMessage() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
        final File testFile = new File(taskDirectory, "testFile");
        assertThat(testFile.mkdir(), is(true));
        assertThat(directory.directoryForTaskIsEmpty(taskId), is(false));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            final long cleanupDelayMs = 0;
            time.sleep(5000);
            directory.cleanRemovedTasks(cleanupDelayMs);
            assertThat(appender.getMessages(), hasItem(endsWith("ms has elapsed (cleanup delay is " + cleanupDelayMs + "ms).")));
        }
    }

    @Test
    public void shouldLogTempDirMessage() {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            new StateDirectory(
                new StreamsConfig(
                    mkMap(
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ""),
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "")
                    )
                ),
                new MockTime(),
                true,
                false
            );
            assertThat(
                appender.getMessages(),
                hasItem("Using an OS temp directory in the state.dir property can cause failures with writing the" +
                            " checkpoint file due to the fact that this directory can be cleared by the OS." +
                            " Resolved state.dir: [" + System.getProperty("java.io.tmpdir") + "/kafka-streams]")
            );
        }
    }

    /* Named Topology Tests */

    @Test
    public void shouldCreateTaskDirectoriesUnderNamedTopologyDirs() throws IOException {
        initializeStateDirectory(true, true);

        directory.getOrCreateDirectoryForTask(new TaskId(0, 0, "topology1"));
        directory.getOrCreateDirectoryForTask(new TaskId(0, 1, "topology1"));
        directory.getOrCreateDirectoryForTask(new TaskId(0, 0, "topology2"));

        assertThat(new File(appDir, "__topology1__").exists(), is(true));
        assertThat(new File(appDir, "__topology1__").isDirectory(), is(true));
        assertThat(new File(appDir, "__topology2__").exists(), is(true));
        assertThat(new File(appDir, "__topology2__").isDirectory(), is(true));

        assertThat(new File(new File(appDir, "__topology1__"), "0_0").exists(), is(true));
        assertThat(new File(new File(appDir, "__topology1__"), "0_0").isDirectory(), is(true));
        assertThat(new File(new File(appDir, "__topology1__"), "0_1").exists(), is(true));
        assertThat(new File(new File(appDir, "__topology1__"), "0_1").isDirectory(), is(true));
        assertThat(new File(new File(appDir, "__topology2__"), "0_0").exists(), is(true));
        assertThat(new File(new File(appDir, "__topology2__"), "0_0").isDirectory(), is(true));
    }

    @Test
    public void shouldOnlyListNonEmptyTaskDirectoriesInNamedTopologies() throws IOException {
        initializeStateDirectory(true, true);

        TestUtils.tempDirectory(appDir.toPath(), "foo");
        final TaskDirectory taskDir1 = new TaskDirectory(directory.getOrCreateDirectoryForTask(new TaskId(0, 0, "topology1")), "topology1");
        final TaskDirectory taskDir2 = new TaskDirectory(directory.getOrCreateDirectoryForTask(new TaskId(0, 1, "topology1")), "topology1");
        final TaskDirectory taskDir3 = new TaskDirectory(directory.getOrCreateDirectoryForTask(new TaskId(0, 0, "topology2")), "topology2");

        final File storeDir = new File(taskDir1.file(), "store");
        assertTrue(storeDir.mkdir());

        assertThat(new HashSet<>(directory.listAllTaskDirectories()), equalTo(Set.of(taskDir1, taskDir2, taskDir3)));
        assertThat(directory.listNonEmptyTaskDirectories(), equalTo(singletonList(taskDir1)));

        Utils.delete(taskDir1.file());

        assertThat(new HashSet<>(directory.listAllTaskDirectories()), equalTo(Set.of(taskDir2, taskDir3)));
        assertThat(directory.listNonEmptyTaskDirectories(), equalTo(emptyList()));
    }

    @Test
    public void shouldRemoveNonEmptyNamedTopologyDirsWhenCallingClean() throws Exception {
        initializeStateDirectory(true, true);
        final File taskDir = directory.getOrCreateDirectoryForTask(new TaskId(2, 0, "topology1"));
        final File namedTopologyDir = new File(appDir, "__topology1__");

        assertThat(taskDir.exists(), is(true));
        assertThat(namedTopologyDir.exists(), is(true));
        directory.clean();
        assertThat(taskDir.exists(), is(false));
        assertThat(namedTopologyDir.exists(), is(false));
    }

    @Test
    public void shouldRemoveEmptyNamedTopologyDirsWhenCallingClean() throws IOException {
        initializeStateDirectory(true, true);
        final File namedTopologyDir = new File(appDir, "__topology1__");
        assertThat(namedTopologyDir.mkdir(), is(true));
        assertThat(namedTopologyDir.exists(), is(true));
        directory.clean();
        assertThat(namedTopologyDir.exists(), is(false));
    }

    @Test
    public void shouldRemoveNonEmptyNamedTopologyDirsWhenCallingClearLocalStateForNamedTopology() throws Exception {
        initializeStateDirectory(true, true);
        final String topologyName = "topology1";
        final File taskDir = directory.getOrCreateDirectoryForTask(new TaskId(2, 0, topologyName));
        final File namedTopologyDir = new File(appDir, "__" + topologyName + "__");

        assertThat(taskDir.exists(), is(true));
        assertThat(namedTopologyDir.exists(), is(true));
        directory.clearLocalStateForNamedTopology(topologyName);
        assertThat(taskDir.exists(), is(false));
        assertThat(namedTopologyDir.exists(), is(false));
    }

    @Test
    public void shouldRemoveEmptyNamedTopologyDirsWhenCallingClearLocalStateForNamedTopology() throws IOException {
        initializeStateDirectory(true, true);
        final String topologyName = "topology1";
        final File namedTopologyDir = new File(appDir, "__" + topologyName + "__");
        assertThat(namedTopologyDir.mkdir(), is(true));
        assertThat(namedTopologyDir.exists(), is(true));
        directory.clearLocalStateForNamedTopology(topologyName);
        assertThat(namedTopologyDir.exists(), is(false));
    }

    @Test
    public void shouldNotRemoveDirsThatDoNotMatchNamedTopologyDirsWhenCallingClean() throws IOException {
        initializeStateDirectory(true, true);
        final File someDir = new File(appDir, "_not-a-valid-named-topology_dir_name_");
        assertThat(someDir.mkdir(), is(true));
        assertThat(someDir.exists(), is(true));
        directory.clean();
        assertThat(someDir.exists(), is(true));
    }

    @Test
    public void shouldCleanupObsoleteTaskDirectoriesInNamedTopologiesAndDeleteTheParentDirectories() throws IOException {
        initializeStateDirectory(true, true);

        final File taskDir = directory.getOrCreateDirectoryForTask(new TaskId(2, 0, "topology1"));
        final File namedTopologyDir = new File(appDir, "__topology1__");
        assertThat(namedTopologyDir.exists(), is(true));
        assertThat(taskDir.exists(), is(true));
        assertTrue(new File(taskDir, "store").mkdir());
        assertThat(directory.listAllTaskDirectories().size(), is(1));
        assertThat(directory.listNonEmptyTaskDirectories().size(), is(1));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            time.sleep(5000);
            directory.cleanRemovedTasks(0);
            assertThat(taskDir.exists(), is(false));
            assertThat(namedTopologyDir.exists(), is(false));
            assertThat(directory.listAllTaskDirectories().size(), is(0));
            assertThat(directory.listNonEmptyTaskDirectories().size(), is(0));
            assertThat(
                appender.getMessages(),
                hasItem(containsString("Deleting obsolete state directory"))
            );
        }
    }

    @Test
    public void shouldPersistProcessIdAcrossRestart() {
        final UUID processId = directory.initializeProcessId();
        directory.close();
        assertThat(directory.initializeProcessId(), equalTo(processId));
    }

    @Test
    public void shouldGetFreshProcessIdIfProcessFileDeleted() {
        final UUID processId = directory.initializeProcessId();
        directory.close();

        final File processFile = new File(appDir, PROCESS_FILE_NAME);
        assertThat(processFile.exists(), is(true));
        assertThat(processFile.delete(), is(true));

        assertThat(directory.initializeProcessId(), not(processId));
    }

    @Test
    public void shouldGetFreshProcessIdIfJsonUnreadable() throws Exception {
        final File processFile = new File(appDir, PROCESS_FILE_NAME);
        Files.createFile(processFile.toPath());
        final UUID processId = UUID.randomUUID();

        final FileOutputStream fileOutputStream = new FileOutputStream(processFile);
        try (final BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))) {
            writer.write(processId.toString());
            writer.flush();
            fileOutputStream.getFD().sync();
        }

        assertThat(directory.initializeProcessId(), not(processId));
    }

    @Test
    public void shouldReadFutureProcessFileFormat() throws Exception {
        final File processFile = new File(appDir, PROCESS_FILE_NAME);
        final ObjectMapper mapper = new ObjectMapper();
        final UUID processId = UUID.randomUUID();
        mapper.writeValue(processFile, new FutureStateDirectoryProcessFile(processId, "some random junk"));

        assertThat(directory.initializeProcessId(), equalTo(processId));
    }

    @Test
    public void shouldNotInitializeStandbyTasksWhenNoLocalState() {
        final TaskId taskId = new TaskId(0, 0);
        initializeStartupTasks(new TaskId(0, 0), false);
        assertFalse(directory.hasStartupTasks());
        assertNull(directory.removeStartupTask(taskId));
        assertFalse(directory.hasStartupTasks());
    }

    @Test
    public void shouldInitializeStandbyTasksForLocalState() {
        final TaskId taskId = new TaskId(0, 0);
        initializeStartupTasks(new TaskId(0, 0), true);
        assertTrue(directory.hasStartupTasks());
        assertNotNull(directory.removeStartupTask(taskId));
        assertFalse(directory.hasStartupTasks());
        assertNull(directory.removeStartupTask(taskId));
    }

    @Test
    public void shouldNotAssignStartupTasksWeDontHave() {
        final TaskId taskId = new TaskId(0, 0);
        initializeStartupTasks(taskId, false);
        final Task task = directory.removeStartupTask(taskId);
        assertNull(task);
    }

    private class FakeStreamThread extends Thread {
        private final TaskId taskId;
        private final AtomicReference<Task> result;

        private FakeStreamThread(final TaskId taskId, final AtomicReference<Task> result) {
            this.taskId = taskId;
            this.result = result;
        }

        @Override
        public void run() {
            result.set(directory.removeStartupTask(taskId));
        }
    }

    @Test
    public void shouldAssignStartupTaskToStreamThread() throws InterruptedException {
        final TaskId taskId = new TaskId(0, 0);

        initializeStartupTasks(taskId, true);

        // main thread owns the newly initialized tasks
        assertThat(directory.lockOwner(taskId), is(Thread.currentThread()));

        // spawn off a "fake" StreamThread, so we can verify the lock was updated to the correct thread
        final AtomicReference<Task> result = new AtomicReference<>();
        final Thread streamThread = new FakeStreamThread(taskId, result);
        streamThread.start();
        streamThread.join();
        final Task task = result.get();

        assertNotNull(task);
        assertThat(task, instanceOf(StandbyTask.class));

        // verify the owner of the task directory lock has been shifted over to our assigned StreamThread
        assertThat(directory.lockOwner(taskId), is(instanceOf(FakeStreamThread.class)));
    }

    @Test
    public void shouldUnlockStartupTasksOnClose() {
        final TaskId taskId = new TaskId(0, 0);
        initializeStartupTasks(taskId, true);

        assertEquals(Thread.currentThread(), directory.lockOwner(taskId));
        directory.closeStartupTasks();
        assertNull(directory.lockOwner(taskId));
    }

    @Test
    public void shouldCloseStartupTasksOnDirectoryClose() {
        final StateStore store = initializeStartupTasks(new TaskId(0, 0), true);

        assertTrue(directory.hasStartupTasks());
        assertTrue(store.isOpen());

        directory.close();

        assertFalse(directory.hasStartupTasks());
        assertFalse(store.isOpen());
    }

    @Test
    public void shouldNotCloseStartupTasksOnAutoCleanUp() {
        // we need to set this because the auto-cleanup uses the last-modified time from the filesystem,
        // which can't be mocked
        time.setCurrentTimeMs(System.currentTimeMillis());

        final StateStore store = initializeStartupTasks(new TaskId(0, 0), true);

        assertTrue(directory.hasStartupTasks());
        assertTrue(store.isOpen());

        time.sleep(10000);

        directory.cleanRemovedTasks(1000);

        assertTrue(directory.hasStartupTasks());
        assertTrue(store.isOpen());
    }

    private StateStore initializeStartupTasks(final TaskId taskId, final boolean createTaskDir) {
        directory.initializeProcessId();
        final TopologyMetadata metadata = Mockito.mock(TopologyMetadata.class);
        final TopologyConfig topologyConfig = new TopologyConfig(config);

        final StateStore store = new MockKeyValueStore("test", true);

        if (createTaskDir) {
            final File taskDir = directory.getOrCreateDirectoryForTask(taskId);
            final File storeDir = new File(taskDir, store.name());
            storeDir.mkdir();
        }

        final ProcessorTopology processorTopology = new ProcessorTopology(
                Collections.emptyList(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.singletonList(store),
                Collections.emptyList(),
                Collections.singletonMap(store.name(), store.name() + "-changelog"),
                Collections.emptySet(),
                Collections.emptyMap()
        );
        Mockito.when(metadata.buildSubtopology(ArgumentMatchers.any())).thenReturn(processorTopology);
        Mockito.when(metadata.taskConfig(ArgumentMatchers.any())).thenReturn(topologyConfig.getTaskConfig());

        directory.initializeStartupTasks(metadata, new StreamsMetricsImpl(new Metrics(), "test", "processId", time), new LogContext("test"));

        return store;
    }

    private static class FutureStateDirectoryProcessFile {

        @JsonProperty
        private final UUID processId;

        @JsonProperty
        private final String newField;

        // required by jackson -- do not remove, your IDE may be warning that this is unused but it's lying to you
        public FutureStateDirectoryProcessFile() {
            this.processId = null;
            this.newField = null;
        }

        FutureStateDirectoryProcessFile(final UUID processId, final String newField) {
            this.processId = processId;
            this.newField = newField;

        }
    }

    private static class CreateTaskDirRunner implements Runnable {
        private final StateDirectory directory;
        private final TaskId taskId;
        private final AtomicBoolean passed;

        private File taskDirectory;

        private CreateTaskDirRunner(final StateDirectory directory,
                                    final TaskId taskId,
                                    final AtomicBoolean passed) {
            this.directory = directory;
            this.taskId = taskId;
            this.passed = passed;
        }

        @Override
        public void run() {
            try {
                taskDirectory = directory.getOrCreateDirectoryForTask(taskId);
            } catch (final ProcessorStateException error) {
                passed.set(false);
            }
        }
    }
}