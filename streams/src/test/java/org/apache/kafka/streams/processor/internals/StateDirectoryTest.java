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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.testutil.LogCaptureAppender;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.processor.internals.StateDirectory.LOCK_FILE_NAME;
import static org.apache.kafka.streams.processor.internals.StateManagerUtil.CHECKPOINT_FILE_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class StateDirectoryTest {

    private final MockTime time = new MockTime();
    private File stateDir;
    private final String applicationId = "applicationId";
    private StateDirectory directory;
    private File appDir;

    private void initializeStateDirectory(final boolean createStateDirectory) throws IOException {
        stateDir = new File(TestUtils.IO_TMP_DIR, "kafka-" + TestUtils.randomString(5));
        if (!createStateDirectory) {
            cleanup();
        }
        directory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time, createStateDirectory);
        appDir = new File(stateDir, applicationId);
    }

    @Before
    public void before() throws IOException {
        initializeStateDirectory(true);
    }

    @After
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
        final Set<PosixFilePermission> expectedPermissions = EnumSet.of(
            PosixFilePermission.OWNER_EXECUTE,
            PosixFilePermission.GROUP_READ,
            PosixFilePermission.OWNER_WRITE,
            PosixFilePermission.GROUP_EXECUTE,
            PosixFilePermission.OWNER_READ);

        final Path statePath = Paths.get(stateDir.getPath());
        final Path basePath = Paths.get(appDir.getPath());
        try {
            final Set<PosixFilePermission> baseFilePermissions = Files.getPosixFilePermissions(statePath);
            final Set<PosixFilePermission> appFilePermissions = Files.getPosixFilePermissions(basePath);
            assertThat(expectedPermissions, equalTo(baseFilePermissions));
            assertThat(expectedPermissions, equalTo(appFilePermissions));
        } catch (final IOException e) {
            fail("Should create correct files and set correct permissions");
        }
    }

    @Test
    public void shouldCreateTaskStateDirectory() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);
        assertTrue(taskDirectory.exists());
        assertTrue(taskDirectory.isDirectory());
    }

    @Test
    public void shouldLockTaskStateDirectory() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);

        directory.lock(taskId);

        try (
            final FileChannel channel = FileChannel.open(
                new File(taskDirectory, LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        ) {
            assertThrows(OverlappingFileLockException.class, channel::tryLock);
        } finally {
            directory.unlock(taskId);
        }
    }

    @Test
    public void shouldBeTrueIfAlreadyHoldsLock() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        directory.directoryForTask(taskId);
        directory.lock(taskId);
        try {
            assertTrue(directory.lock(taskId));
        } finally {
            directory.unlock(taskId);
        }
    }

    @Test
    public void shouldBeAbleToUnlockEvenWithoutLocking() throws IOException {
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
        final OffsetCheckpoint checkpointFile = new OffsetCheckpoint(new File(directory.directoryForTask(taskId), CHECKPOINT_FILE_NAME));
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        checkpointFile.write(Collections.singletonMap(new TopicPartition("topic", 0), 0L));
        assertTrue(directory.directoryForTaskIsEmpty(taskId));

        // if some store dir is created, it should not be empty
        final File dbDir = new File(new File(directory.directoryForTask(taskId), "db"), "store1");

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

        assertThrows(ProcessorStateException.class, () -> directory.directoryForTask(taskId));
    }

    @Test
    public void shouldNotLockDeletedDirectory() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);
        assertFalse(directory.lock(taskId));
    }
    
    @Test
    public void shouldLockMultipleTaskDirectories() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final File task1Dir = directory.directoryForTask(taskId);
        final TaskId taskId2 = new TaskId(1, 0);
        final File task2Dir = directory.directoryForTask(taskId2);


        try (
            final FileChannel channel1 = FileChannel.open(
                new File(task1Dir, LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
            final FileChannel channel2 = FileChannel.open(new File(task2Dir, LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            directory.lock(taskId);
            directory.lock(taskId2);

            assertThrows(OverlappingFileLockException.class, channel1::tryLock);
            assertThrows(OverlappingFileLockException.class, channel2::tryLock);
        } finally {
            directory.unlock(taskId);
            directory.unlock(taskId2);
        }
    }

    @Test
    public void shouldReleaseTaskStateDirectoryLock() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);

        directory.lock(taskId);
        directory.unlock(taskId);

        try (
            final FileChannel channel = FileChannel.open(
                new File(taskDirectory, LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            channel.tryLock();
        }
    }

    @Test
    public void shouldCleanUpTaskStateDirectoriesThatAreNotCurrentlyLocked() throws IOException {
        final TaskId task0 = new TaskId(0, 0);
        final TaskId task1 = new TaskId(1, 0);
        final TaskId task2 = new TaskId(2, 0);
        try {
            assertTrue(new File(directory.directoryForTask(task0), "store").mkdir());
            assertTrue(new File(directory.directoryForTask(task1), "store").mkdir());
            assertTrue(new File(directory.directoryForTask(task2), "store").mkdir());

            directory.lock(task0);
            directory.lock(task1);

            final File dir0 = new File(appDir, task0.toString());
            final File dir1 = new File(appDir, task1.toString());
            final File dir2 = new File(appDir, task2.toString());

            Set<File> files = Arrays.stream(
                Objects.requireNonNull(directory.listAllTaskDirectories())).collect(Collectors.toSet());
            assertEquals(mkSet(dir0, dir1, dir2), files);

            files = Arrays.stream(
                Objects.requireNonNull(directory.listNonEmptyTaskDirectories())).collect(Collectors.toSet());
            assertEquals(mkSet(dir0, dir1, dir2), files);

            time.sleep(5000);
            directory.cleanRemovedTasks(0);

            files = Arrays.stream(
                Objects.requireNonNull(directory.listAllTaskDirectories())).collect(Collectors.toSet());
            assertEquals(mkSet(dir0, dir1, dir2), files);

            files = Arrays.stream(
                Objects.requireNonNull(directory.listNonEmptyTaskDirectories())).collect(Collectors.toSet());
            assertEquals(mkSet(dir0, dir1), files);
        } finally {
            directory.unlock(task0);
            directory.unlock(task1);
        }
    }


    @Test
    public void shouldCleanupStateDirectoriesWhenLastModifiedIsLessThanNowMinusCleanupDelay() {
        final File dir = directory.directoryForTask(new TaskId(2, 0));
        assertTrue(new File(dir, "store").mkdir());

        final int cleanupDelayMs = 60000;
        directory.cleanRemovedTasks(cleanupDelayMs);
        assertTrue(dir.exists());
        assertEquals(1, directory.listAllTaskDirectories().length);
        assertEquals(1, directory.listNonEmptyTaskDirectories().length);

        time.sleep(cleanupDelayMs + 1000);
        directory.cleanRemovedTasks(cleanupDelayMs);
        assertTrue(dir.exists());
        assertEquals(1, directory.listAllTaskDirectories().length);
        assertEquals(0, directory.listNonEmptyTaskDirectories().length);
    }

    @Test
    public void shouldCleanupObsoleteStateDirectoriesOnlyOnce() {
        final File dir = directory.directoryForTask(new TaskId(2, 0));
        assertTrue(new File(dir, "store").mkdir());
        assertEquals(1, directory.listAllTaskDirectories().length);
        assertEquals(1, directory.listNonEmptyTaskDirectories().length);

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            time.sleep(5000);
            directory.cleanRemovedTasks(0);
            assertTrue(dir.exists());
            assertEquals(1, directory.listAllTaskDirectories().length);
            assertEquals(0, directory.listNonEmptyTaskDirectories().length);
            assertThat(
                appender.getMessages(),
                hasItem(containsString("Deleting obsolete state directory"))
            );
        }

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            time.sleep(5000);
            directory.cleanRemovedTasks(0);
            assertTrue(dir.exists());
            assertEquals(1, directory.listAllTaskDirectories().length);
            assertEquals(0, directory.listNonEmptyTaskDirectories().length);
            assertThat(
                appender.getMessages(),
                not(hasItem(containsString("Deleting obsolete state directory")))
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
        initializeStateDirectory(false);
        assertTrue(Arrays.asList(directory.listAllTaskDirectories()).isEmpty());
    }

    @Test
    public void shouldReturnEmptyArrayIfStateDirDoesntExist() throws IOException {
        cleanup();
        assertFalse(stateDir.exists());
        assertTrue(Arrays.asList(directory.listAllTaskDirectories()).isEmpty());
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
            time, true);
        appDir = new File(stateDir, applicationId);

        // make sure the File#listFiles returns null and StateDirectory#listAllTaskDirectories is able to handle null
        Utils.delete(appDir);
        assertTrue(appDir.createNewFile());
        assertTrue(appDir.exists());
        assertNull(appDir.listFiles());
        assertEquals(0, directory.listAllTaskDirectories().length);
    }

    @Test
    public void shouldOnlyListNonEmptyTaskDirectories() throws IOException {
        TestUtils.tempDirectory(stateDir.toPath(), "foo");
        final File taskDir1 = directory.directoryForTask(new TaskId(0, 0));
        final File taskDir2 = directory.directoryForTask(new TaskId(0, 1));

        final File storeDir = new File(taskDir1, "store");
        assertTrue(storeDir.mkdir());

        assertEquals(mkSet(taskDir1, taskDir2), Arrays.stream(
            directory.listAllTaskDirectories()).collect(Collectors.toSet()));
        assertEquals(mkSet(taskDir1), Arrays.stream(
            directory.listNonEmptyTaskDirectories()).collect(Collectors.toSet()));

        Utils.delete(taskDir1, Collections.singletonList(new File(taskDir1, LOCK_FILE_NAME)));

        assertEquals(mkSet(taskDir1, taskDir2), Arrays.stream(
            directory.listAllTaskDirectories()).collect(Collectors.toSet()));
        assertEquals(Collections.emptySet(), Arrays.stream(
            directory.listNonEmptyTaskDirectories()).collect(Collectors.toSet()));
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
            time, true);
        final File taskDir = stateDirectory.directoryForTask(new TaskId(0, 0));
        assertTrue(stateDir.exists());
        assertTrue(taskDir.exists());
    }

    @Test
    public void shouldLockGlobalStateDirectory() throws IOException {
        try (
            final FileChannel channel = FileChannel.open(
                new File(directory.globalStateDir(), LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            directory.lockGlobalState();
            assertThrows(OverlappingFileLockException.class, channel::lock);
        } finally {
            directory.unlockGlobalState();
        }
    }

    @Test
    public void shouldUnlockGlobalStateDirectory() throws IOException {
        directory.lockGlobalState();
        directory.unlockGlobalState();

        try (
            final FileChannel channel = FileChannel.open(
                new File(directory.globalStateDir(), LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            // should lock without any exceptions
            channel.lock();
        }
    }

    @Test
    public void shouldNotLockStateDirLockedByAnotherThread() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final AtomicReference<IOException> exceptionOnThread = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                directory.lock(taskId);
            } catch (final IOException e) {
                exceptionOnThread.set(e);
            }
        });
        thread.start();
        thread.join(30000);
        assertNull("should not have had an exception during locking on other thread", exceptionOnThread.get());
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

        assertNull("should not have had an exception on other thread", exceptionOnThread.get());
        directory.unlock(taskId);
        assertFalse(directory.lock(taskId));

        unlockLatch.countDown();
        thread.join(30000);

        assertNull("should not have had an exception on other thread", exceptionOnThread.get());
        assertTrue(directory.lock(taskId));
    }

    @Test
    public void shouldCleanupAllTaskDirectoriesIncludingGlobalOne() {
        final TaskId id = new TaskId(1, 0);
        directory.directoryForTask(id);
        directory.globalStateDir();

        final File dir0 = new File(appDir, id.toString());
        final File globalDir = new File(appDir, "global");
        assertEquals(mkSet(dir0, globalDir), Arrays.stream(
            Objects.requireNonNull(appDir.listFiles())).collect(Collectors.toSet()));

        directory.clean();

        assertEquals(Collections.emptySet(), Arrays.stream(
            Objects.requireNonNull(appDir.listFiles())).collect(Collectors.toSet()));
    }

    @Test
    public void shouldNotCreateBaseDirectory() throws IOException {
        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            initializeStateDirectory(false);
            assertThat(stateDir.exists(), is(false));
            assertThat(appDir.exists(), is(false));
            assertThat(appender.getMessages(),
                not(hasItem(containsString("Error changing permissions for the state or base directory"))));
        }
    }

    @Test
    public void shouldNotCreateTaskStateDirectory() throws IOException {
        initializeStateDirectory(false);
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);
        assertFalse(taskDirectory.exists());
    }

    @Test
    public void shouldNotCreateGlobalStateDirectory() throws IOException {
        initializeStateDirectory(false);
        final File globalStateDir = directory.globalStateDir();
        assertFalse(globalStateDir.exists());
    }

    @Test
    public void shouldLockTaskStateDirectoryWhenDirectoryCreationDisabled() throws IOException {
        initializeStateDirectory(false);
        final TaskId taskId = new TaskId(0, 0);
        assertTrue(directory.lock(taskId));
    }

    @Test
    public void shouldLockGlobalStateDirectoryWhenDirectoryCreationDisabled() throws IOException {
        initializeStateDirectory(false);
        assertTrue(directory.lockGlobalState());
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
    public void shouldLogManualUserCallMessage() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);
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
        final File taskDirectory = directory.directoryForTask(taskId);
        final File testFile = new File(taskDirectory, "testFile");
        assertThat(testFile.mkdir(), is(true));
        assertThat(directory.directoryForTaskIsEmpty(taskId), is(false));

        try (final LogCaptureAppender appender = LogCaptureAppender.createAndRegister(StateDirectory.class)) {
            final long cleanupDelayMs = 0;
            time.sleep(5000);
            directory.cleanRemovedTasks(cleanupDelayMs);
            assertThat(appender.getMessages(), hasItem(endsWith("ms has elapsed (cleanup delay is " +  cleanupDelayMs + "ms).")));
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
                taskDirectory = directory.directoryForTask(taskId);
            } catch (final ProcessorStateException error) {
                passed.set(false);
            }
        }
    }
}