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

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StateDirectoryTest {

    private final MockTime time = new MockTime();
    private File stateDir;
    private String applicationId = "applicationId";
    private StateDirectory directory;
    private File appDir;

    @Before
    public void before() {
        stateDir = new File(TestUtils.IO_TMP_DIR, TestUtils.randomString(5));
        directory = new StateDirectory(
            new StreamsConfig(new Properties() {
                {
                    put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
                    put(StreamsConfig.STATE_DIR_CONFIG, stateDir.getPath());
                }
            }),
            time);
        appDir = new File(stateDir, applicationId);
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
    public void shouldCreateTaskStateDirectory() {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);
        assertTrue(taskDirectory.exists());
        assertTrue(taskDirectory.isDirectory());
    }

    @Test
    public void shouldLockTaskStateDirectory() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.stateDir();

        directory.lock(taskId);

        File f = new File(taskDirectory, taskId + StateDirectory.LOCK_FILE_NAME);
        try (
            final FileChannel channel = FileChannel.open(
                f.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE)
        ) {
            FileLock lock = channel.tryLock();
            System.out.println("l ocked " + f + " " + lock);
            fail("shouldn't be able to lock already locked directory");
        } catch (final OverlappingFileLockException e) {
            // pass
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

    @Test(expected = ProcessorStateException.class)
    public void shouldThrowProcessorStateException() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);
        directory.directoryForTask(taskId);
    }

    @Test
    public void shouldNotLockDeletedDirectory() throws IOException {
        final TaskId taskId = new TaskId(0, 0);

        Utils.delete(stateDir);
        assertFalse(directory.lock(taskId));
    }
    
    @Test
    public void shouldLockMulitpleTaskDirectories() throws IOException {
        final TaskId taskId = new TaskId(0, 0);
        final File task1Dir = directory.stateDir();
        final TaskId taskId2 = new TaskId(1, 0);
        final File task2Dir = directory.stateDir();


        try (
            final FileChannel channel1 = FileChannel.open(
                new File(task1Dir, taskId + StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
            final FileChannel channel2 = FileChannel.open(
                new File(task2Dir, taskId2 + StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            directory.lock(taskId);
            directory.lock(taskId2);

            channel1.tryLock();
            channel2.tryLock();
            fail("shouldn't be able to lock already locked directory");
        } catch (final OverlappingFileLockException e) {
            // pass
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
                new File(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(),
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
        try {
            directory.lock(task0);
            directory.lock(task1);
            directory.directoryForTask(new TaskId(2, 0));

            List<File> files = Arrays.asList(appDir.listFiles());
            assertEquals(3, files.size());

            time.sleep(1000);
            directory.cleanRemovedTasks(0);

            files = Arrays.asList(appDir.listFiles());
            assertEquals(2, files.size());
            assertTrue(files.contains(new File(appDir, task0.toString())));
            assertTrue(files.contains(new File(appDir, task1.toString())));
        } finally {
            directory.unlock(task0);
            directory.unlock(task1);
        }
    }

    @Test
    public void shouldCleanupStateDirectoriesWhenLastModifiedIsLessThanNowMinusCleanupDelay() {
        final File dir = directory.directoryForTask(new TaskId(2, 0));
        final int cleanupDelayMs = 60000;
        directory.cleanRemovedTasks(cleanupDelayMs);
        assertTrue(dir.exists());

        time.sleep(cleanupDelayMs + 1000);
        directory.cleanRemovedTasks(cleanupDelayMs);
        assertFalse(dir.exists());
    }

    @Test
    public void shouldNotRemoveNonTaskDirectoriesAndFiles() {
        final File otherDir = TestUtils.tempDirectory(stateDir.toPath(), "foo");
        directory.cleanRemovedTasks(0);
        assertTrue(otherDir.exists());
    }

    @Test
    public void shouldListAllTaskDirectories() {
        TestUtils.tempDirectory(stateDir.toPath(), "foo");
        final File taskDir1 = directory.directoryForTask(new TaskId(0, 0));
        final File taskDir2 = directory.directoryForTask(new TaskId(0, 1));

        final List<File> dirs = Arrays.asList(directory.listTaskDirectories());
        assertEquals(2, dirs.size());
        assertTrue(dirs.contains(taskDir1));
        assertTrue(dirs.contains(taskDir2));
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
            time);
        final File taskDir = stateDirectory.directoryForTask(new TaskId(0, 0));
        assertTrue(stateDir.exists());
        assertTrue(taskDir.exists());
    }

    @Test(expected = OverlappingFileLockException.class)
    public void shouldLockGlobalStateDirectory() throws IOException {
        directory.lockGlobalState();

        try (
            final FileChannel channel = FileChannel.open(
                new File(directory.globalStateDir(), StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            channel.lock();
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
                new File(directory.globalStateDir(), StateDirectory.LOCK_FILE_NAME).toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
        ) {
            // should lock without any exceptions
            channel.lock();
        }
    }

    @Test
    public void shouldNotLockStateDirLockedByAnotherThread() throws IOException, InterruptedException {
        final TaskId taskId = new TaskId(0, 0);
        final AtomicReference<IOException> exceptionOnThread = new AtomicReference<>();
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    directory.lock(taskId);
                } catch (final IOException e) {
                    exceptionOnThread.set(e);
                }
            }
        });
        thread.start();
        thread.join(30000);
        assertNull("should not have had an exception during locking on other thread", exceptionOnThread.get());
        assertFalse(directory.lock(taskId));
    }

    @Test
    public void shouldNotUnLockStateDirLockedByAnotherThread() throws IOException, InterruptedException {
        final TaskId taskId = new TaskId(0, 0);
        final CountDownLatch lockLatch = new CountDownLatch(1);
        final CountDownLatch unlockLatch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionOnThread = new AtomicReference<>();
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    directory.lock(taskId);
                    lockLatch.countDown();
                    unlockLatch.await();
                    directory.unlock(taskId);
                } catch (final Exception e) {
                    exceptionOnThread.set(e);
                }
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
        directory.directoryForTask(new TaskId(1, 0));
        directory.globalStateDir();

        List<File> files = Arrays.asList(appDir.listFiles());
        assertEquals(2, files.size());

        directory.clean();

        files = Arrays.asList(appDir.listFiles());
        assertEquals(0, files.size());
    }
}