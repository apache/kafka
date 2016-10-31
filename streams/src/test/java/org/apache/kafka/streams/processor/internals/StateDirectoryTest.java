/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StateDirectoryTest {

    private File stateDir;
    private String applicationId = "applicationId";
    private StateDirectory directory;
    private File appDir;

    @Before
    public void before() {
        stateDir = new File(TestUtils.IO_TMP_DIR, TestUtils.randomString(5));
        directory = new StateDirectory(applicationId, stateDir.getPath());
        appDir = new File(stateDir, applicationId);
    }

    @After
    public void cleanup() {
        if (stateDir.exists()) {
            Utils.delete(stateDir);
        }
    }

    @Test
    public void shouldCreateBaseDirectory() throws Exception {
        assertTrue(stateDir.exists());
        assertTrue(stateDir.isDirectory());
        assertTrue(appDir.exists());
        assertTrue(appDir.isDirectory());
    }

    @Test
    public void shouldCreateTaskStateDirectory() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);
        assertTrue(taskDirectory.exists());
        assertTrue(taskDirectory.isDirectory());
    }

    @Test
    public void shouldLockTaskStateDirectory() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);

        directory.lock(taskId, 0);

        final FileChannel channel = FileChannel.open(new File(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            channel.tryLock();
            fail("shouldn't be able to lock already locked directory");
        } catch (OverlappingFileLockException e) {
            // pass
        }

    }

    @Test
    public void shouldBeTrueIfAlreadyHoldsLock() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        directory.directoryForTask(taskId);
        directory.lock(taskId, 0);
        assertTrue(directory.lock(taskId, 0));
    }


    @Test
    public void shouldLockMulitpleTaskDirectories() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final File task1Dir = directory.directoryForTask(taskId);
        final TaskId taskId2 = new TaskId(1, 0);
        final File task2Dir = directory.directoryForTask(taskId2);

        directory.lock(taskId, 0);
        directory.lock(taskId2, 0);

        final FileChannel channel1 = FileChannel.open(new File(task1Dir, StateDirectory.LOCK_FILE_NAME).toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        final FileChannel channel2 = FileChannel.open(new File(task2Dir, StateDirectory.LOCK_FILE_NAME).toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        try {
            channel1.tryLock();
            channel2.tryLock();
            fail("shouldn't be able to lock already locked directory");
        } catch (OverlappingFileLockException e) {
            // pass
        }
    }

    @Test
    public void shouldReleaseTaskStateDirectoryLock() throws Exception {
        final TaskId taskId = new TaskId(0, 0);
        final File taskDirectory = directory.directoryForTask(taskId);

        directory.lock(taskId, 1);
        directory.unlock(taskId);

        final FileChannel channel = FileChannel.open(new File(taskDirectory, StateDirectory.LOCK_FILE_NAME).toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        channel.tryLock();
    }

    @Test
    public void shouldCleanUpTaskStateDirectoriesThatAreNotCurrentlyLocked() throws Exception {
        final TaskId task0 = new TaskId(0, 0);
        final TaskId task1 = new TaskId(1, 0);
        directory.lock(task0, 0);
        directory.lock(task1, 0);
        directory.directoryForTask(new TaskId(2, 0));

        directory.cleanRemovedTasks();
        final List<File> files = Arrays.asList(appDir.listFiles());
        assertEquals(2, files.size());
        assertTrue(files.contains(new File(appDir, task0.toString())));
        assertTrue(files.contains(new File(appDir, task1.toString())));

    }

    @Test
    public void shouldNotRemoveNonTaskDirectoriesAndFiles() throws Exception {
        final File otherDir = TestUtils.tempDirectory(stateDir.toPath(), "foo");
        directory.cleanRemovedTasks();
        assertTrue(otherDir.exists());
    }

    @Test
    public void shouldListAllTaskDirectories() throws Exception {
        TestUtils.tempDirectory(stateDir.toPath(), "foo");
        final File taskDir1 = directory.directoryForTask(new TaskId(0, 0));
        final File taskDir2 = directory.directoryForTask(new TaskId(0, 1));

        final List<File> dirs = Arrays.asList(directory.listTaskDirectories());
        assertEquals(2, dirs.size());
        assertTrue(dirs.contains(taskDir1));
        assertTrue(dirs.contains(taskDir2));
    }

    @Test
    public void shouldCreateDirectoriesIfParentDoesntExist() throws Exception {
        final File tempDir = TestUtils.tempDirectory();
        final File stateDir = new File(new File(tempDir, "foo"), "state-dir");
        final StateDirectory stateDirectory = new StateDirectory(applicationId, stateDir.getPath());
        final File taskDir = stateDirectory.directoryForTask(new TaskId(0, 0));
        assertTrue(stateDir.exists());
        assertTrue(taskDir.exists());
    }

}