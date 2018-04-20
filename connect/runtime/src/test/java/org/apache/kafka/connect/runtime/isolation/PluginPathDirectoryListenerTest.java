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
package org.apache.kafka.connect.runtime.isolation;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class PluginPathDirectoryListenerTest {

    // How long to wait for every plugin creation to be registered before failing the test, in ms
    private static final long CREATE_WAIT_TIMEOUT = 15000;

    // How long to wait for every plugin deletion to be registered before failing the test, in ms
    private static final long DELETE_WAIT_TIMEOUT = 15000;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testPluginDirectoryListener() throws IOException, InterruptedException {
        File testPluginDirectory = folder.newFolder("test-plugin-directory");

        Path pluginDir1 = Paths.get(testPluginDirectory.toString(), "pluginDir1");
        Path pluginDir2 = Paths.get(testPluginDirectory.toString(), "pluginDir2");
        Path pluginDir3 = Paths.get(testPluginDirectory.toString(), "pluginDir3");
        Path pluginDir4 = Paths.get(testPluginDirectory.toString(), "pluginDir4");

        Path pluginJar1 = Paths.get(testPluginDirectory.toString(), "pluginJar1.jar");
        Path pluginJar2 = Paths.get(testPluginDirectory.toString(), "pluginJar2.jar");
        Path pluginJar3 = Paths.get(testPluginDirectory.toString(), "pluginJar3.jar");
        Path pluginJar4 = Paths.get(testPluginDirectory.toString(), "pluginJar4.jar");

        Path pluginZip1 = Paths.get(testPluginDirectory.toString(), "pluginZip1.zip");
        Path pluginZip2 = Paths.get(testPluginDirectory.toString(), "pluginZip2.zip");
        Path pluginZip3 = Paths.get(testPluginDirectory.toString(), "pluginZip3.zip");
        Path pluginZip4 = Paths.get(testPluginDirectory.toString(), "pluginZip4.zip");

        Path irrelevantFile = Paths.get(testPluginDirectory.toString(), "irrelevantFile");

        createDirectory(pluginDir4);
        createFile(pluginJar4);
        createFile(pluginZip4);

        final Set<Path> expectedDirCreates = new HashSet<>(Arrays.asList(
                pluginDir1, pluginDir2, pluginDir3
        ));
        final Set<Path> expectedFileCreates = new HashSet<>(Arrays.asList(
                pluginJar1, pluginJar2, pluginJar3, pluginZip1, pluginZip2, pluginZip3
        ));

        final Set<Path> expectedCreates = new HashSet<>();
        expectedCreates.addAll(expectedDirCreates);
        expectedCreates.addAll(expectedFileCreates);

        final Set<Path> expectedDeletes = new HashSet<>(Arrays.asList(
                pluginDir2, pluginDir4,
                pluginJar2, pluginJar4,
                pluginZip2, pluginZip4
        ));

        final Set<Path> actualCreates = new HashSet<>();
        final Set<Path> actualDeletes = new HashSet<>();

        PluginPathDirectoryListener.PluginReceiver pluginReceiver = new PluginPathDirectoryListener.PluginReceiver() {
            @Override
            public void onCreate(Path pluginLocation) {
                actualCreates.add(pluginLocation);
                if (actualCreates.equals(expectedCreates)) {
                    synchronized (expectedCreates) {
                        expectedCreates.notify();
                    }
                }
            }

            @Override
            public void onDelete(Path pluginLocation) {
                actualDeletes.add(pluginLocation);
                if (actualDeletes.equals(expectedDeletes)) {
                    synchronized (expectedDeletes) {
                        expectedDeletes.notify();
                    }
                }
            }
        };

        PluginPathDirectoryListener testListener =
                new PluginPathDirectoryListener(pluginReceiver, Collections.singleton(testPluginDirectory.toPath()));
        new Thread(testListener).start();

        for (Path directory : expectedDirCreates) {
            createDirectory(directory);
        }
        for (Path file : expectedFileCreates) {
            createFile(file);
        }
        createFile(irrelevantFile);
        synchronized (expectedCreates) {
            expectedCreates.wait(CREATE_WAIT_TIMEOUT);
        }
        assertEquals(
                "Not all expected plugin directory creates were observed during testing",
                expectedCreates,
                actualCreates
        );

        for (Path directory : expectedDeletes) {
            deleteFileOrDirectory(directory);
        }
        deleteFileOrDirectory(irrelevantFile);
        synchronized (expectedDeletes) {
            expectedDeletes.wait(DELETE_WAIT_TIMEOUT);
        }
        assertEquals(
                "Not all expected plugin directory deletes were observed during testing",
                expectedDeletes,
                actualDeletes
        );
    }

    private void createFile(Path path) throws IOException {
        if (!path.toFile().createNewFile()) {
            fail("Failed to create test file " + path.toString());
        }
    }

    private void createDirectory(Path path) {
        if (!path.toFile().mkdir()) {
            fail("Failed to create test directory " + path.toString());
        }
    }

    private void deleteFileOrDirectory(Path path) {
        if (!path.toFile().delete()) {
            fail("Failed to delete test directory " + path.toString());
        }
    }
}
