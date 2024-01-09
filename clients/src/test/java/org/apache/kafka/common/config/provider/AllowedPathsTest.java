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
package org.apache.kafka.common.config.provider;

import org.apache.kafka.common.config.internals.AllowedPaths;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AllowedPathsTest {

    private AllowedPaths allowedPaths;
    private Path dir;
    private Path myFile;
    private Path dir2;

    @BeforeEach
    public void setup() throws IOException {
        File parent = TestUtils.tempDirectory();
        dir = Files.createDirectory(Paths.get(parent.getAbsolutePath(), "dir"));
        myFile = Files.createFile(Paths.get(dir.toString(), "myFile"));
        dir2 = Files.createDirectory(Paths.get(parent.getAbsolutePath(), "dir2"));
    }

    @Test
    public void testAllowedPath() {
        allowedPaths = AllowedPaths.configureAllowedPaths(String.join(",", dir.toString(), dir2.toString()));

        Path actual = allowedPaths.getIfPathIsAllowed(myFile);
        assertEquals(myFile, actual);
    }

    @Test
    public void testNotAllowedPath() {
        allowedPaths = AllowedPaths.configureAllowedPaths(dir.toString());

        Path actual = allowedPaths.getIfPathIsAllowed(dir2);
        assertNull(actual);
    }

    @Test
    public void testNullAllowedPaths() {
        allowedPaths = AllowedPaths.configureAllowedPaths(null);
        Path actual = allowedPaths.getIfPathIsAllowed(myFile);
        assertEquals(myFile, actual);
    }

    @Test
    public void testNoTraversal() {
        allowedPaths = AllowedPaths.configureAllowedPaths(dir.toString());

        Path traversedPath = Paths.get(dir.toString(), "..", "dir2");
        Path actual = allowedPaths.getIfPathIsAllowed(traversedPath);
        assertNull(actual);
    }

    @Test
    public void testAllowedTraversal() {
        allowedPaths = AllowedPaths.configureAllowedPaths(String.join(",", dir.toString(), dir2.toString()));

        Path traversedPath = Paths.get(dir.toString(), "..", "dir2");
        Path actual = allowedPaths.getIfPathIsAllowed(traversedPath);
        assertEquals(traversedPath.normalize(), actual);
    }

    @Test
    public void testNullAllowedPathsTraversal() {
        allowedPaths = AllowedPaths.configureAllowedPaths("");
        Path traversedPath = Paths.get(dir.toString(), "..", "dir2");
        Path actual = allowedPaths.getIfPathIsAllowed(traversedPath);
        // we expect non-normalised path if allowed.paths is not specified to avoid backward compatibility
        assertEquals(traversedPath, actual);
    }
}
