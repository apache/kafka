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

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.internals.AllowedPaths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AllowedPathsTest {

    private AllowedPaths allowedPaths;
    @TempDir
    private File parent;
    private String dir;
    private String myFile;
    private String dir2;

    @BeforeEach
    public void setup() throws IOException {
        dir = Files.createDirectory(Paths.get(parent.toString(), "dir")).toString();
        myFile = Files.createFile(Paths.get(dir, "myFile")).toString();
        dir2 = Files.createDirectory(Paths.get(parent.toString(), "dir2")).toString();
    }

    @Test
    public void testAllowedPath() {
        allowedPaths = new AllowedPaths(String.join(",", dir, dir2));

        Path actual = allowedPaths.parseUntrustedPath(myFile);
        assertEquals(myFile, actual.toString());
    }

    @Test
    public void testNotAllowedPath() {
        allowedPaths = new AllowedPaths(dir);

        Path actual = allowedPaths.parseUntrustedPath(dir2);
        assertNull(actual);
    }

    @Test
    public void testNullAllowedPaths() {
        allowedPaths = new AllowedPaths(null);
        Path actual = allowedPaths.parseUntrustedPath(myFile);
        assertEquals(myFile, actual.toString());
    }

    @Test
    public void testNoTraversal() {
        allowedPaths = new AllowedPaths(dir);

        Path traversedPath = Paths.get(dir, "..", "dir2");
        Path actual = allowedPaths.parseUntrustedPath(traversedPath.toString());
        assertNull(actual);
    }

    @Test
    public void testAllowedTraversal() {
        allowedPaths = new AllowedPaths(String.join(",", dir, dir2));

        Path traversedPath = Paths.get(dir, "..", "dir2");
        Path actual = allowedPaths.parseUntrustedPath(traversedPath.toString());
        assertEquals(traversedPath.normalize(), actual);
    }

    @Test
    public void testNullAllowedPathsTraversal() {
        allowedPaths = new AllowedPaths("");
        Path traversedPath = Paths.get(dir, "..", "dir2");
        Path actual = allowedPaths.parseUntrustedPath(traversedPath.toString());
        // we expect non-normalised path if allowed.paths is not specified to avoid backward compatibility
        assertEquals(traversedPath, actual);
    }

    @Test
    public void testAllowedPathDoesNotExist() {
        Exception e = assertThrows(ConfigException.class, () -> new AllowedPaths("/foo"));
        assertEquals("Path /foo does not exist", e.getMessage());
    }

    @Test
    public void testAllowedPathIsNotAbsolute() {
        Exception e = assertThrows(ConfigException.class, () -> new AllowedPaths("foo bar "));
        assertEquals("Path foo bar  is not absolute", e.getMessage());
    }
}
