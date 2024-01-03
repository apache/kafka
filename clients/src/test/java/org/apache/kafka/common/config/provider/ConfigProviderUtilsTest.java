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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;


import static org.apache.kafka.common.config.internals.ConfigProviderUtils.configureAllowedPaths;
import static org.apache.kafka.common.config.internals.ConfigProviderUtils.pathIsAllowed;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class ConfigProviderUtilsTest {

    @Test
    public void testAllowedPath() {
        List<Path> allowedPaths = new ArrayList<>();
        allowedPaths.add(Paths.get("dir"));
        allowedPaths.add(Paths.get("dir2"));

        Path path = Paths.get("dir", "myFile");
        Path actual = pathIsAllowed(path, allowedPaths);
        assertEquals(path, actual);
    }

    @Test
    public void testNotAllowedPath() {
        List<Path> allowedPaths = new ArrayList<>();
        allowedPaths.add(Paths.get("dir"));

        Path path = Paths.get("dir2", "myFile");
        Path actual = pathIsAllowed(path, allowedPaths);
        assertNull(actual);
    }

    @Test
    public void testNullAllowedPaths() {
        Path path = Paths.get("dir", "myFile");
        Path actual = pathIsAllowed(path, null);
        assertEquals(path, actual);
    }

    @Test
    public void testNoTraversal() {
        List<Path> allowedPaths = new ArrayList<>();
        allowedPaths.add(Paths.get("dir"));

        Path path = Paths.get("dir", "..", "dir2");
        Path actual = pathIsAllowed(path, allowedPaths);
        assertNull(actual);
    }

    @Test
    public void testAllowedTraversal() {
        List<Path> allowedPaths = new ArrayList<>();
        allowedPaths.add(Paths.get("dir"));
        allowedPaths.add(Paths.get("dir2"));

        Path path = Paths.get("dir", "..", "dir2");
        Path actual = pathIsAllowed(path, allowedPaths);
        assertEquals(path.normalize(), actual);
    }

    @Test
    public void testNullAllowedPathsTraversal() {
        Path path = Paths.get("dir", "..", "dir2");
        Path actual = pathIsAllowed(path, null);
        // we expect normalised path only if allowed.paths is specified to avoid backward compatibility
        assertEquals(path, actual);
    }

    @Test
    public void testConfigureAllowedPaths() {
        List<Path> actual = configureAllowedPaths("/dir,/dir2/subdir");
        List<Path> expected = new ArrayList<>();
        expected.add(Paths.get("/dir"));
        expected.add(Paths.get("/dir2", "subdir"));
        assertEquals(expected, actual);
    }


    @Test
    public void testConfigureAllowedPathsEmpty() {
        List<Path> actual = configureAllowedPaths("");
        assertNull(actual);
    }
}
