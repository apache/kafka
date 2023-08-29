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
package org.apache.kafka.connect.transforms.field;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FieldPathNotationTest {
    final static String[] EMPTY_PATH = new String[] {};

    @Test
    void shouldBuildV1WithDotsAndBacktickPair() {
        // Given v1
        // When path contains dots, then single step path
        assertArrayEquals(
                new String[] {"foo.bar.baz"},
                new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V1).path());
        // When path contains backticks, then single step path
        assertArrayEquals(
                new String[] {"foo`bar`"},
                new SingleFieldPath("foo`bar`", FieldSyntaxVersion.V1).path());
        // When path contains dots and backticks, then single step path
        assertArrayEquals(
                new String[] {"foo.`bar.baz`"},
                new SingleFieldPath("foo.`bar.baz`", FieldSyntaxVersion.V1).path());
    }

    @Test
    void shouldBuildV2WithEmptyPath() {
        // Given v2
        // When path is empty
        // Then build a path with no steps
        assertArrayEquals(EMPTY_PATH, new SingleFieldPath("", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldBuildV2WithoutDots() {
        // Given v2
        // When path without dots
        // Then build a single step path
        assertArrayEquals(new String[] {"foobarbaz"}, new SingleFieldPath("foobarbaz", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldBuildV2WhenIncludesDots() {
        // Given v2 and fields without dots
        // When path includes dots
        // Then build a path with steps separated by dots
        assertArrayEquals(new String[] {"foo", "bar", "baz"}, new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldBuildV2WithoutWrappingBackticks() {
        // Given v2 and fields without dots
        // When backticks are not wrapping a field name
        // Then build a single step path including backticks
        assertArrayEquals(new String[] {"foo`bar`baz"}, new SingleFieldPath("foo`bar`baz", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldBuildV2WhenIncludesDotsAndBacktickPair() {
        // Given v2 and fields including dots
        // When backticks are wrapping a field name (i.e. withing edges or between dots)
        // Then build a path with steps separated by dots and not including backticks
        assertArrayEquals(new String[] {"foo.bar.baz"}, new SingleFieldPath("`foo.bar.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar.baz"}, new SingleFieldPath("foo.`bar.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo.bar", "baz"}, new SingleFieldPath("`foo.bar`.baz", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar", "baz"}, new SingleFieldPath("foo.`bar`.baz", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldBuildV2AndIgnoreBackticksThatAreNotWrapping() {
        // Given v2 and fields including dots and backticks
        // When backticks are wrapping a field name (i.e. withing edges or between dots)
        // Then build a path with steps separated by dots and including non-wrapping backticks
        assertArrayEquals(new String[] {"foo", "`bar.baz"}, new SingleFieldPath("foo.``bar.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar.baz`"}, new SingleFieldPath("foo.`bar.baz``", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "ba`r.baz"}, new SingleFieldPath("foo.`ba`r.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "ba`r", "baz"}, new SingleFieldPath("foo.ba`r.baz", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "`bar`", "baz"}, new SingleFieldPath("foo.``bar``.baz", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"`foo.bar.baz`"}, new SingleFieldPath("``foo.bar.baz``", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldBuildV2AndEscapeBackticks() {
        // Given v2 and fields including dots and backticks
        // When backticks are wrapping a field name (i.e. withing edges or between dots)
        // and wrapping backticks that are part of the field name are escaped with backslashes
        // Then build a path with steps separated by dots and including escaped and non-wrapping backticks
        assertArrayEquals(new String[] {"foo", "bar`.baz"}, new SingleFieldPath("foo.`bar\\`.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar.`baz"}, new SingleFieldPath("foo.`bar.`baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar`.`baz"}, new SingleFieldPath("foo.`bar\\`.`baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar\\`.\\`baz"}, new SingleFieldPath("foo.`bar\\\\`.\\`baz`", FieldSyntaxVersion.V2).path());
    }

    @Test
    void shouldFailV2WhenIncompleteBackticks() {
        // Given v2
        // When backticks are not closed and not escaped
        // Then it should fail
        ConfigException e0 = assertThrows(ConfigException.class,
                () -> new SingleFieldPath("`foo.bar.baz", FieldSyntaxVersion.V2));
        assertEquals(
                "Incomplete backtick pair in path: [`foo.bar.baz], consider adding a backslash before backtick at position 0 to escape it",
                e0.getMessage()
        );
        ConfigException e1 = assertThrows(ConfigException.class,
                () -> new SingleFieldPath("foo.`bar.baz", FieldSyntaxVersion.V2));
        assertEquals(
                "Incomplete backtick pair in path: [foo.`bar.baz], consider adding a backslash before backtick at position 4 to escape it",
                e1.getMessage()
        );
        ConfigException e2 = assertThrows(ConfigException.class,
                () -> new SingleFieldPath("foo.bar.`baz", FieldSyntaxVersion.V2));
        assertEquals(
                "Incomplete backtick pair in path: [foo.bar.`baz], consider adding a backslash before backtick at position 8 to escape it",
                e2.getMessage()
        );
        ConfigException e3 = assertThrows(ConfigException.class,
                () -> {
                    SingleFieldPath p = new SingleFieldPath("foo.bar.`baz\\`", FieldSyntaxVersion.V2);
                    System.out.println(Arrays.toString(p.path()));
                });
        assertEquals(
                "Incomplete backtick pair in path: [foo.bar.`baz\\`], consider adding a backslash before backtick at position 8 to escape it",
                e3.getMessage()
        );
    }
}