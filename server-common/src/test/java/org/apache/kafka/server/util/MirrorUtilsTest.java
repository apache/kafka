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
package org.apache.kafka.server.util;

import com.google.re2j.Pattern;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MirrorUtilsTest {
    @Test
    public void testCompilePatternWithValidPattern() {
        Pattern pattern = MirrorUtils.compilePattern("test.*");
        assertFalse(pattern.matcher("foo").matches());
        assertTrue(pattern.matcher("test123").matches());
        assertTrue(pattern.matcher("testing").matches());
    }

    @Test
    public void testCompilePatternWithBlankString() {
        assertNull(MirrorUtils.compilePattern("   "));
    }

    @Test
    public void testCompilePatternWithNull() {
        assertNull(MirrorUtils.compilePattern(null));
    }

    @Test
    public void testCompilePatternWithEmptyString() {
        assertNull(MirrorUtils.compilePattern(""));
    }

    @Test
    public void testCompilePatternWithWhitespace() {
        Pattern pattern = MirrorUtils.compilePattern("  test.*  ");
        assertTrue(pattern.matcher("test123").matches());
    }

    @Test
    public void testCompilePatternListWithMultiplePatterns() {
        Pattern pattern = MirrorUtils.compilePatternList(Arrays.asList("topic1", "topic2"));
        assertTrue(pattern.matcher("topic1").matches());
        assertTrue(pattern.matcher("topic2").matches());
        assertFalse(pattern.matcher("topic3").matches());
    }

    @Test
    public void testCompilePatternListWithRegexPatterns() {
        Pattern pattern = MirrorUtils.compilePatternList(Arrays.asList("test.*", "prod.*"));
        assertTrue(pattern.matcher("test-topic").matches());
        assertTrue(pattern.matcher("prod-topic").matches());
        assertFalse(pattern.matcher("staging-topic").matches());
    }

    @Test
    public void testCompilePatternListWithEmptyList() {
        assertNull(MirrorUtils.compilePatternList(Collections.emptyList()));
    }

    @Test
    public void testCompilePatternListWithBlankStrings() {
        Pattern pattern = MirrorUtils.compilePatternList(Arrays.asList("  ", "", "topic1"));
        assertTrue(pattern.matcher("topic1").matches());
        assertFalse(pattern.matcher("topic2").matches());
    }

    @Test
    public void testCompilePatternListWithWhitespace() {
        Pattern pattern = MirrorUtils.compilePatternList(Arrays.asList("  topic1  ", "  topic2  "));
        assertTrue(pattern.matcher("topic1").matches());
        assertTrue(pattern.matcher("topic2").matches());
    }

    @Test
    public void testCompilePatternListSingleItem() {
        Pattern pattern = MirrorUtils.compilePatternList(Arrays.asList("single-topic"));
        assertTrue(pattern.matcher("single-topic").matches());
        assertFalse(pattern.matcher("other-topic").matches());
    }

    @Test
    public void testCompilePatternListMatchesWildcard() {
        Pattern pattern = MirrorUtils.compilePatternList(Arrays.asList(".*"));
        assertTrue(pattern.matcher("anything").matches());
        assertTrue(pattern.matcher("123").matches());
        assertTrue(pattern.matcher("").matches());
    }
}
