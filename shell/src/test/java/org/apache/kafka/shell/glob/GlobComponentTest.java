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

package org.apache.kafka.shell.glob;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120)
public class GlobComponentTest {
    private void verifyIsLiteral(GlobComponent globComponent, String component) {
        assertTrue(globComponent.literal());
        assertEquals(component, globComponent.component());
        assertTrue(globComponent.matches(component));
        assertFalse(globComponent.matches(component + "foo"));
    }

    @Test
    public void testLiteralComponent() {
        verifyIsLiteral(new GlobComponent("abc"), "abc");
        verifyIsLiteral(new GlobComponent(""), "");
        verifyIsLiteral(new GlobComponent("foobar_123"), "foobar_123");
        verifyIsLiteral(new GlobComponent("$blah+"), "$blah+");
    }

    @Test
    public void testToRegularExpression() {
        assertNull(GlobComponent.toRegularExpression("blah"));
        assertNull(GlobComponent.toRegularExpression(""));
        assertNull(GlobComponent.toRegularExpression("does not need a regex, actually"));
        assertEquals("^\\$blah.*$", GlobComponent.toRegularExpression("$blah*"));
        assertEquals("^.*$", GlobComponent.toRegularExpression("*"));
        assertEquals("^foo(?:(?:bar)|(?:baz))$", GlobComponent.toRegularExpression("foo{bar,baz}"));
    }

    @Test
    public void testGlobMatch() {
        GlobComponent star = new GlobComponent("*");
        assertFalse(star.literal());
        assertTrue(star.matches(""));
        assertTrue(star.matches("anything"));
        GlobComponent question = new GlobComponent("b?b");
        assertFalse(question.literal());
        assertFalse(question.matches(""));
        assertTrue(question.matches("bob"));
        assertTrue(question.matches("bib"));
        assertFalse(question.matches("bic"));
        GlobComponent foobarOrFoobaz = new GlobComponent("foo{bar,baz}");
        assertFalse(foobarOrFoobaz.literal());
        assertTrue(foobarOrFoobaz.matches("foobar"));
        assertTrue(foobarOrFoobaz.matches("foobaz"));
        assertFalse(foobarOrFoobaz.matches("foobah"));
        assertFalse(foobarOrFoobaz.matches("foo"));
        assertFalse(foobarOrFoobaz.matches("baz"));
    }

    @Test
    public void testCharacterRangeToRegularExpression() {
        // Basic character range patterns
        assertEquals("^[a-z]$", GlobComponent.toRegularExpression("[a-z]"));
        assertEquals("^[0-9]$", GlobComponent.toRegularExpression("[0-9]"));
        assertEquals("^[abc]$", GlobComponent.toRegularExpression("[abc]"));
        assertEquals("^file[0-9]$", GlobComponent.toRegularExpression("file[0-9]"));
        assertEquals("^[a-zA-Z]$", GlobComponent.toRegularExpression("[a-zA-Z]"));

        // Negation patterns
        assertEquals("^[^a-z]$", GlobComponent.toRegularExpression("[!a-z]"));
        assertEquals("^[^abc]$", GlobComponent.toRegularExpression("[^abc]"));

        // Literal ] as first character
        assertEquals("^[]abc]$", GlobComponent.toRegularExpression("[]abc]"));
        assertEquals("^[^]abc]$", GlobComponent.toRegularExpression("[!]abc]"));
        assertEquals("^[^]abc]$", GlobComponent.toRegularExpression("[^]abc]"));

        // Hyphen variants
        assertEquals("^[a-c-e]$", GlobComponent.toRegularExpression("[a-c-e]"));
        assertEquals("^[abc-]$", GlobComponent.toRegularExpression("[abc-]"));
        assertEquals("^[-abc]$", GlobComponent.toRegularExpression("[-abc]"));

        // Special regex characters inside brackets should be literal
        assertEquals("^[.*+?|(){}$^]$", GlobComponent.toRegularExpression("[.*+?|(){}$^]"));
    }

    @Test
    public void testCharacterRangeMatch() {
        // Test basic range [a-z]
        GlobComponent lowerRange = new GlobComponent("[a-z]");
        assertFalse(lowerRange.literal());
        assertTrue(lowerRange.matches("a"));
        assertTrue(lowerRange.matches("m"));
        assertTrue(lowerRange.matches("z"));
        assertFalse(lowerRange.matches("A"));
        assertFalse(lowerRange.matches("5"));
        assertFalse(lowerRange.matches("ab"));

        // Test numeric range [0-9]
        GlobComponent digitRange = new GlobComponent("[0-9]");
        assertFalse(digitRange.literal());
        assertTrue(digitRange.matches("0"));
        assertTrue(digitRange.matches("5"));
        assertTrue(digitRange.matches("9"));
        assertFalse(digitRange.matches("a"));
        assertFalse(digitRange.matches("10"));

        // Test individual characters [abc]
        GlobComponent charSet = new GlobComponent("[abc]");
        assertFalse(charSet.literal());
        assertTrue(charSet.matches("a"));
        assertTrue(charSet.matches("b"));
        assertTrue(charSet.matches("c"));
        assertFalse(charSet.matches("d"));
        assertFalse(charSet.matches("ab"));

        // Test literal ] as first character
        GlobComponent firstBracket = new GlobComponent("[]abc]");
        assertTrue(firstBracket.matches("]"));
        assertTrue(firstBracket.matches("a"));
        assertFalse(firstBracket.matches("d"));

        // Test pattern with prefix: file[0-3]
        GlobComponent filePattern = new GlobComponent("file[0-3]");
        assertFalse(filePattern.literal());
        assertTrue(filePattern.matches("file0"));
        assertTrue(filePattern.matches("file1"));
        assertTrue(filePattern.matches("file3"));
        assertFalse(filePattern.matches("file4"));
        assertFalse(filePattern.matches("file"));

        // Test pattern with suffix: [a-c].txt
        GlobComponent suffixPattern = new GlobComponent("[a-c].txt");
        assertFalse(suffixPattern.literal());
        assertTrue(suffixPattern.matches("a.txt"));
        assertTrue(suffixPattern.matches("b.txt"));
        assertFalse(suffixPattern.matches("d.txt"));
        assertFalse(suffixPattern.matches("ab.txt"));
        
        // Test in middle: foo[a-c]bar
        GlobComponent middlePattern = new GlobComponent("foo[a-c]bar");
        assertTrue(middlePattern.matches("fooabar"));
        assertTrue(middlePattern.matches("foocbar"));
        assertFalse(middlePattern.matches("foobar"));
    }

    @Test
    public void testCharacterRangeNegation() {
        // Test negation with !
        GlobComponent notLower = new GlobComponent("[!a-z]");
        assertFalse(notLower.literal());
        assertTrue(notLower.matches("A"));
        assertTrue(notLower.matches("5"));
        assertFalse(notLower.matches("a"));
        assertFalse(notLower.matches("z"));

        // Test negation with ^
        GlobComponent notDigit = new GlobComponent("[^0-9]");
        assertFalse(notDigit.literal());
        assertTrue(notDigit.matches("a"));
        assertTrue(notDigit.matches("Z"));
        assertFalse(notDigit.matches("0"));
        assertFalse(notDigit.matches("9"));
        
        // Test negation with literal ]
        GlobComponent notClosing = new GlobComponent("[!]a-c]");
        assertTrue(notClosing.matches("x"));
        assertFalse(notClosing.matches("]"));
        assertFalse(notClosing.matches("a"));
    }

    @Test
    public void testCharacterRangeEscaping() {
        // Test escaped hyphen
        GlobComponent escapedHyphen = new GlobComponent("[a\\-c]");
        assertTrue(escapedHyphen.matches("a"));
        assertTrue(escapedHyphen.matches("-"));
        assertTrue(escapedHyphen.matches("c"));
        assertFalse(escapedHyphen.matches("b"));

        // Test escaped brackets
        GlobComponent escapedBrackets = new GlobComponent("[\\[\\]]");
        assertTrue(escapedBrackets.matches("["));
        assertTrue(escapedBrackets.matches("]"));
        assertFalse(escapedBrackets.matches("a"));
    }

    @Test
    public void testCharacterRangeCombinedWithOtherPatterns() {
        // Combine character range with wildcard
        GlobComponent combined = new GlobComponent("[a-z]*");
        assertFalse(combined.literal());
        assertTrue(combined.matches("a"));
        assertTrue(combined.matches("abc"));
        assertTrue(combined.matches("z123"));
        assertFalse(combined.matches("123"));
        assertFalse(combined.matches("Abc"));

        // Combine character range with question mark
        GlobComponent questionCombined = new GlobComponent("[a-z]?[0-9]");
        assertFalse(questionCombined.literal());
        assertTrue(questionCombined.matches("aX5"));
        assertTrue(questionCombined.matches("z99"));
        assertTrue(questionCombined.matches("ab5"));
        assertTrue(questionCombined.matches("a55"));
        assertFalse(questionCombined.matches("a5"));
        assertFalse(questionCombined.matches("abc5"));

        // Multiple ranges
        GlobComponent multipleRanges = new GlobComponent("[a-z][0-9]");
        assertTrue(multipleRanges.matches("a5"));
        assertTrue(multipleRanges.matches("z0"));
        assertFalse(multipleRanges.matches("aa"));
    }

    @Test
    public void testUnterminatedCharacterRange() {
        // Unterminated character range should result in literal matching (exception caught)
        GlobComponent unterminated = new GlobComponent("[a-z");
        assertTrue(unterminated.literal());
        assertTrue(unterminated.matches("[a-z"));
        assertFalse(unterminated.matches("a"));

        GlobComponent unterminatedNegated = new GlobComponent("[!");
        assertTrue(unterminatedNegated.literal());
        assertTrue(unterminatedNegated.matches("[!"));
    }
}
