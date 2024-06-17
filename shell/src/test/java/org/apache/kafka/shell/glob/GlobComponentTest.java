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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

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
}
