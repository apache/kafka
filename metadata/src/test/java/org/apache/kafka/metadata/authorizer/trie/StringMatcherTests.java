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

package org.apache.kafka.metadata.authorizer.trie;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StringMatcherTests {
    @Test
    public void getSegmentAndAdvance() {
        Matcher<String> underTest = new StringMatcher<>("HelloWorld", n -> false);

        assertEquals("HelloWorld", underTest.getFragment());
        Matcher<String> underTest2 = underTest.advance(1);
        assertNotEquals(underTest, underTest2);
        assertEquals("elloWorld", underTest2.getFragment());
        assertEquals("HelloWorld", underTest.getFragment());
        Matcher<String> underTest3 = underTest.advance(5);
        assertEquals("World", underTest3.getFragment());
        assertEquals("elloWorld", underTest2.getFragment());
        assertEquals("HelloWorld", underTest.getFragment());
        underTest3 = underTest2.advance(4);
        assertEquals("World", underTest3.getFragment());
        assertEquals("elloWorld", underTest2.getFragment());
        assertEquals("HelloWorld", underTest.getFragment());
    }
}
