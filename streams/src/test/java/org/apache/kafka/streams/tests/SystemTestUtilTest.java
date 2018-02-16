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

package org.apache.kafka.streams.tests;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;

public class SystemTestUtilTest {

    private Map<String, String> expectedParsedMap = new TreeMap<>();

    @Before
    public void setUp(){
        expectedParsedMap.put("foo", "foo1");
        expectedParsedMap.put("bar", "bar1");
        expectedParsedMap.put("baz", "baz1");
    }

    @Test
    public void shouldParseCorrectMap() {
        String formattedConfigs = "foo=foo1,bar=bar1,baz=baz1";
        Map<String,String> parsedMap = SystemTestUtil.parseConfigs(formattedConfigs);
        TreeMap<String, String> sortedParsedMap = new TreeMap<>(parsedMap);
        assertEquals(sortedParsedMap, expectedParsedMap);
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowExceptionOnNull() {
        SystemTestUtil.parseConfigs(null);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionIfNotCorrectKeyValueSeparator(){
        String badString = "foo:bar,baz:boo";
        SystemTestUtil.parseConfigs(badString);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionIfNotCorrectKeyValuePairSeparator() {
        String badString = "foo=bar;baz=boo";
        SystemTestUtil.parseConfigs(badString);
    }

    @Test
    public void shouldParseSingleKeyValuePairString() {
        Map<String, String> expectedSinglePairMap = new HashMap<>();
        expectedSinglePairMap.put("foo", "bar");
        String singleValueString ="foo=bar";
        Map<String, String> parsedMap = SystemTestUtil.parseConfigs(singleValueString);
        assertEquals(expectedSinglePairMap, parsedMap);
    }



}