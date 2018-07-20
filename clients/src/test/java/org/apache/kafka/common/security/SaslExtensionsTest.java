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
package org.apache.kafka.common.security;

import org.apache.kafka.common.security.auth.SaslExtensions;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SaslExtensionsTest {
    @Test
    public void testToStringConvertsMapCorrectly() {
        Map<String, String> extensionsMap = new LinkedHashMap<>();
        extensionsMap.put("what", "42");
        extensionsMap.put("who", "me");
        String expectedRepresentation = "what=42,who=me";

        SaslExtensions extensions = new SaslExtensions(extensionsMap, ",");
        String stringRepresentation = extensions.toString();

        assertEquals(expectedRepresentation, stringRepresentation);
    }

    @Test
    public void testExtensionNamesReturnsAllNames() {
        Set<String> expectedNames = new HashSet<>(Arrays.asList("what", "who"));

        SaslExtensions extensions = new SaslExtensions("what=42,who=me", ",");
        Set<String> receivedNames = extensions.extensionNames();

        assertEquals(receivedNames, expectedNames);
    }

    @Test
    public void testReturnsExtensionValueByName() {
        SaslExtensions extensions = new SaslExtensions("what=42,who=me", ",");


        assertEquals("42", extensions.extensionValue("what"));
        assertEquals("me", extensions.extensionValue("who"));
    }
}
