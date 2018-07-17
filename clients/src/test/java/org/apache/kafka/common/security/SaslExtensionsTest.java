package org.apache.kafka.common.security;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SaslExtensionsTest {
    @Test
    public void testToStringConvertsMapCorrectly() {
        Map<String, String> extensionsMap = new HashMap<>();
        extensionsMap.put("what", "42");
        extensionsMap.put("who", "me");
        List<String> correctRepresentations = Arrays.asList("what=42,who=me", "who=me,what=42");

        SaslExtensions extensions = new SaslExtensions(extensionsMap);
        String stringRepresentation = extensions.toString();

        // HashMap does not necessarily preserve ordering, so accept both orders
        assertTrue(correctRepresentations.contains(stringRepresentation));
    }

    @Test
    public void testExtensionNamesReturnsAllNames() {
        Set<String> expectedNames = new HashSet<>(Arrays.asList("what", "who"));

        SaslExtensions extensions = new SaslExtensions("what=42,who=me");
        Set<String> receivedNames = extensions.extensionNames();

        assertEquals(receivedNames, expectedNames);
    }

    @Test
    public void testReturnsExtensionValueByName() {
        SaslExtensions extensions = new SaslExtensions("what=42,who=me");


        assertEquals("42", extensions.extensionValue("what"));
        assertEquals("me", extensions.extensionValue("who"));
    }
}
