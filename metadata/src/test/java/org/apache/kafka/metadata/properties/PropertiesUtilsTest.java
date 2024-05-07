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

package org.apache.kafka.metadata.properties;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;

import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class PropertiesUtilsTest {
    @Test
    public void testReadPropertiesFile() throws IOException {
        File tempFile = TestUtils.tempFile();
        try {
            String testContent = "a=1\nb=2\n#a comment\n\nc=3\nd=";
            Files.write(tempFile.toPath(), testContent.getBytes());
            Properties props = PropertiesUtils.readPropertiesFile(tempFile.getAbsolutePath());
            assertEquals(4, props.size());
            assertEquals("1", props.get("a"));
            assertEquals("2", props.get("b"));
            assertEquals("3", props.get("c"));
            assertEquals("", props.get("d"));
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testWritePropertiesFile(boolean fsync) throws IOException {
        File tempFile = TestUtils.tempFile();
        try {
            Properties props = new Properties();
            props.setProperty("abc", "123");
            props.setProperty("def", "456");
            PropertiesUtils.writePropertiesFile(props, tempFile.getAbsolutePath(), fsync);
            Properties props2 = PropertiesUtils.readPropertiesFile(tempFile.getAbsolutePath());
            assertEquals(props, props2);
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }

    @Test
    public void loadRequiredIntProp() {
        Properties props = new Properties();
        props.setProperty("foo.bar", "123");
        assertEquals(123, PropertiesUtils.loadRequiredIntProp(props, "foo.bar"));
    }

    @Test
    public void loadMissingRequiredIntProp() {
        Properties props = new Properties();
        assertEquals("Failed to find foo.bar",
            assertThrows(RuntimeException.class,
                () -> PropertiesUtils.loadRequiredIntProp(props, "foo.bar")).
                    getMessage());
    }

    @Test
    public void loadNonIntegerRequiredIntProp() {
        Properties props = new Properties();
        props.setProperty("foo.bar", "b");
        assertEquals("Unable to read foo.bar as a base-10 number.",
            assertThrows(RuntimeException.class,
                () -> PropertiesUtils.loadRequiredIntProp(props, "foo.bar")).
                    getMessage());
    }
}
