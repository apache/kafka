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

package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CachedFileTest extends OAuthBearerTest {

    @Test
    public void testStaticPolicy() throws Exception {
        File tmpFile = tempFile("  foo  ");

        CachedFile.Transformer<String> transformer = (file, contents) -> contents.trim();
        CachedFile.RefreshPolicy<String> refreshPolicy = CachedFile.RefreshPolicy.staticPolicy();
        CachedFile<String> cachedFile = new CachedFile<>(tmpFile, transformer, refreshPolicy);

        assertEquals(cachedFile.lastModified(), tmpFile.lastModified());
        assertEquals(7, cachedFile.size());
        assertEquals("  foo  ", cachedFile.contents());
        assertEquals("foo", cachedFile.transformed());

        // Sleep for a bit to make sure our timestamp changes, then update the file.
        Utils.sleep(10);
        Files.writeString(tmpFile.toPath(), "  bar baz  ", StandardOpenOption.WRITE, StandardOpenOption.APPEND);

        assertNotEquals(cachedFile.lastModified(), tmpFile.lastModified());
        assertNotEquals(cachedFile.size(), tmpFile.length());
        assertEquals(7, cachedFile.size());
        assertEquals("  foo  ", cachedFile.contents());
        assertEquals("foo", cachedFile.transformed());
    }

    @Test
    public void testLastModifiedPolicy() throws Exception {
        File tmpFile = tempFile("  foo  ");

        CachedFile.Transformer<String> transformer = (file, contents) -> contents.trim();
        CachedFile.RefreshPolicy<String> refreshPolicy = CachedFile.RefreshPolicy.lastModifiedPolicy();
        CachedFile<String> cachedFile = new CachedFile<>(tmpFile, transformer, refreshPolicy);

        assertEquals(cachedFile.lastModified(), tmpFile.lastModified());
        assertEquals(7, cachedFile.size());
        assertEquals("  foo  ", cachedFile.contents());
        assertEquals("foo", cachedFile.transformed());

        // Sleep for a bit to make sure our timestamp changes, then update the file.
        Utils.sleep(10);
        Files.writeString(tmpFile.toPath(), "  bar baz  ", StandardOpenOption.WRITE, StandardOpenOption.APPEND);

        assertEquals(18, cachedFile.size());
        assertEquals("  foo    bar baz  ", cachedFile.contents());
        assertEquals("foo    bar baz", cachedFile.transformed());
    }

    @Test
    public void testFileDoesNotExist() throws IOException {
        File tmpFile = tempFile("  foo  ");

        CachedFile.RefreshPolicy<String> refreshPolicy = CachedFile.RefreshPolicy.lastModifiedPolicy();
        CachedFile<String> cachedFile = new CachedFile<>(tmpFile, CachedFile.STRING_NOOP_TRANSFORMER, refreshPolicy);

        // All is well...
        assertTrue(tmpFile.exists());
        assertDoesNotThrow(cachedFile::size);
        assertDoesNotThrow(cachedFile::lastModified);
        assertDoesNotThrow(cachedFile::contents);
        assertDoesNotThrow(cachedFile::transformed);

        // Delete the file and ensure that exceptions are thrown
        assertTrue(tmpFile.delete());
        Utils.sleep(50);

        assertFalse(tmpFile.exists());
        assertThrows(KafkaException.class, cachedFile::size);
        assertThrows(KafkaException.class, cachedFile::lastModified);
        assertThrows(KafkaException.class, cachedFile::contents);
        assertThrows(KafkaException.class, cachedFile::transformed);

        // "Restore" the file and make sure it's refreshed.
        Utils.sleep(10);
        Files.writeString(tmpFile.toPath(), "valid data!", StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);

        assertTrue(tmpFile.exists());
        assertDoesNotThrow(cachedFile::size);
        assertDoesNotThrow(cachedFile::lastModified);
        assertDoesNotThrow(cachedFile::contents);
        assertDoesNotThrow(cachedFile::transformed);
    }

    @Test
    public void testTransformerError() throws Exception {
        File tmpFile = tempFile("[\"foo\"]");

        @SuppressWarnings("unchecked")
        CachedFile.Transformer<List<String>> jsonTransformer = (file, json) -> {
            try {
                ObjectMapper mapper = new ObjectMapper();
                return (List<String>) mapper.readValue(json, List.class);
            } catch (Exception e) {
                throw new KafkaException(e);
            }
        };

        CachedFile.RefreshPolicy<List<String>> refreshPolicy = CachedFile.RefreshPolicy.lastModifiedPolicy();
        CachedFile<List<String>> cachedFile = new CachedFile<>(tmpFile, jsonTransformer, refreshPolicy);

        assertEquals(List.of("foo"), cachedFile.transformed());

        // Sleep then update the file with proper JSON.
        Utils.sleep(10);
        Files.writeString(tmpFile.toPath(), "[\"foo\", \"bar\", \"baz\"]", StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);

        assertEquals(List.of("foo", "bar", "baz"), cachedFile.transformed());
    }
}
