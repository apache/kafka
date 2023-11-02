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
package org.apache.kafka.streams.processor.internals;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ProcessorMetadataTest {

    @Test
    public void shouldAddandGetKeyValueWithEmptyConstructor() {
        final ProcessorMetadata metadata = new ProcessorMetadata();
        final String key = "some_key";
        final long value = 100L;

        metadata.put(key, value);
        final Long actualValue =  metadata.get(key);

        assertThat(actualValue, is(value));

        final Long noValue = metadata.get("no_key");
        assertThat(noValue, is(nullValue()));
    }

    @Test
    public void shouldAddandGetKeyValueWithExistingMeta() {
        final Map<String, Long> map = new HashMap<>();
        map.put("key1", 1L);
        map.put("key2", 2L);

        final ProcessorMetadata metadata = new ProcessorMetadata(map);

        final long value1 = metadata.get("key1");
        assertThat(value1, is(1L));

        final long value2 = metadata.get("key2");
        assertThat(value2, is(2L));

        final Long noValue = metadata.get("key3");
        assertThat(noValue, is(nullValue()));

        metadata.put("key3", 3L);
        final long value3 = metadata.get("key3");
        assertThat(value3, is(3L));
    }

    @Test
    public void shouldSerializeAndDeserialize() {
        final ProcessorMetadata metadata = new ProcessorMetadata();
        final String key1 = "key1", key2 = "key2", key3 = "key3";
        final long value1 = 1L, value2 = 2L, value3 = 3L;

        metadata.put(key1, value1);
        metadata.put(key2, value2);
        metadata.put(key3, value3);

        final byte[] serialized = metadata.serialize();
        final ProcessorMetadata deserialized = ProcessorMetadata.deserialize(serialized);

        assertThat(deserialized.get(key1), is(value1));
        assertThat(deserialized.get(key2), is(value2));
        assertThat(deserialized.get(key3), is(value3));
    }

    @Test
    public void shouldDeserializeNull() {
        final ProcessorMetadata deserialized = ProcessorMetadata.deserialize(null);
        assertThat(deserialized, is(new ProcessorMetadata()));
    }

    @Test
    public void shouldUpdate() {
        final ProcessorMetadata emptyMeta = new ProcessorMetadata();
        emptyMeta.update(null);

        assertThat(emptyMeta, is(new ProcessorMetadata()));

        {
            final Map<String, Long> map1 = new HashMap<>();
            map1.put("key1", 1L);
            map1.put("key2", 2L);
            final ProcessorMetadata metadata1 = new ProcessorMetadata(map1);
            emptyMeta.update(metadata1);
            assertThat(emptyMeta.get("key1"), is(1L));
            assertThat(emptyMeta.get("key2"), is(2L));
        }

        {
            final Map<String, Long> map1 = new HashMap<>();
            map1.put("key1", 0L);
            map1.put("key2", 1L);
            final ProcessorMetadata metadata1 = new ProcessorMetadata(map1);
            emptyMeta.update(metadata1);
            assertThat(emptyMeta.get("key1"), is(1L));
            assertThat(emptyMeta.get("key2"), is(2L));
        }

        {
            final Map<String, Long> map1 = new HashMap<>();
            map1.put("key1", 2L);
            map1.put("key2", 3L);
            final ProcessorMetadata metadata1 = new ProcessorMetadata(map1);
            emptyMeta.update(metadata1);
            assertThat(emptyMeta.get("key1"), is(2L));
            assertThat(emptyMeta.get("key2"), is(3L));
        }
    }

    @Test
    public void shouldUpdateCommitFlag() {
        final ProcessorMetadata emptyMeta = new ProcessorMetadata();
        assertFalse(emptyMeta.needsCommit());

        emptyMeta.setNeedsCommit(true);
        assertTrue(emptyMeta.needsCommit());

        emptyMeta.setNeedsCommit(false);
        assertFalse(emptyMeta.needsCommit());

        emptyMeta.put("key1", 1L);
        assertTrue(emptyMeta.needsCommit());

        final Map<String, Long> map1 = new HashMap<>();
        map1.put("key1", 2L);
        map1.put("key2", 3L);
        final ProcessorMetadata metadata1 = new ProcessorMetadata(map1);
        emptyMeta.update(metadata1);
        assertTrue(emptyMeta.needsCommit());
    }

    @Test
    public void shouldNotUseCommitFlagForHashcodeAndEquals() {
        final ProcessorMetadata metadata1 = new ProcessorMetadata();
        metadata1.setNeedsCommit(true);
        final ProcessorMetadata metadata2 = new ProcessorMetadata();
        metadata2.setNeedsCommit(false);

        assertEquals(metadata1, metadata2);
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
    }
}
