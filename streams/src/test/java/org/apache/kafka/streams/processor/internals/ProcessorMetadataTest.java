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

public class ProcessorMetadataTest {

    @Test
    public void shouldAddandGetKeyValueWithEmptyConstructor() {
        final ProcessorMetadata metadata = ProcessorMetadata.emptyMetadata();
        final String key = "some_key";
        final long value = 100L;

        metadata.addMetadata(key, value);
        final Long actualValue =  metadata.getMetadata(key);

        assertThat(actualValue, is(value));

        final Long noValue = metadata.getMetadata("no_key");
        assertThat(noValue, is(nullValue()));
    }

    @Test
    public void shouldAddandGetKeyValueWithExistingMeta() {
        final Map<String, Long> map = new HashMap<>();
        map.put("key1", 1L);
        map.put("key2", 2L);

        final ProcessorMetadata metadata = ProcessorMetadata.with(map);

        final long value1 = metadata.getMetadata("key1");
        assertThat(value1, is(1L));

        final long value2 = metadata.getMetadata("key2");
        assertThat(value2, is(2L));

        final Long noValue = metadata.getMetadata("key3");
        assertThat(noValue, is(nullValue()));

        metadata.addMetadata("key3", 3L);
        final long value3 = metadata.getMetadata("key3");
        assertThat(value3, is(3L));
    }

    @Test
    public void shouldSerializeAndDeserialize() {
        final ProcessorMetadata metadata = ProcessorMetadata.emptyMetadata();
        final String key1 = "key1", key2 = "key2", key3 = "key3";
        final long value1 = 1L, value2 = 2L, value3 = 3L;

        metadata.addMetadata(key1, value1);
        metadata.addMetadata(key2, value2);
        metadata.addMetadata(key3, value3);

        final byte[] serialized = metadata.serialize();
        final ProcessorMetadata deserialized = ProcessorMetadata.deserialize(serialized);

        assertThat(deserialized.getMetadata(key1), is(value1));
        assertThat(deserialized.getMetadata(key2), is(value2));
        assertThat(deserialized.getMetadata(key3), is(value3));
    }

    @Test
    public void shouldDeserializeNull() {
        final ProcessorMetadata deserialized = ProcessorMetadata.deserialize(null);
        assertThat(deserialized, is(ProcessorMetadata.emptyMetadata()));
    }

}
