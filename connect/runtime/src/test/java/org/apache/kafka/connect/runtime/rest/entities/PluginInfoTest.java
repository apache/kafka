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
package org.apache.kafka.connect.runtime.rest.entities;

import org.apache.kafka.connect.runtime.isolation.PluginDesc;
import org.apache.kafka.connect.runtime.isolation.PluginType;
import org.apache.kafka.connect.tools.MockSinkConnector;
import org.apache.kafka.connect.tools.MockSourceConnector;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PluginInfoTest {

    @Test
    public void testNoVersionFilter() {
        PluginInfo.NoVersionFilter filter = new PluginInfo.NoVersionFilter();
        // We intentionally refrain from using assertEquals and assertNotEquals
        // here to ensure that the filter's equals() method is used
        assertFalse(filter.equals("1.0"));
        assertFalse(filter.equals(new Object()));
        assertFalse(filter.equals(null));
        assertTrue(filter.equals(PluginDesc.UNDEFINED_VERSION));
    }

    @Test
    public void testPluginInfoJsonSerialization() throws Exception {
        ClassLoader classLoader = PluginInfoTest.class.getClassLoader();
        PluginInfo sinkInfo = new PluginInfo(
            new PluginDesc<>(MockSinkConnector.class, "1.0.0", PluginType.SINK, classLoader)
        );
        PluginInfo sourceInfo = new PluginInfo(
            new PluginDesc<>(MockSourceConnector.class, "2.0.0", PluginType.SOURCE, classLoader)
        );

        final ObjectMapper objectMapper = new ObjectMapper();

        // Serialize to JSON
        String serializedSink = objectMapper.writeValueAsString(sinkInfo);
        String serializedSource = objectMapper.writeValueAsString(sourceInfo);

        // Verify type field is lowercase in JSON
        assertTrue(serializedSink.contains("\"type\":\"sink\""),
            "Expected type to be lowercase 'sink' but got: " + serializedSink);
        assertTrue(serializedSource.contains("\"type\":\"source\""),
            "Expected type to be lowercase 'source' but got: " + serializedSource);

        // Deserialize back and verify
        PluginInfo deserializedSink = objectMapper.readValue(serializedSink, PluginInfo.class);
        PluginInfo deserializedSource = objectMapper.readValue(serializedSource, PluginInfo.class);

        assertEquals(PluginType.SINK, deserializedSink.type());
        assertEquals(PluginType.SOURCE, deserializedSource.type());
    }
}
