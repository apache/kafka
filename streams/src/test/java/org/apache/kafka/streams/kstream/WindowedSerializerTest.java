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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class WindowedSerializerTest {
    @Test
    public void testWindowedSerializerNoArgConstructors() {
        Map<String, String> props = new HashMap<>();
        // test key[value].serializer.inner.class takes precedence over serializer.inner.class
        WindowedSerializer<StringSerializer> windowedSerializer = new WindowedSerializer<>();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:1");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "appId");
        props.put("key.serializer.inner.class", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("serializer.inner.class", "org.apache.kafka.common.serialization.StringSerializer");
        windowedSerializer.configure(props, true);
        Serializer<?> inner = windowedSerializer.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner);
        assertTrue("Inner serializer type should be StringSerializer", inner instanceof StringSerializer);
        // test serializer.inner.class
        props.put("serializer.inner.class", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.remove("key.serializer.inner.class");
        props.remove("value.serializer.inner.class");
        WindowedSerializer<?> windowedSerializer1 = new WindowedSerializer<>();
        windowedSerializer1.configure(props, false);
        Serializer<?> inner1 = windowedSerializer1.innerSerializer();
        assertNotNull("Inner serializer should be not null", inner1);
        assertTrue("Inner serializer type should be ByteArraySerializer", inner1 instanceof ByteArraySerializer);
    }
}
