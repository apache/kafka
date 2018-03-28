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
package org.apache.kafka.tools;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class VerifiableConsumerMainTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testCreateFromArgs() throws Exception {
        String encoding = "BIG5";
        File f = createPropertiesFile(encoding);
        String[] args = new String[]{
            "--topic", "testCreateBasicPlatform",
            "--max-messages", "10",
            "--group-id", "testCreateBasicPlatform",
            "--broker-list", "localhost:9092",
            "--consumer.config", f.getAbsolutePath()
        };
        try (VerifiableConsumer vc = VerifiableConsumer.createFromArgs(VerifiableConsumer.argParser(), args)) {
            StringDeserializer value = getField(getField(vc, "consumer", KafkaConsumer.class),
                    "keyDeserializer", StringDeserializer.class);
            assertEquals(encoding, getField(value, "encoding", String.class));
        }
    }

    private static <T> T getField(Object obj, String name, Class<T> type) throws Exception {
        Field field = obj.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return type.cast(field.get(obj));
    }

    private static File createPropertiesFile(String encoding) throws Exception {
        File f = File.createTempFile("VerifiableConsumerMainTest", null);
        Properties props = new Properties();
        props.put("key.deserializer.encoding", encoding);
        try (FileOutputStream output = new FileOutputStream(f)) {
            props.store(output, null);
        }
        return f;
    }
}
