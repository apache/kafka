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

package test.plugins;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.isolation.SamplingTestPlugin;
import org.apache.kafka.connect.runtime.isolation.TestPlugins;
import org.apache.kafka.connect.storage.Converter;

/**
 * Fake plugin class for testing classloading isolation.
 * See {@link org.apache.kafka.connect.runtime.isolation.TestPlugins}.
 * <p>Exfiltrates data via {@link ThingTwo#fromConnectData(String, Schema, Object)}.
 */
public class ThingTwo implements Converter {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        return "Thing two".getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        return null;
    }
}
