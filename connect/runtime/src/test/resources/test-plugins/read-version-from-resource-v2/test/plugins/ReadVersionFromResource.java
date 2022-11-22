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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Enumeration;
import java.net.URL;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

/**
 * Fake plugin class for testing classloading isolation.
 * See {@link org.apache.kafka.connect.runtime.isolation.TestPlugins}.
 * <p>Load resource(s) from the isolated classloader instance.
 * Exfiltrates data via {@link ReadVersionFromResource#fromConnectData(String, Schema, Object)}
 * and {@link ReadVersionFromResource#toConnectData(String, byte[])}.
 */
public class ReadVersionFromResource implements Converter {
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    private String version(InputStream stream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            return reader.lines()
                    .filter(s -> !s.isEmpty() && !s.startsWith("#"))
                    .collect(Collectors.toList())
                    .get(0);
        }
    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        try (InputStream stream = this.getClass().getResourceAsStream("/version")) {
            return version(stream).getBytes(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        try {
            Enumeration<URL> e = this.getClass().getClassLoader().getResources("version");
            ArrayList<String> versions = new ArrayList<>();
            while (e.hasMoreElements()) {
                try (InputStream stream = e.nextElement().openStream()) {
                    versions.add(version(stream));
                }
            }
            return new SchemaAndValue(null, versions);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}