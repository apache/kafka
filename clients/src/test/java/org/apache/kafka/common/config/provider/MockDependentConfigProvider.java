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
package org.apache.kafka.common.config.provider;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockDependentConfigProvider extends FileConfigProvider {

    private static final Map<String, MockDependentConfigProvider> INSTANCES = Collections.synchronizedMap(new HashMap<>());
    private String key;
    private boolean closed = false;

    public void configure(Map<String, ?> configs) {
        Object key = configs.get("secret.key");
        if (key == null) {
            throw new RuntimeException(getClass().getName() + " missing 'secret.key' config");
        }
        // The value is expected to match the 'password' property in the MockFileConfigProvider
        if (!key.equals("randomPassword")) {
            throw new RuntimeException(getClass().getName() + " expected 'secret.key=randomPassword', but found 'secret.key=" + key + "'");
        }
        if (this.key != null) {
            throw new RuntimeException(getClass().getName() + " instance was configured twice");
        }
        this.key = key.toString();
        INSTANCES.put(key.toString(), this);
    }

    @Override
    protected Reader reader(String path) throws IOException {
        return new StringReader("app.key=appKey\napp.password=appPassword");
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    public static void assertClosed(String id) {
        MockDependentConfigProvider instance = INSTANCES.remove(id);
        assertNotNull(instance);
        synchronized (instance) {
            assertTrue(instance.closed);
        }
    }
}
