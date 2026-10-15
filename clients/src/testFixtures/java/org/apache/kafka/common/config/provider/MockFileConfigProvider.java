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
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockFileConfigProvider extends FileConfigProvider {

    private static final Map<String, MockFileConfigProvider> INSTANCES = Collections.synchronizedMap(new HashMap<>());
    private String id;
    private boolean closed = false;

    public void configure(Map<String, ?> configs) {
        super.configure(configs);

        Object id = configs.get("testId");
        if (id == null) {
            throw new RuntimeException(getClass().getName() + " missing 'testId' config");
        }
        if (this.id != null) {
            throw new RuntimeException(getClass().getName() + " instance was configured twice");
        }
        this.id = id.toString();
        INSTANCES.put(id.toString(), this);
    }

    @Override
    protected Reader reader(Path path) throws IOException {
        return new StringReader("key=testKey\npassword=randomPassword");
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    public static void assertClosed(String id) {
        MockFileConfigProvider instance = INSTANCES.remove(id);
        assertNotNull(instance);
        synchronized (instance) {
            assertTrue(instance.closed);
        }
    }
}
