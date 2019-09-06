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

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.runtime.isolation.DelegatingClassLoader;
import org.apache.kafka.connect.runtime.isolation.PluginClassLoader;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verify that a PluginClassLoader is being used to load this class at all times.
 */
public class ExpectPluginClassLoader implements Converter {

    private static final Logger log = LoggerFactory.getLogger(ExpectPluginClassLoader.class);

    private static final ClassLoader staticClassloader;

    static {
        log.info("Starting static initialization");
        staticClassloader = Thread.currentThread().getContextClassLoader();
        log.info("Finished static initialization");
    }

    {
        log.info("Started initialization");
        log.debug("Classloader: {}", staticClassloader);
        Assert.assertFalse(staticClassloader instanceof DelegatingClassLoader);
        Assert.assertTrue(staticClassloader instanceof PluginClassLoader);
        Assert.assertEquals(staticClassloader, Thread.currentThread().getContextClassLoader());
        Assert.assertEquals(staticClassloader, ExpectPluginClassLoader.class.getClassLoader());
        log.info("Finished initialization");
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    @Override
    public byte[] fromConnectData(final String topic, final Schema schema, final Object value) {
        return new byte[0];
    }

    @Override
    public SchemaAndValue toConnectData(final String topic, final byte[] value) {
        return null;
    }
}
