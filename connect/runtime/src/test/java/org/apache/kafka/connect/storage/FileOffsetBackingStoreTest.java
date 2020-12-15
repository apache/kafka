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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.Callback;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class FileOffsetBackingStoreTest {

    FileOffsetBackingStore store;
    Map<String, String> props;
    StandaloneConfig config;
    File tempFile;

    private static Map<ByteBuffer, ByteBuffer> firstSet = new HashMap<>();
    private static final Runnable EMPTY_RUNNABLE = () -> {
    };

    static {
        firstSet.put(buffer("key"), buffer("value"));
        firstSet.put(null, null);
    }

    @SuppressWarnings("deprecation")
    @Before
    public void setup() throws IOException {
        store = new FileOffsetBackingStore();
        tempFile = File.createTempFile("fileoffsetbackingstore", null);
        props = new HashMap<String, String>();
        props.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, tempFile.getAbsolutePath());
        props.put(StandaloneConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(StandaloneConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        config = new StandaloneConfig(props);
        store.configure(config);
        store.start();
    }

    @After
    public void teardown() {
        tempFile.delete();
    }

    @Test
    public void testGetSet() throws Exception {
        Callback<Void> setCallback = expectSuccessfulSetCallback();
        PowerMock.replayAll();

        store.set(firstSet, setCallback).get();

        Map<ByteBuffer, ByteBuffer> values = store.get(Arrays.asList(buffer("key"), buffer("bad"))).get();
        assertEquals(buffer("value"), values.get(buffer("key")));
        assertEquals(null, values.get(buffer("bad")));

        PowerMock.verifyAll();
    }

    @Test
    public void testSaveRestore() throws Exception {
        Callback<Void> setCallback = expectSuccessfulSetCallback();
        PowerMock.replayAll();

        store.set(firstSet, setCallback).get();
        store.stop();

        // Restore into a new store to ensure correct reload from scratch
        FileOffsetBackingStore restore = new FileOffsetBackingStore();
        restore.configure(config);
        restore.start();
        Map<ByteBuffer, ByteBuffer> values = restore.get(Arrays.asList(buffer("key"))).get();
        assertEquals(buffer("value"), values.get(buffer("key")));

        PowerMock.verifyAll();
    }

    @Test
    public void testThreadName() {
        assertTrue(((ThreadPoolExecutor) store.executor).getThreadFactory()
                .newThread(EMPTY_RUNNABLE).getName().startsWith(FileOffsetBackingStore.class.getSimpleName()));
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

    private Callback<Void> expectSuccessfulSetCallback() {
        @SuppressWarnings("unchecked")
        Callback<Void> setCallback = PowerMock.createMock(Callback.class);
        setCallback.onCompletion(EasyMock.isNull(Throwable.class), EasyMock.isNull(Void.class));
        PowerMock.expectLastCall();
        return setCallback;
    }
}
