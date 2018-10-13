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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.util.SafeObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of OffsetBackingStore that saves data locally to a file. To ensure this behaves
 * similarly to a real backing store, operations are executed asynchronously on a background thread.
 */
public class FileOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(FileOffsetBackingStore.class);

    private File file;

    public FileOffsetBackingStore() {

    }

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        file = new File(config.getString(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG));
    }

    @Override
    public synchronized void start() {
        super.start();
        log.info("Starting FileOffsetBackingStore with file {}", file);
        load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        // Nothing to do since this doesn't maintain any outstanding connections/data
        log.info("Stopped FileOffsetBackingStore");
    }

    @SuppressWarnings("unchecked")
    private void load() {
        try (SafeObjectInputStream is = new SafeObjectInputStream(Files.newInputStream(file.toPath()))) {
            Object obj = is.readObject();
            if (!(obj instanceof HashMap))
                throw new ConnectException("Expected HashMap but found " + obj.getClass());
            Map<byte[], byte[]> raw = (Map<byte[], byte[]>) obj;
            data = new HashMap<>();
            for (Map.Entry<byte[], byte[]> mapEntry : raw.entrySet()) {
                ByteBuffer key = (mapEntry.getKey() != null) ? ByteBuffer.wrap(mapEntry.getKey()) : null;
                ByteBuffer value = (mapEntry.getValue() != null) ? ByteBuffer.wrap(mapEntry.getValue()) : null;
                data.put(key, value);
            }
        } catch (NoSuchFileException | EOFException e) {
            // NoSuchFileException: Ignore, may be new.
            // EOFException: Ignore, this means the file was missing or corrupt
        } catch (IOException | ClassNotFoundException e) {
            throw new ConnectException(e);
        }
    }

    @Override
    protected void save() {
        try (ObjectOutputStream os = new ObjectOutputStream(Files.newOutputStream(file.toPath()))) {
            Map<byte[], byte[]> raw = new HashMap<>();
            for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
                byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                raw.put(key, value);
            }
            os.writeObject(raw);
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
