/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of OffsetBackingStore that saves data locally to a file. To ensure this behaves
 * similarly to a real backing store, operations are executed asynchronously on a background thread.
 */
public class FileOffsetBackingStore extends MemoryOffsetBackingStore {
    private static final Logger log = LoggerFactory.getLogger(FileOffsetBackingStore.class);

    public final static String OFFSET_STORAGE_FILE_FILENAME_CONFIG = "offset.storage.file.filename";
    private File file;

    public FileOffsetBackingStore() {

    }

    @Override
    public void configure(Map<String, ?> props) {
        super.configure(props);
        String filename = (String) props.get(OFFSET_STORAGE_FILE_FILENAME_CONFIG);
        file = new File(filename);
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
        try {
            ObjectInputStream is = new ObjectInputStream(new FileInputStream(file));
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
            is.close();
        } catch (FileNotFoundException | EOFException e) {
            // FileNotFoundException: Ignore, may be new.
            // EOFException: Ignore, this means the file was missing or corrupt
        } catch (IOException | ClassNotFoundException e) {
            throw new ConnectException(e);
        }
    }

    protected void save() {
        try {
            ObjectOutputStream os = new ObjectOutputStream(new FileOutputStream(file));
            Map<byte[], byte[]> raw = new HashMap<>();
            for (Map.Entry<ByteBuffer, ByteBuffer> mapEntry : data.entrySet()) {
                byte[] key = (mapEntry.getKey() != null) ? mapEntry.getKey().array() : null;
                byte[] value = (mapEntry.getValue() != null) ? mapEntry.getValue().array() : null;
                raw.put(key, value);
            }
            os.writeObject(raw);
            os.close();
        } catch (IOException e) {
            throw new ConnectException(e);
        }
    }
}
