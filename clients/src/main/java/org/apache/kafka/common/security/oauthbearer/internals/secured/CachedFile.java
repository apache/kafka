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
package org.apache.kafka.common.security.oauthbearer.internals.secured;

import org.apache.kafka.common.KafkaException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class CachedFile<T> {

    public static final Function<String, String> STRING_TRANSFORMER = s -> s;

    private final Path path;

    private final Function<String, T> transformer;

    private final ReadWriteLock lock;

    private CachedFileInfo cachedFile;

    public CachedFile(Path path, Function<String, T> transformer) {
        this.path = path;
        this.transformer = transformer;
        this.lock = new ReentrantReadWriteLock();
    }

    public long size() {
        return cachedFileInfo().size;
    }

    public long lastModified() {
        return cachedFileInfo().lastModified;
    }

    public String contents() {
        return cachedFileInfo().contents;
    }

    public T transformed() {
        return cachedFileInfo().transformed;
    }

    private CachedFileInfo cachedFileInfo() {
        try {
            lock.readLock().lock();

            if (cachedFile == null) {
                try {
                    // Downgrade lock and double check that
                    lock.readLock().unlock();
                    lock.writeLock().lock();

                    if (cachedFile == null) {
                        File file = path.toFile();
                        long size = file.length();
                        long lastModified = file.lastModified();
                        String contents;

                        try {
                            contents = Files.readString(path);
                        } catch (IOException e) {
                            throw new KafkaException("Error reading the file contents of OAuth resource " + path + " for caching");
                        }

                        T transformed = transformer.apply(contents);
                        cachedFile = new CachedFileInfo(size, lastModified, contents, transformed);
                    }
                } finally {
                    lock.readLock().lock();
                    lock.writeLock().unlock();
                }
            }

            return cachedFile;
        } finally {
            lock.readLock().unlock();
        }
    }

    private final class CachedFileInfo {

        private final long size;

        private final long lastModified;

        private final String contents;

        private final T transformed;

        private CachedFileInfo(long size, long lastModified, String contents, T transformed) {
            this.size = size;
            this.lastModified = lastModified;
            this.contents = contents;
            this.transformed = transformed;
        }
    }
}
