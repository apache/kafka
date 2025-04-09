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
package org.apache.kafka.common.security.oauthbearer;

import org.apache.kafka.common.KafkaException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.BiFunction;

public class CachedFile<T> {

    public static final BiFunction<File, String, String> NOOP_TRANSFORMER = (file, contents) -> contents;

    protected final File file;
    protected final BiFunction<File, String, T> transformer;
    protected final BiFunction<File, CachedFileInfo<T>, Boolean> cacheRefreshPolicy;
    protected CachedFileInfo<T> cachedFileInfo;

    public CachedFile(File file,
                      BiFunction<File, String, T> transformer,
                      BiFunction<File, CachedFileInfo<T>, Boolean> cacheRefreshPolicy) {
        this.file = file;
        this.transformer = transformer;
        this.cacheRefreshPolicy = cacheRefreshPolicy;
    }

    public static <T> BiFunction<File, CachedFileInfo<T>, Boolean> staticCacheRefreshPolicy() {
        return (file, cachedFileInfo) -> cachedFileInfo == null;
    }

    public static <T> BiFunction<File, CachedFileInfo<T>, Boolean> lastModifiedCacheRefreshPolicy() {
        return (file, cachedFileInfo) -> cachedFileInfo == null || cachedFileInfo.lastModified() != file.lastModified();
    }

    public long size() {
        return cachedFileInfo().size();
    }

    public long lastModified() {
        return cachedFileInfo().lastModified();
    }

    public String contents() {
        return cachedFileInfo().contents();
    }

    public T transformed() {
        return cachedFileInfo().transformed();
    }

    protected CachedFileInfo<T> cachedFileInfo() {
        if (cacheRefreshPolicy.apply(file, cachedFileInfo)) {
            long size = file.length();
            long lastModified = file.lastModified();
            String contents;

            try {
                contents = Files.readString(file.toPath());
            } catch (IOException e) {
                throw new KafkaException("Error reading the file contents of OAuth resource " + file.getPath() + " for caching");
            }

            T transformed = transformer.apply(file, contents);
            cachedFileInfo = new CachedFileInfo<>(size, lastModified, contents, transformed);
        }

        return cachedFileInfo;
    }

    public static class CachedFileInfo<T> {

        private final long size;

        private final long lastModified;

        private final String contents;

        private final T transformed;

        public CachedFileInfo(long size, long lastModified, String contents, T transformed) {
            this.size = size;
            this.lastModified = lastModified;
            this.contents = contents;
            this.transformed = transformed;
        }

        public long size() {
            return size;
        }

        public long lastModified() {
            return lastModified;
        }

        public String contents() {
            return contents;
        }

        public T transformed() {
            return transformed;
        }
    }
}
