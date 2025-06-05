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
import org.apache.kafka.common.security.oauthbearer.JwtValidatorException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredJws;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

/**
 * {@code CachedFile} goes a little beyond the basic file caching mechanism by allowing the file to be "transformed"
 * into an in-memory representation of the file contents for easier use by the caller.
 *
 * @param <T> Type of the "transformed" file contents
 */
public class CachedFile<T> {

    /**
     * Function object that provides as arguments the file and its contents and returns the in-memory representation
     * of the file contents.
     */
    public interface Transformer<T> {

        /**
         * Transforms the raw contents into a (possibly) different representation.
         *
         * @param file     File containing the source data
         * @param contents Data from file; could be zero length but not {@code null}
         */
        T transform(File file, String contents);
    }

    /**
     * Function object that provides as arguments the file and its metadata and returns a flag to determine if the
     * file should be reloaded from disk.
     */
    public interface RefreshPolicy<T> {

        /**
         * Given the {@link File} and its snapshot, determine if the file should be reloaded from disk.
         */
        boolean shouldRefresh(File file, Snapshot<T> snapshot);

        /**
         * This cache refresh policy only loads the file once.
         */
        static <T> RefreshPolicy<T> staticPolicy() {
            return (file, snapshot) -> snapshot == null;
        }

        /**
         * This policy will refresh the cached file if the snapshot's time is older than the current timestamp.
         */
        static <T> RefreshPolicy<T> lastModifiedPolicy() {
            return (file, snapshot) -> {
                if (snapshot == null)
                    return true;

                return file.lastModified() != snapshot.lastModified();
            };
        }
    }

    /**
     * No-op transformer that retains the exact file contents as a string.
     */
    public static final Transformer<String> STRING_NOOP_TRANSFORMER = (file, contents) -> contents;

    /**
     * This transformer really only validates that the given file contents represent a properly-formed JWT.
     * If not, a {@link OAuthBearerIllegalTokenException} or {@link JwtValidatorException} is thrown.
     */
    public static final Transformer<String> STRING_JSON_VALIDATING_TRANSFORMER = (file, contents) -> {
        contents = contents.trim();
        SerializedJwt serializedJwt = new SerializedJwt(contents);
        OAuthBearerUnsecuredJws.toMap(serializedJwt.getHeader());
        OAuthBearerUnsecuredJws.toMap(serializedJwt.getPayload());
        return contents;
    };

    private final File file;
    private final Transformer<T> transformer;
    private final RefreshPolicy<T> cacheRefreshPolicy;
    private Snapshot<T> snapshot;

    public CachedFile(File file, Transformer<T> transformer, RefreshPolicy<T> cacheRefreshPolicy) {
        this.file = file;
        this.transformer = transformer;
        this.cacheRefreshPolicy = cacheRefreshPolicy;
        this.snapshot = snapshot();
    }

    public long size() {
        return snapshot().size();
    }

    public long lastModified() {
        return snapshot().lastModified();
    }

    public String contents() {
        return snapshot().contents();
    }

    public T transformed() {
        return snapshot().transformed();
    }

    private Snapshot<T> snapshot() {
        if (cacheRefreshPolicy.shouldRefresh(file, snapshot)) {
            long size = file.length();
            long lastModified = file.lastModified();
            String contents;

            try {
                contents = Files.readString(file.toPath());
            } catch (IOException e) {
                throw new KafkaException("Error reading the file contents of OAuth resource " + file.getPath() + " for caching");
            }

            T transformed = transformer.transform(file, contents);
            snapshot = new Snapshot<>(size, lastModified, contents, transformed);
        }

        return snapshot;
    }

    public static class Snapshot<T> {

        private final long size;

        private final long lastModified;

        private final String contents;

        private final T transformed;

        public Snapshot(long size, long lastModified, String contents, T transformed) {
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
