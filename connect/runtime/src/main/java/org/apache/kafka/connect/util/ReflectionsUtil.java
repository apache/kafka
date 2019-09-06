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
package org.apache.kafka.connect.util;

import org.reflections.vfs.Vfs;
import org.reflections.vfs.Vfs.Dir;
import org.reflections.vfs.Vfs.File;
import org.reflections.vfs.Vfs.UrlType;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * CLASSPATH on OSX contains .mar and .jnilib file extensions. Vfs used by Reflections does not recognize
 * urls with those extensions and log WARNs when scan them. Those WARNs can be eliminated by registering
 * URL types before using reflection.
 */
public class ReflectionsUtil {

    private static final String FILE_PROTOCOL = "file";
    private static final List<String> ENDINGS = Arrays.asList(".mar", ".jnilib", "*");

    public static void registerUrlTypes() {
        final List<UrlType> urlTypes = new LinkedList<>();
        urlTypes.add(new EmptyUrlType(ENDINGS));
        urlTypes.addAll(Arrays.asList(Vfs.DefaultUrlTypes.values()));
        Vfs.setDefaultURLTypes(urlTypes);
    }

    private static class EmptyUrlType implements UrlType {

        private final List<String> endings;

        private EmptyUrlType(final List<String> endings) {
            this.endings = endings;
        }

        @Override
        public boolean matches(URL url) {
            final String protocol = url.getProtocol();
            final String externalForm = url.toExternalForm();
            if (!protocol.equals(FILE_PROTOCOL)) {
                return false;
            }
            for (String ending : endings) {
                if (externalForm.endsWith(ending)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Dir createDir(final URL url) throws Exception {
            return emptyVfsDir(url);
        }

        private static Dir emptyVfsDir(final URL url) {
            return new Dir() {
                @Override
                public String getPath() {
                    return url.toExternalForm();
                }

                @Override
                public Iterable<File> getFiles() {
                    return Collections.emptyList();
                }

                @Override
                public void close() {

                }
            };
        }
    }
}