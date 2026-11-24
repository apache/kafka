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
package org.apache.kafka.apicheck;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Builds a temp {@code .jar} with class-bytes and/or HTML entries — covers both jar shapes the
 * checker consumes: project/reference jars (class bytes) and javadoc jars (HTML).
 */
public final class TempJarBuilder {

    private final List<Entry> entries = new ArrayList<>();

    public static TempJarBuilder jar() {
        return new TempJarBuilder();
    }

    private TempJarBuilder() {}

    /** Add a class entry; jar path is derived from the binary name. */
    public TempJarBuilder addClass(String binaryName, byte[] bytes) {
        entries.add(new Entry(binaryName.replace('.', '/') + ".class", bytes));
        return this;
    }

    /** Convenience: build the bytes from a {@link AsmClassFactory.ClassBuilder} and add them. */
    public TempJarBuilder addClass(AsmClassFactory.ClassBuilder builder) {
        return addClass(builder.binaryName(), builder.toBytes());
    }

    /** Add a javadoc HTML entry under the given jar-relative path; body may be empty. */
    public TempJarBuilder addHtml(String entryPath, String body) {
        entries.add(new Entry(entryPath, body.getBytes(StandardCharsets.UTF_8)));
        return this;
    }

    /** Escape hatch for arbitrary entry paths (e.g. {@code module-info.class}, package-info, ...). */
    public TempJarBuilder addEntry(String entryPath, byte[] bytes) {
        entries.add(new Entry(entryPath, bytes));
        return this;
    }

    public File writeTo(File jarFile) throws IOException {
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile))) {
            for (Entry e : entries) {
                jos.putNextEntry(new JarEntry(e.path));
                jos.write(e.bytes);
                jos.closeEntry();
            }
        }
        return jarFile;
    }

    public File writeTo(Path tempDir, String fileName) throws IOException {
        return writeTo(tempDir.resolve(fileName).toFile());
    }

    private static final class Entry {
        final String path;
        final byte[] bytes;
        Entry(String path, byte[] bytes) {
            this.path = path;
            this.bytes = bytes;
        }
    }
}
