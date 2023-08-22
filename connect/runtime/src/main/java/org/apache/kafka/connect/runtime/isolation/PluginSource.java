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
package org.apache.kafka.connect.runtime.isolation;

import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

public class PluginSource {

    public enum Type {
        CLASSPATH, MULTI_JAR, SINGLE_JAR, CLASS_HIERARCHY
    }

    private final Path location;
    private final Type type;
    private final ClassLoader loader;
    private final URL[] urls;

    public PluginSource(Path location, Type type, ClassLoader loader, URL[] urls) {
        this.location = location;
        this.type = type;
        this.loader = loader;
        this.urls = urls;
    }

    public Path location() {
        return location;
    }

    public Type type() {
        return type;
    }

    public ClassLoader loader() {
        return loader;
    }

    public URL[] urls() {
        return urls;
    }

    public boolean isolated() {
        return location != null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PluginSource that = (PluginSource) o;
        return Objects.equals(location, that.location) && loader.equals(that.loader) && Arrays.equals(urls, that.urls);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(location, loader);
        result = 31 * result + Arrays.hashCode(urls);
        return result;
    }

    public String toString() {
        return location == null ? "classpath" : location.toString();
    }
}
