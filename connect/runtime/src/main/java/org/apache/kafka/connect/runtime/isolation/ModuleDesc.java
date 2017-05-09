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

import java.util.Objects;
import org.apache.maven.artifact.versioning.DefaultArtifactVersion;

public class ModuleDesc implements Comparable<ModuleDesc> {
    private final Class<?> klass;
    private final String name;
    private final String version;
    private final DefaultArtifactVersion encodedVersion;

    public ModuleDesc(Class<?> klass, String version) {
        this.klass = klass;
        this.name = klass.getCanonicalName();
        this.version = version;
        this.encodedVersion = new DefaultArtifactVersion(version);
    }

    public Class<?> moduleClass() {
        return klass;
    }

    public String className() {
        return name;
    }

    public String version() {
        return version;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ModuleDesc)) {
            return false;
        }

        ModuleDesc that = (ModuleDesc) o;

        if (klass != null ? !klass.equals(that.klass) : that.klass != null) {
            return false;
        }
        return version != null ? version.equals(that.version) : that.version == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(klass, version);
    }

    @Override
    public int compareTo(ModuleDesc other) {
        int nameComp = name.compareTo(other.name);
        return nameComp != 0 ? nameComp : encodedVersion.compareTo(other.encodedVersion);
    }
}
